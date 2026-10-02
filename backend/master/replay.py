"""Deterministic replay and automatic result comparison.

A **replay** re-runs an already-finished job under conditions as identical as
the framework can make them:

* the exact same input shards are copied byte-for-byte (never regenerated — the
  synthetic generator's seed is derived from the unique job id, so a plain
  re-submission draws a different dataset);
* the same mapper/reducer names, the same ``params`` and the same map/reduce
  task count and split boundaries are adopted;
* the replay job runs through the ordinary scheduler, so worker placement,
  dispatch order, retries and speculative execution are allowed to differ —
  none of those are part of the *computation*.

When the replay finishes, its result records are compared against the original
job's persisted records.  Comparison is **logical, not positional**: records
are aligned by key (a MapReduce result is an unordered bag of key groups), so
node allocation and execution order never produce a false "inconsistent".
Three difference kinds are localised to the individual record:

* ``VALUE_DIFF``  — same keys, same count, but a field value differs;
* ``COUNT_DIFF``  — a key is missing in the replay or appears only there;
* ``ORDER_ONLY``   — identical bag of records, only the presentation order
                     differs (explicitly flagged as a benign, non-numeric
                     discrepancy).

The original job is never touched: the replay gets its own job directory and
the verdict is written under ``replays/{replay_id}.json``.
"""

from __future__ import annotations

import copy
import hashlib
import math
import threading
from typing import Any, Optional

from backend.common import constants as C
from backend.common import jsonutil
from backend.common.ids import new_id
from backend.common.jsonutil import canonical_dumps, now_ms
from backend.common.logbus import LogBus
from backend.common.models import Job, Task, new_job, new_task
from backend.common.storage import Storage, list_files, read_json
from backend.master.job_manager import JobManager

# Numeric comparison tolerances: a replay across separate processes is allowed
# to differ by a rounding epsilon (e.g. float summation order) without that
# being called a genuine discrepancy.
ABS_TOLERANCE = 1e-9
REL_TOLERANCE = 1e-6
MAX_EXAMPLES_PER_KIND = 200  # cap per-diff examples so a bad run can't blow the report up


# ===========================================================================
# Pure comparison engine (unit-tested directly, no cluster needed)
# ===========================================================================
def _record_key(rec: Any, position: int) -> Any:
    """Logical identity of a result record.

    Built-in reducers always emit ``{"key": ...}``; a result set that is not
    keyed (records without ``key``) falls back to the whole record as identity,
    which means identical unkeyed records are treated as interchangeable.
    """
    if isinstance(rec, dict) and "key" in rec:
        return rec["key"]
    return ("__unkeyed__", canonical_dumps(rec))


def _to_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None  # True is not "numerically equal to 1" in a result record
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def values_close(a: Any, b: Any) -> bool:
    """Numeric/equality rule used for individual field comparison."""
    # bool is a subclass of int (True == 1), but a bool/non-bool change is a
    # genuine value difference, so resolve it before the numeric fast path.
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a == b
    if a == b:
        return True
    fa, fb = _to_float(a), _to_float(b)
    if fa is not None and fb is not None:
        if math.isnan(fa) and math.isnan(fb):
            return True
        return math.isclose(fa, fb, rel_tol=REL_TOLERANCE, abs_tol=ABS_TOLERANCE)
    return False


def _index_records(records: list) -> dict[Any, list[dict]]:
    out: dict[Any, list[dict]] = {}
    for i, rec in enumerate(records):
        out.setdefault(_record_key(rec, i), []).append({"rec": rec, "index": i})
    return out


def _compare_value(orig_rec: dict, replay_rec: dict) -> list[dict]:
    """Field-by-field diff between two records with the same logical key."""
    diffs: list[dict] = []
    fields = sorted(set(orig_rec) | set(replay_rec))
    for field in fields:
        a = orig_rec.get(field)
        b = replay_rec.get(field)
        if field not in orig_rec:
            diffs.append({"field": field, "original": None, "replay": b,
                          "detail": "field only in replay"})
        elif field not in replay_rec:
            diffs.append({"field": field, "original": a, "replay": None,
                          "detail": "field missing in replay"})
        elif not values_close(a, b):
            entry = {"field": field, "original": a, "replay": b}
            fa, fb = _to_float(a), _to_float(b)
            if fa is not None and fb is not None:
                entry["delta"] = round(fb - fa, 6)
            diffs.append(entry)
    return diffs


def _order_matches(orig: list, replay: list) -> bool:
    """True iff both result sequences present logical keys in the same order."""
    return [_record_key(r, i) for i, r in enumerate(orig)] == \
           [_record_key(r, i) for i, r in enumerate(replay)]


def compare_records(original: list, replay: list) -> dict:
    """Compare two result-record bags, aligned by logical key.

    Returns a structured report: counts per difference kind, concrete examples
    (capped), an ``order_matches`` flag and an overall ``consistent`` verdict.
    Positions in the examples refer to the original/replay result sequences so
    a human can jump straight to the offending row.
    """
    orig_idx = _index_records(original)
    replay_idx = _index_records(replay)
    all_keys = list(orig_idx.keys()) + [k for k in replay_idx if k not in orig_idx]

    value_diffs: list[dict] = []
    missing: list[dict] = []      # in original, absent in replay
    extra: list[dict] = []        # in replay, absent in original
    compared_keys = 0
    original_matched_rows = 0
    replay_matched_rows = 0

    for key in all_keys:
        o_rows = orig_idx.get(key, [])
        r_rows = replay_idx.get(key, [])
        if not r_rows:
            for row in o_rows:
                missing.append({"key": key, "original_index": row["index"], "record": row["rec"]})
            continue
        if not o_rows:
            for row in r_rows:
                extra.append({"key": key, "replay_index": row["index"], "record": row["rec"]})
            continue

        # Same key appears in both runs.  Pair occurrences positionally within
        # the key group; a multiplicity change is reported as missing/extra.
        pairs = min(len(o_rows), len(r_rows))
        compared_keys += 1
        original_matched_rows += pairs
        replay_matched_rows += pairs
        for i in range(pairs):
            o_rec = o_rows[i]["rec"]
            r_rec = r_rows[i]["rec"]
            field_diffs = _compare_value(o_rec, r_rec) if isinstance(o_rec, dict) and isinstance(r_rec, dict) \
                else ([] if values_close(o_rec, r_rec) else [{"field": None, "original": o_rec, "replay": r_rec}])
            if field_diffs:
                value_diffs.append({
                    "key": key,
                    "original_index": o_rows[i]["index"],
                    "replay_index": r_rows[i]["index"],
                    "fields": field_diffs,
                })
        for row in o_rows[pairs:]:
            missing.append({"key": key, "original_index": row["index"], "record": row["rec"]})
        for row in r_rows[pairs:]:
            extra.append({"key": key, "replay_index": row["index"], "record": row["rec"]})

    order_matches = _order_matches(original, replay)
    n_value = len(value_diffs)
    n_count = len(missing) + len(extra)
    has_diff = n_value > 0 or n_count > 0

    if not has_diff:
        verdict = C.REPLAY_MATCH if order_matches else C.REPLAY_ORDER_ONLY
        kinds = [] if order_matches else [C.DIFF_ORDER]
    else:
        kinds = []
        if n_value:
            kinds.append(C.DIFF_VALUE)
        if n_count:
            kinds.append(C.DIFF_COUNT)
        verdict = C.REPLAY_MIXED_DIFF if (n_value and n_count) else (
            C.REPLAY_VALUE_DIFF if n_value else C.REPLAY_COUNT_DIFF)

    return {
        "verdict": verdict,
        "consistent": not has_diff,
        "order_matches": order_matches,
        "difference_kinds": kinds,
        "summary": {
            "original_records": len(original),
            "replay_records": len(replay),
            "keys_compared": compared_keys,
            "value_diff_records": n_value,
            "missing_in_replay": len(missing),
            "extra_in_replay": len(extra),
        },
        "value_diffs": value_diffs[:MAX_EXAMPLES_PER_KIND],
        "missing": missing[:MAX_EXAMPLES_PER_KIND],
        "extra": extra[:MAX_EXAMPLES_PER_KIND],
        "truncated": (n_value > MAX_EXAMPLES_PER_KIND
                      or len(missing) > MAX_EXAMPLES_PER_KIND
                      or len(extra) > MAX_EXAMPLES_PER_KIND),
    }


# ===========================================================================
# ReplayService: launch replays and finalise the verdict
# ===========================================================================
class ReplayService:
    """Owns replay manifests (``replays/*.json``) and the comparison lifecycle."""

    def __init__(self, storage: Storage, job_manager: JobManager, logbus: LogBus) -> None:
        self.storage = storage
        self.job_manager = job_manager
        self.logbus = logbus
        self._lock = threading.RLock()
        self._running: dict[str, str] = {}   # replay_job_id -> replay_id
        self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _path(self, replay_id: str) -> list[str]:
        return ["replays", f"{replay_id}.json"]

    def _load(self) -> None:
        for path in list_files(self.storage.path("replays"), suffix=".json"):
            doc = read_json(path)
            if doc and doc.get("status") == C.REPLAY_RUNNING:
                self._running[doc.get("replay_job_id", "")] = doc.get("replay_id", "")

    def _save(self, manifest: dict) -> None:
        self.storage.write(manifest, *self._path(manifest["replay_id"]))

    def get(self, replay_id: str) -> Optional[dict]:
        return self.storage.read(*self._path(replay_id), default=None)

    def get_for_original(self, original_job_id: str) -> Optional[dict]:
        """Most recent replay of an original job (any status)."""
        docs = [read_json(p) for p in list_files(self.storage.path("replays"), suffix=".json")]
        docs = [d for d in docs if d and d.get("original_job_id") == original_job_id]
        docs.sort(key=lambda d: d.get("started_ms", 0), reverse=True)
        return docs[0] if docs else None

    def list_replays(self) -> list[dict]:
        docs = [read_json(p) for p in list_files(self.storage.path("replays"), suffix=".json")]
        docs = [d for d in docs if d]
        docs.sort(key=lambda d: d.get("started_ms", 0), reverse=True)
        return docs

    # ------------------------------------------------------------------
    # Launch
    # ------------------------------------------------------------------
    def start(self, original_job_id: str) -> dict:
        """Create a replay job fed identical shards/params; return its manifest."""
        with self._lock:
            original = self.job_manager.get_job(original_job_id)
            if original is None:
                raise ValueError(f"unknown job {original_job_id!r}")
            if original.status != C.JOB_SUCCEEDED:
                raise ValueError(
                    f"only succeeded jobs can be replayed (job is {original.status})"
                )
            if original.replay_of:
                raise ValueError("cannot replay a replay; select the original job")
            active = self.job_manager.get_job(original_job_id)
            for replay_job_id in list(self._running):
                rj = self.job_manager.get_job(replay_job_id)
                if rj is not None and not rj.is_terminal:
                    raise ValueError(f"a replay of this job is already running ({replay_job_id})")

            # 1. Build the replay job shell: identical functions/params/splits.
            replay = new_job(
                name=f"{original.name} (回放 Replay)",
                mapper=original.mapper,
                reducer=original.reducer,
                num_map_tasks=original.num_map_tasks,
                num_reduce_tasks=original.num_reduce_tasks,
                input_rows=original.input_rows,
                params=copy.deepcopy(original.params),
            )
            replay.replay_of = original_job_id
            replay.stats["input_kind"] = original.params.get("input_kind")

            # 2. Copy the original input shards verbatim and prove it.
            shards = self.job_manager.planner.copy_input_shards(
                original_job_id, replay.job_id
            )
            if not shards:
                raise ValueError("original job has no persisted input shards; cannot replay")
            fingerprint = hashlib.sha256(
                "".join(s["sha256"] for s in shards).encode("utf-8")
            ).hexdigest()
            total_input_records = sum(s["count"] for s in shards)

            # 3. Rebuild the identical task graph (m/r counts and split indices).
            map_tasks = [new_task(replay, C.TASK_MAP, i) for i in range(len(shards))]
            reduce_tasks = [new_task(replay, C.TASK_REDUCE, p)
                            for p in range(original.num_reduce_tasks)]
            replay.stats["total_records"] = original.stats.get("total_records", total_input_records)
            self.job_manager.register_replay(replay, map_tasks, reduce_tasks)

            # 4. Persist the replay manifest (original results remain untouched).
            replay_id = new_id("replay")
            manifest = {
                "replay_id": replay_id,
                "original_job_id": original_job_id,
                "original_name": original.name,
                "replay_job_id": replay.job_id,
                "status": C.REPLAY_RUNNING,
                "verdict": None,
                "started_ms": now_ms(),
                "finished_ms": 0,
                "spec": {
                    "mapper": original.mapper,
                    "reducer": original.reducer,
                    "params": copy.deepcopy(original.params),
                    "num_map_tasks": replay.num_map_tasks,
                    "num_reduce_tasks": replay.num_reduce_tasks,
                    "input_rows": original.input_rows,
                },
                "input_fingerprint": {
                    "sha256": fingerprint,
                    "shard_count": len(shards),
                    "total_records": total_input_records,
                    "shards": shards,
                },
                "report": None,
            }
            self._save(manifest)
            self._running[replay.job_id] = replay_id
            self.logbus.info(
                original_job_id,
                f"replay {replay.job_id} launched with {len(shards)} identical input shards",
                task_id="replay",
            )
            return manifest

    # ------------------------------------------------------------------
    # Finalisation (called from the scheduler tick)
    # ------------------------------------------------------------------
    def sweep_terminal(self) -> list[str]:
        """Finalise the comparison for any replay job that reached a terminal state."""
        finalised: list[str] = []
        with self._lock:
            for replay_job_id in list(self._running):
                replay_job = self.job_manager.get_job(replay_job_id)
                if replay_job is None or not replay_job.is_terminal:
                    continue
                replay_id = self._running.pop(replay_job_id)
                self._finalise(replay_id, replay_job)
                finalised.append(replay_id)
        return finalised

    def _read_results(self, job_id: str) -> list[dict]:
        records: list[dict] = []
        root = self.storage.path("jobs", job_id, "results", C.STAGE_REDUCE)
        for path in list_files(root, suffix=".json"):
            doc = read_json(path)
            if doc:
                records.extend(doc.get("records", []))
        return records

    def _partition_counts(self, job_id: str) -> dict[str, int]:
        root = self.storage.path("jobs", job_id, "results", C.STAGE_REDUCE)
        out: dict[str, int] = {}
        for path in list_files(root, suffix=".json"):
            doc = read_json(path)
            if doc:
                out[str(doc.get("partition_name", doc.get("partition")))] = int(doc.get("count", 0))
        return out

    def _finalise(self, replay_id: str, replay_job: Job) -> dict:
        manifest = self.get(replay_id)
        if manifest is None:
            return {}
        original_job_id = manifest["original_job_id"]
        original = self.job_manager.get_job(original_job_id)

        manifest["finished_ms"] = now_ms()
        if replay_job.status != C.JOB_SUCCEEDED or original is None:
            manifest["status"] = C.REPLAY_FAILED
            manifest["verdict"] = C.REPLAY_FAILED
            manifest["error"] = replay_job.error or "replay job did not succeed"
            self._save(manifest)
            self.logbus.error(
                original_job_id, f"replay {replay_job.job_id} failed: {manifest['error']}",
                task_id="replay",
            )
            return manifest

        original_records = self._read_results(original_job_id)
        replay_records = self._read_results(replay_job.job_id)
        report = compare_records(original_records, replay_records)

        # Per-partition count comparison localises a discrepancy to one reducer.
        op = self._partition_counts(original_job_id)
        rp = self._partition_counts(replay_job.job_id)
        partition_diffs = [
            {"partition": name,
             "original": op.get(name, 0),
             "replay": rp.get(name, 0)}
            for name in sorted(set(op) | set(rp))
            if op.get(name, 0) != rp.get(name, 0)
        ]
        if partition_diffs:
            report.setdefault("difference_kinds", []).append(C.DIFF_PARTITION_COUNT)
        report["partition_counts"] = {"original": op, "replay": rp}
        report["partition_count_diffs"] = partition_diffs

        manifest["status"] = report["verdict"]
        manifest["verdict"] = report["verdict"]
        manifest["report"] = report
        self._save(manifest)

        level = self.logbus.info if report["consistent"] else self.logbus.warn
        level(
            original_job_id,
            f"replay {replay_job.job_id} verdict: {report['verdict']} "
            f"(original {len(original_records)} records, replay {len(replay_records)}, "
            f"value diffs {report['summary']['value_diff_records']}, "
            f"missing {report['summary']['missing_in_replay']}, "
            f"extra {report['summary']['extra_in_replay']})",
            task_id="replay",
        )
        return manifest
