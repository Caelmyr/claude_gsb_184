"""Deterministic job replay and result comparison.

A replay is a normal MapReduce execution, but its input is byte-for-byte copied
from a completed source job.  Worker assignment, task completion interleaving and
retry attempts are therefore allowed to vary; comparison is performed on logical
result records (normally keyed by the reducer's ``key``), not on execution
metadata.  Both the replay manifest and comparison report are stored under the
new replay job, so a source job's historical files are never modified.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Optional

from backend.common import constants as C
from backend.common import jsonutil
from backend.common.jsonutil import now_ms
from backend.common.logbus import LogBus
from backend.common.models import Job, new_job
from backend.common.storage import Storage, list_files, read_json
from backend.master.job_manager import JobManager

DEFAULT_NUMERIC_TOLERANCE = 1e-9
REPLAY_RUNNING = "RUNNING"
REPLAY_MATCH = "MATCH"
REPLAY_ORDER_DIFF = "ORDER_DIFF"
REPLAY_MISMATCH = "MISMATCH"
REPLAY_FAILED = "FAILED"


class ReplayConflict(RuntimeError):
    """Raised when a replay cannot be started in the requested state."""


@dataclass
class ResultEntry:
    record: dict
    partition: int
    position: int
    key: str
    identity: str
    explicit_key: bool


# ---------------------------------------------------------------------------
# Canonical serialization / fingerprints
# ---------------------------------------------------------------------------
def canonical_json(value: Any) -> str:
    return json.dumps(
        jsonutil.sanitize(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    )


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _read_input_shard_docs(storage: Storage, job_id: str) -> list[dict]:
    root = storage.path("jobs", job_id, "shards", C.STAGE_INPUT)
    docs = [doc for path in list_files(root, suffix=".json") if (doc := read_json(path))]
    docs.sort(key=lambda d: int(d.get("index", 0)))
    return docs


def input_manifest(storage: Storage, job_id: str) -> dict:
    docs = _read_input_shard_docs(storage, job_id)
    shards = []
    for doc in docs:
        records = doc.get("records", [])
        shards.append({
            "shard_id": doc.get("shard_id"),
            "index": int(doc.get("index", 0)),
            "count": len(records),
            "fingerprint": fingerprint(records),
        })
    return {
        "shard_count": len(docs),
        "record_count": sum(s["count"] for s in shards),
        "shards": shards,
        "fingerprint": fingerprint([
            {"index": s["index"], "shard_id": s["shard_id"], "fingerprint": s["fingerprint"]}
            for s in shards
        ]),
    }


# ---------------------------------------------------------------------------
# Result loading and semantic comparison
# ---------------------------------------------------------------------------
def read_result_documents(storage: Storage, job_id: str) -> list[dict]:
    root = storage.path("jobs", job_id, "results", C.STAGE_REDUCE)
    docs = [doc for path in list_files(root, suffix=".json") if (doc := read_json(path))]
    docs.sort(key=lambda d: int(d.get("partition", 0)))
    return docs


def _entries_from_docs(docs: list[dict]) -> list[ResultEntry]:
    entries: list[ResultEntry] = []
    position = 0
    for doc in docs:
        partition = int(doc.get("partition", 0))
        for rec in doc.get("records", []):
            if isinstance(rec, dict) and "key" in rec:
                key = str(rec.get("key"))
                identity = "key:" + type(rec.get("key")).__name__ + ":" + canonical_json(rec.get("key"))
                explicit_key = True
            else:
                key = f"#{position}"
                # Keyless records do not have a stable semantic identity.  Align
                # them positionally so a changed value is not reported as two
                # count differences.
                identity = f"position:{position}"
                explicit_key = False
            entries.append(ResultEntry(
                record=rec,
                partition=partition,
                position=position,
                key=key,
                identity=identity,
                explicit_key=explicit_key,
            ))
            position += 1
    return entries


def _annotate_occurrences(entries: list[ResultEntry]) -> list[dict]:
    seen: dict[str, int] = {}
    annotated = []
    for entry in entries:
        occurrence = seen.get(entry.identity, 0)
        seen[entry.identity] = occurrence + 1
        annotated.append({
            "entry": entry,
            "uid": f"{entry.identity}#{occurrence}",
            "base_identity": entry.identity,
            "occurrence": occurrence,
        })
    return annotated


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _numbers_close(expected: Any, actual: Any, tolerance: float) -> tuple[bool, Optional[float]]:
    if not (_is_number(expected) and _is_number(actual)):
        return False, None
    if not (math.isfinite(expected) and math.isfinite(actual)):
        return expected == actual, None
    delta = abs(float(actual) - float(expected))
    scale = max(1.0, abs(float(expected)), abs(float(actual)))
    return delta <= tolerance or delta / scale <= tolerance, delta


def _field_differences(path: str, expected: Any, actual: Any,
                       tolerance: float) -> list[dict]:
    close, delta = _numbers_close(expected, actual, tolerance)
    if close:
        return []
    if _is_number(expected) or _is_number(actual):
        diff = {
            "type": "numeric_difference",
            "path": path,
            "source_value": expected,
            "replay_value": actual,
        }
        if delta is not None:
            diff["abs_delta"] = delta
            denom = expected if expected else actual
            if _is_number(denom) and float(denom) != 0.0:
                diff["rel_delta"] = delta / abs(float(denom))
        return [diff]

    if isinstance(expected, dict) and isinstance(actual, dict):
        out = []
        for key in sorted(set(expected) | set(actual), key=str):
            child = f"{path}.{key}" if path else str(key)
            if key not in expected:
                out.append({"type": "extra_field", "path": child,
                            "source_value": None, "replay_value": actual[key]})
            elif key not in actual:
                out.append({"type": "missing_field", "path": child,
                            "source_value": expected[key], "replay_value": None})
            else:
                out.extend(_field_differences(child, expected[key], actual[key], tolerance))
        return out

    if isinstance(expected, list) and isinstance(actual, list):
        if len(expected) != len(actual):
            return [{"type": "value_difference", "path": path,
                     "source_value": expected, "replay_value": actual,
                     "message": "list length differs"}]
        out = []
        for i, (a, b) in enumerate(zip(expected, actual)):
            out.extend(_field_differences(f"{path}[{i}]", a, b, tolerance))
        return out

    if expected == actual:
        return []

    if type(expected) is not type(actual):
        kind = "type_mismatch"
    else:
        kind = "value_difference"
    return [{"type": kind, "path": path,
             "source_value": expected, "replay_value": actual}]


def compare_results(source_docs: list[dict], replay_docs: list[dict],
                    tolerance: float = DEFAULT_NUMERIC_TOLERANCE) -> dict:
    """Compare logical result records and classify every discrepancy."""
    source = _annotate_occurrences(_entries_from_docs(source_docs))
    replay = _annotate_occurrences(_entries_from_docs(replay_docs))
    source_by_uid = {item["uid"]: item for item in source}
    replay_by_uid = {item["uid"]: item for item in replay}

    differences: list[dict] = []
    counts = {
        "numeric_difference": 0,
        "value_difference": 0,
        "order_difference": 0,
        "count_difference": 0,
        "partition_difference": 0,
        "type_mismatch": 0,
        "extra_field": 0,
        "missing_field": 0,
    }

    def add(diff: dict) -> None:
        differences.append(diff)
        counts[diff["type"]] = counts.get(diff["type"], 0) + 1

    # Missing / extra records (also catches duplicate logical keys).
    for uid, item in source_by_uid.items():
        if uid not in replay_by_uid:
            e = item["entry"]
            add({
                "type": "count_difference",
                "direction": "missing_in_replay",
                "identity": e.identity,
                "key": e.key,
                "occurrence": item["occurrence"],
                "source_partition": e.partition,
                "source_position": e.position,
                "source_record": e.record,
            })
    for uid, item in replay_by_uid.items():
        if uid not in source_by_uid:
            e = item["entry"]
            add({
                "type": "count_difference",
                "direction": "extra_in_replay",
                "identity": e.identity,
                "key": e.key,
                "occurrence": item["occurrence"],
                "replay_partition": e.partition,
                "replay_position": e.position,
                "replay_record": e.record,
            })

    # Field/numeric differences for records present on both sides.
    for uid, source_item in source_by_uid.items():
        replay_item = replay_by_uid.get(uid)
        if replay_item is None:
            continue
        source_entry = source_item["entry"]
        replay_entry = replay_item["entry"]
        for diff in _field_differences("", source_entry.record, replay_entry.record, tolerance):
            if diff.get("path") == "key":
                continue
            diff.update({
                "identity": source_entry.identity,
                "key": source_entry.key,
                "occurrence": source_item["occurrence"],
                "source_partition": source_entry.partition,
                "replay_partition": replay_entry.partition,
                "source_position": source_entry.position,
                "replay_position": replay_entry.position,
            })
            add(diff)
        if source_entry.partition != replay_entry.partition:
            add({
                "type": "partition_difference",
                "identity": source_entry.identity,
                "key": source_entry.key,
                "occurrence": source_item["occurrence"],
                "source_partition": source_entry.partition,
                "replay_partition": replay_entry.partition,
                "source_position": source_entry.position,
                "replay_position": replay_entry.position,
            })

    # Output ordering is compared only for records that have a stable key.
    # Keyless positional records have no semantic order to test.
    if (len(source) == len(replay) and source_by_uid.keys() == replay_by_uid.keys()
            and all(item["entry"].explicit_key for item in source)):
        for idx, (source_item, replay_item) in enumerate(zip(source, replay)):
            if source_item["uid"] != replay_item["uid"]:
                s = source_item["entry"]
                r = replay_item["entry"]
                add({
                    "type": "order_difference",
                    "identity": s.identity,
                    "key": s.key,
                    "source_position": s.position,
                    "replay_position": r.position,
                    "source_partition": s.partition,
                    "replay_partition": r.partition,
                    "sequence_position": idx,
                })

    count_problem = counts["count_difference"] > 0
    value_problem = any(counts[k] > 0 for k in (
        "numeric_difference", "value_difference", "type_mismatch",
        "extra_field", "missing_field",
    ))
    order_problem = any(counts[k] > 0 for k in ("order_difference", "partition_difference"))
    logical_consistent = not count_problem and not value_problem
    consistent = logical_consistent and not order_problem

    if value_problem:
        status = "NUMERIC_DIFF" if counts["numeric_difference"] and not any(
            counts[k] for k in ("value_difference", "type_mismatch", "extra_field", "missing_field")
        ) else REPLAY_MISMATCH
    elif count_problem:
        status = "COUNT_DIFF"
    elif order_problem:
        status = REPLAY_ORDER_DIFF
    else:
        status = REPLAY_MATCH

    return {
        "status": status,
        "consistent": consistent,
        "logical_consistent": logical_consistent,
        "numeric_tolerance": tolerance,
        "source_total": len(source),
        "replay_total": len(replay),
        "difference_count": len(differences),
        "counts": counts,
        "differences": differences,
        "source_partitions": [
            {"partition": int(d.get("partition", 0)), "count": len(d.get("records", []))}
            for d in source_docs
        ],
        "replay_partitions": [
            {"partition": int(d.get("partition", 0)), "count": len(d.get("records", []))}
            for d in replay_docs
        ],
    }


class ReplayService:
    def __init__(self, storage: Storage, job_manager: JobManager,
                 planner: Any, logbus: LogBus) -> None:
        self.storage = storage
        self.job_manager = job_manager
        self.planner = planner
        self.logbus = logbus

    def _manifest_path(self, job_id: str) -> list[str]:
        return ["jobs", job_id, "replay_manifest.json"]

    def _report_path(self, job_id: str) -> list[str]:
        return ["jobs", job_id, "replay_report.json"]

    def start_replay(self, source_job_id: str,
                     tolerance: float = DEFAULT_NUMERIC_TOLERANCE) -> Job:
        source = self.job_manager.get_job(source_job_id)
        if source is None:
            raise ReplayConflict(f"unknown job {source_job_id}")
        if source.status != C.JOB_SUCCEEDED:
            raise ReplayConflict("only a succeeded job can be replayed")

        for job in self.job_manager.list_jobs():
            if job.replay_of == source_job_id and job.replay_status == REPLAY_RUNNING:
                raise ReplayConflict(f"replay {job.job_id} is already running")

        tolerance = max(0.0, float(tolerance))
        source_input = input_manifest(self.storage, source.job_id)
        if source_input["shard_count"] == 0 or source_input["record_count"] == 0:
            raise ReplayConflict("source job has no persisted input records")

        params = copy.deepcopy(source.params)
        replay = new_job(
            f"{source.name} (replay)",
            source.mapper,
            source.reducer,
            source.num_map_tasks,
            source.num_reduce_tasks,
            source.input_rows,
            params,
        )
        replay.replay_of = source.job_id
        replay.replay_status = REPLAY_RUNNING

        plan = self.planner.clone_plan(source, replay)
        replay_input = input_manifest(self.storage, replay.job_id)
        if replay_input["fingerprint"] != source_input["fingerprint"]:
            raise RuntimeError("replay input fingerprint does not match source")

        manifest = {
            "schema_version": 1,
            "replay_job_id": replay.job_id,
            "source_job_id": source.job_id,
            "created_ms": now_ms(),
            "mapper": source.mapper,
            "reducer": source.reducer,
            "num_map_tasks": source.num_map_tasks,
            "num_reduce_tasks": source.num_reduce_tasks,
            "input_rows": source.input_rows,
            "params": params,
            "params_fingerprint": fingerprint(params),
            "numeric_tolerance": tolerance,
            "source_input": source_input,
            "replay_input": replay_input,
        }

        with self.job_manager._lock:
            replay.num_map_tasks = len(plan["map_tasks"])
            replay.map_task_ids = [t.task_id for t in plan["map_tasks"]]
            replay.reduce_task_ids = [t.task_id for t in plan["reduce_tasks"]]
            replay.status = C.JOB_MAP
            replay.started_ms = now_ms()
            replay.stats.update({
                "total_records": plan["total_records"],
                "input_kind": params.get("input_kind"),
                "replay_source_job_id": source.job_id,
                "replay_input_fingerprint": replay_input["fingerprint"],
                "replay_params_fingerprint": manifest["params_fingerprint"],
            })
            self.job_manager._jobs[replay.job_id] = replay
            self.job_manager._tasks[replay.job_id] = {}
            for task in plan["map_tasks"] + plan["reduce_tasks"]:
                self.job_manager._tasks[replay.job_id][task.task_id] = task
                self.job_manager.save_task(replay.job_id, task)
            self.storage.write(manifest, *self._manifest_path(replay.job_id))
            self.job_manager.save_job(replay)

        self.logbus.info(
            replay.job_id,
            f"replay started from {source.job_id}: {replay_input['record_count']} records, "
            f"{replay_input['shard_count']} input shards",
            task_id="replay",
        )
        return replay

    def finalize_replay(self, replay: Job) -> Optional[dict]:
        if not replay.replay_of or replay.replay_status != REPLAY_RUNNING:
            return None
        source = self.job_manager.get_job(replay.replay_of)
        manifest = self.storage.read(*self._manifest_path(replay.job_id), default={}) or {}
        tolerance = float(manifest.get("numeric_tolerance", DEFAULT_NUMERIC_TOLERANCE))

        if replay.status != C.JOB_SUCCEEDED or source is None:
            if source is None:
                error = "source job is unavailable"
            elif replay.status == C.JOB_CANCELLED:
                error = "replay was cancelled before comparison"
            else:
                error = replay.error or "replay did not succeed"
            report = {
                "schema_version": 1,
                "replay_job_id": replay.job_id,
                "source_job_id": replay.replay_of,
                "status": REPLAY_FAILED,
                "consistent": False,
                "logical_consistent": False,
                "created_ms": manifest.get("created_ms", 0),
                "completed_ms": now_ms(),
                "error": error,
                "numeric_tolerance": tolerance,
            }
        else:
            source_docs = read_result_documents(self.storage, source.job_id)
            replay_docs = read_result_documents(self.storage, replay.job_id)
            report = compare_results(source_docs, replay_docs, tolerance=tolerance)
            report.update({
                "schema_version": 1,
                "replay_job_id": replay.job_id,
                "source_job_id": source.job_id,
                "created_ms": manifest.get("created_ms", 0),
                "completed_ms": now_ms(),
                "source_job_name": source.name,
                "replay_job_name": replay.name,
            })

        self.storage.write(report, *self._report_path(replay.job_id))
        summary = {
            key: report.get(key)
            for key in (
                "status", "consistent", "logical_consistent", "difference_count",
                "counts", "source_total", "replay_total", "numeric_tolerance",
                "completed_ms",
            )
        }
        self.job_manager.update_job(
            replay,
            replay_status=report["status"],
            replay_summary=summary,
            replay_report=report,
        )
        level = "info" if report.get("logical_consistent") else "warn"
        getattr(self.logbus, level)(
            replay.job_id,
            f"replay comparison: {report['status']} "
            f"({report.get('difference_count', 0)} differences)",
            task_id="replay",
        )
        return report

    def get_report(self, job_id: str) -> Optional[dict]:
        job = self.job_manager.get_job(job_id)
        if job is None or not job.replay_of:
            return None
        return self.storage.read(*self._report_path(job_id), default=None)

    def list_for_source(self, source_job_id: str) -> list[dict]:
        out = []
        for job in self.job_manager.list_jobs():
            if job.replay_of == source_job_id:
                out.append({
                    "job_id": job.job_id,
                    "name": job.name,
                    "status": job.status,
                    "replay_status": job.replay_status,
                    "created_ms": job.created_ms,
                    "finished_ms": job.finished_ms,
                    "report": job.replay_report or self.get_report(job.job_id) or {},
                })
        out.sort(key=lambda d: d["created_ms"], reverse=True)
        return out
