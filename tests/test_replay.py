"""Tests for deterministic replay: the record comparator, replay launch and a
full end-to-end run against an in-process Master with real thread-mode Workers.

The E2E case is the feature's contract: submit a job, let it finish, launch a
replay with identical shards/params, and assert the automatic verdict is
MATCH — even though the replay's tasks are dispatched on whatever worker the
load-based scheduler happens to pick.
"""

import os
import shutil
import socket
import sys
import tempfile
import threading
import time
import unittest

from backend.common import constants as C
from backend.common.http_client import HttpClient
from backend.master.replay import (
    ABS_TOLERANCE,
    compare_records,
    values_close,
)


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ===========================================================================
# Pure comparator
# ===========================================================================
class TestCompare(unittest.TestCase):
    def test_identical_is_match(self):
        recs = [{"key": "a", "count": 1}, {"key": "b", "count": 2}]
        r = compare_records(recs, [dict(x) for x in recs])
        self.assertEqual(r["verdict"], C.REPLAY_MATCH)
        self.assertTrue(r["consistent"])
        self.assertTrue(r["order_matches"])

    def test_reorder_only_is_consistent_and_flagged(self):
        orig = [{"key": "a", "v": 1}, {"key": "b", "v": 2}, {"key": "c", "v": 3}]
        replay = [{"key": "c", "v": 3}, {"key": "a", "v": 1}, {"key": "b", "v": 2}]
        r = compare_records(orig, replay)
        self.assertEqual(r["verdict"], C.REPLAY_ORDER_ONLY)
        self.assertTrue(r["consistent"])
        self.assertFalse(r["order_matches"])
        self.assertEqual(r["difference_kinds"], [C.DIFF_ORDER])

    def test_reorder_does_not_hide_value_diff(self):
        # Ordering differs AND a value differs: key alignment must still catch it.
        orig = [{"key": "a", "v": 1}, {"key": "b", "v": 2}]
        replay = [{"key": "b", "v": 2}, {"key": "a", "v": 9}]
        r = compare_records(orig, replay)
        self.assertEqual(r["verdict"], C.REPLAY_VALUE_DIFF)
        self.assertFalse(r["order_matches"])
        self.assertEqual(r["summary"]["value_diff_records"], 1)
        self.assertEqual(r["value_diffs"][0]["key"], "a")

    def test_numeric_tolerance(self):
        self.assertTrue(values_close(1.0, 1.0 + ABS_TOLERANCE / 10))
        self.assertFalse(values_close(1.0, 1.0 + 1e-3))
        r = compare_records([{"key": "x", "v": 1.0}],
                            [{"key": "x", "v": 1.0 + 1e-12}])
        self.assertEqual(r["verdict"], C.REPLAY_MATCH)

    def test_value_diff_locates_record_and_field(self):
        r = compare_records(
            [{"key": "a", "count": 1}, {"key": "b", "count": 2}],
            [{"key": "a", "count": 1}, {"key": "b", "count": 5}],
        )
        self.assertEqual(r["verdict"], C.REPLAY_VALUE_DIFF)
        diff = r["value_diffs"][0]
        self.assertEqual(diff["key"], "b")
        self.assertEqual(diff["original_index"], 1)
        self.assertEqual(diff["fields"][0]["field"], "count")
        self.assertEqual(diff["fields"][0]["original"], 2)
        self.assertEqual(diff["fields"][0]["replay"], 5)

    def test_count_diff_missing_and_extra(self):
        r = compare_records(
            [{"key": "a"}, {"key": "b"}, {"key": "c"}],
            [{"key": "a"}, {"key": "d"}],
        )
        self.assertEqual(r["verdict"], C.REPLAY_COUNT_DIFF)
        self.assertEqual(r["summary"]["missing_in_replay"], 2)
        self.assertEqual(r["summary"]["extra_in_replay"], 1)
        self.assertEqual({d["key"] for d in r["missing"]}, {"b", "c"})
        self.assertEqual(r["extra"][0]["key"], "d")
        self.assertIn(C.DIFF_COUNT, r["difference_kinds"])

    def test_mixed_value_and_count(self):
        r = compare_records(
            [{"key": "a", "v": 1}, {"key": "b", "v": 2}],
            [{"key": "a", "v": 8}],
        )
        self.assertEqual(r["verdict"], C.REPLAY_MIXED_DIFF)
        self.assertEqual({C.DIFF_VALUE, C.DIFF_COUNT}, set(r["difference_kinds"]))

    def test_bool_is_not_treated_as_number(self):
        self.assertFalse(values_close(True, 1))
        r = compare_records([{"key": "x", "ok": True}], [{"key": "x", "ok": False}])
        self.assertEqual(r["verdict"], C.REPLAY_VALUE_DIFF)


# ===========================================================================
# End-to-end against a real cluster
# ===========================================================================
class TestReplayEndToEnd(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mr-replay-")
        self.master_port = _free_port()
        self.worker_ports = [_free_port() for _ in range(3)]
        self.master_url = f"http://127.0.0.1:{self.master_port}"
        self.client = HttpClient(timeout=10.0, retries=2)
        self._threads = []
        self._procs = []
        self._start_cluster()

    def tearDown(self):
        for t in self._threads:
            if t.is_alive():
                import ctypes
                # daemon threads die with the test process; just give them room
                pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- cluster bootstrap --------------------------------------------
    def _start_cluster(self):
        from backend.common.config import ClusterConfig
        from backend.master.server import Master
        from backend.worker.server import WorkerServer
        from backend.common.ids import new_id

        data_master = os.path.join(self.tmp, "data")
        master = Master(data_master, host="127.0.0.1", port=self.master_port)
        master.start()

        def serve_master():
            master.app.run(host="127.0.0.1", port=self.master_port,
                           threaded=True, use_reloader=False)

        mt = threading.Thread(target=serve_master, daemon=True)
        mt.start()
        self._threads.append(mt)
        self._wait_ready(self.master_url + "/api/overview")

        cfg = ClusterConfig(heartbeat_timeout_sec=300.0)
        for port in self.worker_ports:
            worker_id = new_id("worker")
            server = WorkerServer(
                data_root=os.path.join(data_master, "workers", worker_id),
                master_url=self.master_url,
                host="127.0.0.1", port=port, name=f"worker-{port}",
                worker_id=worker_id, config=cfg, exec_mode="thread",
            )
            t = threading.Thread(target=server.serve, daemon=True,
                                 kwargs={"host": "127.0.0.1", "port": port})
            # serve() takes no kwargs; bind through the constructed app directly:
            t = threading.Thread(
                target=lambda s=server: s.app.run(
                    host="127.0.0.1", port=s.port, threaded=True, use_reloader=False),
                daemon=True,
            )
            t.start()
            self._threads.append(t)
            server.start()  # registers + heartbeats

        deadline = time.time() + 20
        while time.time() < deadline:
            d = self.client.get_json(self.master_url + "/api/workers", default=None)
            if d and len(d.get("workers", [])) >= 3:
                return
            time.sleep(0.3)
        self.fail("workers did not register in time")

    def _wait_ready(self, url: str):
        deadline = time.time() + 20
        while time.time() < deadline:
            if self.client.get_json(url, default=None) is not None:
                return
            time.sleep(0.3)
        self.fail(f"master not ready: {url}")

    def _wait_job(self, job_id: str, timeout: float = 90.0) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            d = self.client.get_json(f"{self.master_url}/api/jobs/{job_id}", default=None)
            if d:
                job = d["job"]
                if job["status"] in C.JOB_TERMINAL_STATES:
                    return job
            time.sleep(0.5)
        self.fail(f"job {job_id} did not finish")

    def _wait_replay(self, replay_id: str, timeout: float = 90.0) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            d = self.client.get_json(f"{self.master_url}/api/replays/{replay_id}", default=None)
            if d and d.get("status") in C.REPLAY_TERMINAL_STATES:
                return d
            time.sleep(0.5)
        self.fail(f"replay {replay_id} did not finalise")

    # -- scenarios -----------------------------------------------------
    def test_replay_matches_original(self):
        spec = {
            "name": "replay-e2e-wordcount",
            "mapper": "wordcount_mapper",
            "reducer": "count_reducer",
            "num_map_tasks": 6,
            "num_reduce_tasks": 3,
            "input_rows": 1500,
            "params": {},
        }
        resp = self.client.post(self.master_url + "/api/jobs", spec, timeout=10.0)
        self.assertTrue(resp.ok, resp.data)
        original_id = resp.data["job_id"]
        original = self._wait_job(original_id)
        self.assertEqual(original["status"], C.JOB_SUCCEEDED, original.get("error"))

        before = self.client.get_json(
            f"{self.master_url}/api/jobs/{original_id}/results", default=None)
        self.assertGreater(before["total"], 0)

        # Launch the replay and wait for the automatic comparison verdict.
        resp = self.client.post(f"{self.master_url}/api/jobs/{original_id}/replay", {}, timeout=10.0)
        self.assertTrue(resp.ok, resp.data)
        manifest = resp.data
        replay_id = manifest["replay_id"]
        replay_job_id = manifest["replay_job_id"]
        self.assertNotEqual(replay_job_id, original_id)

        # Identical-input guarantee: same shard count and fingerprints recorded.
        self.assertEqual(manifest["input_fingerprint"]["shard_count"], 6)
        replay_job = self._wait_job(replay_job_id)
        self.assertEqual(replay_job["status"], C.JOB_SUCCEEDED, replay_job.get("error"))
        self.assertEqual(replay_job["replay_of"], original_id)

        result = self._wait_replay(replay_id)
        self.assertEqual(result["status"], C.REPLAY_MATCH, result.get("report"))
        report = result["report"]
        self.assertTrue(report["consistent"])
        self.assertTrue(report["order_matches"])
        self.assertEqual(report["summary"]["original_records"],
                         report["summary"]["replay_records"])
        self.assertEqual(report["summary"]["value_diff_records"], 0)
        self.assertEqual(report["summary"]["missing_in_replay"], 0)
        self.assertEqual(report["summary"]["extra_in_replay"], 0)
        self.assertEqual(report["partition_count_diffs"], [])

        # The replay manifest must be listable and the original job untouched.
        listed = self.client.get_json(self.master_url + "/api/replays", default=None)
        self.assertTrue(any(r["replay_id"] == replay_id for r in listed["replays"]))
        untouched = self.client.get_json(
            f"{self.master_url}/api/jobs/{original_id}/results", default=None)
        self.assertEqual(untouched["total"], before["total"])

    def test_only_succeeded_jobs_can_be_replayed(self):
        spec = {
            "name": "replay-e2e-kv",
            "mapper": "kv_mapper",
            "reducer": "sum_reducer",
            "num_map_tasks": 4,
            "num_reduce_tasks": 2,
            "input_rows": 800,
            "params": {},
        }
        resp = self.client.post(self.master_url + "/api/jobs", spec, timeout=10.0)
        original_id = resp.data["job_id"]
        self._wait_job(original_id)
        # Replaying a replay must be refused.
        first = self.client.post(f"{self.master_url}/api/jobs/{original_id}/replay", {}, timeout=10.0)
        self.assertTrue(first.ok)
        replay_job_id = first.data["replay_job_id"]
        self._wait_job(replay_job_id)
        bad = self.client.post(f"{self.master_url}/api/jobs/{replay_job_id}/replay", {}, timeout=10.0)
        self.assertFalse(bad.ok)

    def test_unknown_job_replay_404(self):
        resp = self.client.post(self.master_url + "/api/jobs/job-does-not-exist/replay", {}, timeout=10.0)
        self.assertFalse(resp.ok)
        self.assertEqual(resp.status, 404)


if __name__ == "__main__":
    unittest.main()
