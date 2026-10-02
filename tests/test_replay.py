"""Tests for deterministic replay and semantic result comparison."""

import shutil
import tempfile
import unittest

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common.logbus import LogBus
from backend.common.models import Task
from backend.common.storage import Storage
from backend.master.job_manager import JobManager
from backend.master.replay import (
    ReplayConflict,
    ReplayService,
    compare_results,
    input_manifest,
)


def result_doc(partition, records):
    return {
        "partition": partition,
        "partition_name": f"part-{partition:04d}",
        "task_id": f"r-{partition:04d}",
        "records": records,
        "count": len(records),
    }


class ReplayComparisonTests(unittest.TestCase):
    def test_matching_results_ignore_node_assignment(self):
        source = [result_doc(0, [{"key": "a", "count": 2}, {"key": "b", "count": 3}])]
        replay = [result_doc(0, [{"key": "a", "count": 2}, {"key": "b", "count": 3}])]
        report = compare_results(source, replay)
        self.assertEqual(report["status"], "MATCH")
        self.assertTrue(report["consistent"])
        self.assertEqual(report["difference_count"], 0)

    def test_numeric_difference_is_located(self):
        source = [result_doc(0, [{"key": "a", "count": 2}])]
        replay = [result_doc(0, [{"key": "a", "count": 3}])]
        report = compare_results(source, replay, tolerance=0.0)
        self.assertEqual(report["status"], "NUMERIC_DIFF")
        self.assertEqual(report["counts"]["numeric_difference"], 1)
        self.assertEqual(report["differences"][0]["key"], "a")
        self.assertEqual(report["differences"][0]["path"], "count")

    def test_order_difference_is_distinct_from_value_and_count(self):
        source = [result_doc(0, [{"key": "a"}, {"key": "b"}])]
        replay = [result_doc(0, [{"key": "b"}, {"key": "a"}])]
        report = compare_results(source, replay)
        self.assertEqual(report["status"], "ORDER_DIFF")
        self.assertTrue(report["logical_consistent"])
        self.assertFalse(report["consistent"])
        self.assertEqual(report["counts"]["order_difference"], 2)
        self.assertEqual(report["counts"]["value_difference"], 0)
        self.assertEqual(report["counts"]["count_difference"], 0)

    def test_partition_difference_is_reported(self):
        source = [result_doc(0, [{"key": "a"}]), result_doc(1, [{"key": "b"}])]
        replay = [result_doc(0, [{"key": "a"}, {"key": "b"}]), result_doc(1, [])]
        report = compare_results(source, replay)
        self.assertFalse(report["consistent"])
        self.assertEqual(report["counts"]["partition_difference"], 1)
        self.assertTrue(report["logical_consistent"])

    def test_count_difference_distinguishes_missing_and_extra(self):
        source = [result_doc(0, [{"key": "a"}, {"key": "b"}])]
        replay = [result_doc(0, [{"key": "a"}])]
        report = compare_results(source, replay)
        self.assertEqual(report["status"], "COUNT_DIFF")
        diff = report["differences"][0]
        self.assertEqual(diff["direction"], "missing_in_replay")
        self.assertEqual(diff["key"], "b")

    def test_numeric_tolerance(self):
        source = [result_doc(0, [{"key": "a", "sum": 1.0}])]
        replay = [result_doc(0, [{"key": "a", "sum": 1.0 + 1e-12}])]
        self.assertFalse(compare_results(source, replay, tolerance=0.0)["consistent"])
        self.assertTrue(compare_results(source, replay, tolerance=1e-9)["consistent"])


class ReplayPlanningTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig()
        self.logbus = LogBus(self.storage)
        self.jm = JobManager(self.storage, self.config, self.logbus)
        self.service = ReplayService(
            self.storage, self.jm, self.jm.planner, self.logbus,
        )
        self.source = self.jm.submit({
            "name": "source",
            "mapper": "wordcount_mapper",
            "reducer": "count_reducer",
            "num_map_tasks": 4,
            "num_reduce_tasks": 2,
            "input_rows": 81,
            "params": {"x": 1},
        })
        self.source_source = self.storage.read(
            "jobs", self.source.job_id, "shards", C.STAGE_INPUT, "in-0000.json",
        )

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _succeed(self, job):
        for task in self.jm.tasks_for(job.job_id):
            self.jm.update_task(job.job_id, task.task_id, status=C.TASK_SUCCEEDED)
            if task.kind == C.TASK_REDUCE:
                self.storage.write(
                    result_doc(task.partition, [{"key": f"k-{task.partition}", "count": 1}]),
                    "jobs", job.job_id, "results", C.STAGE_REDUCE,
                    f"part-{task.partition:04d}.json",
                )
        self.jm.set_job_status(job, C.JOB_SUCCEEDED)

    def test_replay_clones_exact_shards_split_and_params(self):
        self._succeed(self.source)
        replay = self.service.start_replay(self.source.job_id)
        self.assertNotEqual(replay.job_id, self.source.job_id)
        self.assertEqual(replay.replay_of, self.source.job_id)
        self.assertEqual(replay.num_map_tasks, self.source.num_map_tasks)
        self.assertEqual(replay.num_reduce_tasks, self.source.num_reduce_tasks)
        self.assertEqual(replay.params, self.source.params)

        source_manifest = input_manifest(self.storage, self.source.job_id)
        replay_manifest = input_manifest(self.storage, replay.job_id)
        self.assertEqual(source_manifest["fingerprint"], replay_manifest["fingerprint"])
        self.assertEqual(source_manifest["record_count"], 81)

        replay_shard = self.storage.read(
            "jobs", replay.job_id, "shards", C.STAGE_INPUT, "in-0000.json",
        )
        self.assertEqual(replay_shard["records"], self.source_source["records"])

        manifest = self.storage.read("jobs", replay.job_id, "replay_manifest.json")
        self.assertEqual(manifest["source_job_id"], self.source.job_id)

    def test_only_succeeded_job_can_replay(self):
        with self.assertRaises(ReplayConflict):
            self.service.start_replay(self.source.job_id)

    def test_replay_does_not_modify_source(self):
        self._succeed(self.source)
        before = self.storage.read("jobs", self.source.job_id, "job.json")
        self.service.start_replay(self.source.job_id)
        after = self.storage.read("jobs", self.source.job_id, "job.json")
        self.assertEqual(before, after)

    def test_finalize_compares_replay_and_persists_report_under_replay(self):
        self._succeed(self.source)
        replay = self.service.start_replay(self.source.job_id)
        self._succeed(replay)
        report = self.service.finalize_replay(replay)
        self.assertEqual(report["status"], "MATCH")
        persisted = self.storage.read("jobs", replay.job_id, "replay_report.json")
        self.assertEqual(persisted["source_job_id"], self.source.job_id)
        self.assertIsNone(self.storage.read("jobs", self.source.job_id, "replay_report.json",
                                            default=None))


if __name__ == "__main__":
    unittest.main()
