import csv
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from src.vine_parse.metric_facts import generate_metric_facts


class MetricFactsTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.run = Path(self.tempdir.name)
        (self.run / "vine-logs").mkdir()
        (self.run / "csv-files").mkdir()

    def tearDown(self):
        self.tempdir.cleanup()

    def test_source_backed_metrics_and_cross_source_conservation(self):
        transactions = """# transaction fixture
1000000 7 MANAGER 7 START 0
1100000 7 WORKER worker-a CONNECTION 127.0.0.1:9000
1200000 7 TASK 1 READY alpha FIRST_RESOURCES 1 {"cores":[1,"cores"]}
1300000 7 TASK 1 RUNNING worker-a FIRST_RESOURCES {"cores":[1,"cores"]}
1800000 7 TASK 1 WAITING_RETRIEVAL worker-a
1900000 7 TASK 1 RETRIEVED SUCCESS {} {"time_worker_start":[1.3,"s"],"time_worker_end":[1.8,"s"],"wall_time":[0.5,"s"]}
1950000 7 TASK 1 DONE SUCCESS 0
2000000 7 TASK 2 READY beta FIRST_RESOURCES 1 {"cores":[1,"cores"]}
2100000 7 TASK 2 RUNNING worker-a FIRST_RESOURCES {"cores":[1,"cores"]}
2400000 7 TASK 2 WAITING_RETRIEVAL worker-a
2500000 7 TASK 2 RETRIEVED UNKNOWN {} {"time_worker_start":[2.1,"s"],"time_worker_end":[2.4,"s"],"wall_time":[0.3,"s"]}
2600000 7 WORKER worker-a TRANSFER INPUT file-a 100 1000 2599000
2700000 7 WORKER worker-a DISCONNECTION EXPLICIT
3000000 7 MANAGER 7 END 2000000
"""
        (self.run / "vine-logs" / "transactions").write_text(transactions, encoding="utf-8")
        performance = """# timestamp workers_connected tasks_running total_cores committed_cores bytes_sent bytes_received workers_joined
1000000 0 0 0 0 0 0 0
2000000 1 1 4 1 0 0 1
3000000 0 0 0 0 100 0 1
"""
        (self.run / "vine-logs" / "performance").write_text(performance, encoding="utf-8")
        task = lambda recovery=False, library=False: SimpleNamespace(
            is_recovery_task=recovery, is_library_task=library
        )
        dp = SimpleNamespace(tasks={(1, 1): task(), (2, 1): task()})

        evidence = generate_metric_facts(str(self.run), dp)

        self.assertFalse(evidence["partial_reasons"])
        self.assertTrue(
            all(item["status"] == "PASS" for item in evidence["contracts"].values()),
            evidence["contracts"],
        )
        with (self.run / "csv-files" / "metric_task_latency.csv").open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual([row["outcome"] for row in rows], ["SUCCESS", "UNKNOWN"])
        self.assertEqual(float(rows[0]["execution_seconds"]), 0.5)
        with (self.run / "csv-files" / "metric_transfer_throughput.csv").open(newline="") as handle:
            transfer = list(csv.DictReader(handle))
        self.assertEqual(sum(float(row["input_bytes"]) for row in transfer), 100)

    def test_incomplete_run_fails_closed(self):
        (self.run / "vine-logs" / "transactions").write_text(
            "1000000 7 MANAGER 7 START 0\n"
            "1200000 7 TASK 1 READY alpha FIRST_RESOURCES 1 {}\n",
            encoding="utf-8",
        )
        dp = SimpleNamespace(tasks={(1, 1): SimpleNamespace(is_recovery_task=False, is_library_task=False)})
        evidence = generate_metric_facts(str(self.run), dp)
        self.assertIn("metric_manager_end_missing", evidence["partial_reasons"])
        self.assertEqual(evidence["contracts"]["task-throughput"]["status"], "PARTIAL")

    def test_cross_source_mismatch_is_assigned_to_affected_metrics(self):
        (self.run / "vine-logs" / "transactions").write_text(
            "1000000 7 MANAGER 7 START 0\n"
            "1100000 7 WORKER worker-a CONNECTION host:1\n"
            "1200000 7 WORKER worker-a TRANSFER INPUT file-a 100 1000 1199000\n"
            "1300000 7 WORKER worker-a DISCONNECTION EXPLICIT\n"
            "2000000 7 MANAGER 7 END 1000000\n",
            encoding="utf-8",
        )
        (self.run / "vine-logs" / "performance").write_text(
            "# timestamp workers_connected tasks_running total_cores committed_cores bytes_sent bytes_received workers_joined\n"
            "1000000 1 0 4 0 0 0 0\n"
            "2000000 0 0 0 0 50 0 2\n",
            encoding="utf-8",
        )
        evidence = generate_metric_facts(str(self.run), SimpleNamespace(tasks={}))
        self.assertEqual(evidence["contracts"]["transfer-throughput"]["status"], "PARTIAL")
        self.assertEqual(evidence["contracts"]["worker-churn"]["status"], "PARTIAL")
        self.assertEqual(evidence["contracts"]["worker-capacity"]["status"], "PASS")


if __name__ == "__main__":
    unittest.main()
