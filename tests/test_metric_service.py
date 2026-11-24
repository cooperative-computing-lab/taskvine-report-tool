import csv
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from src.vine_report.metric_service import _bounded, metric_payload


class MetricServiceTest(unittest.TestCase):
    def test_downsampling_preserves_endpoints_and_extrema(self):
        points = [[index, 1] for index in range(2500)]
        points[777][1] = -10
        points[1777][1] = 20
        sampled = _bounded(points, maximum=1000)
        self.assertIn(points[0], sampled)
        self.assertIn(points[-1], sampled)
        self.assertIn(points[777], sampled)
        self.assertIn(points[1777], sampled)
        self.assertLessEqual(len(sampled), 1004)

    def test_source_rate_fills_empty_buckets_and_full_domain(self):
        with tempfile.TemporaryDirectory() as directory:
            csv_dir = Path(directory)

            def write(name, fields, rows):
                with (csv_dir / name).open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.writer(handle)
                    writer.writerow(fields)
                    writer.writerows(rows)

            write("metadata.csv", ["key", "value"], [
                ["manager_start_time", 1000], ["manager_end_time", 1040], ["manager_duration", 40],
            ])
            write("metric_contracts.csv", [
                "metric_id", "semantics_version", "status", "source", "coverage",
                "unit", "inclusion", "exclusions", "caveat",
            ], [["task-throughput", 1, "PASS", "transactions", 1, "attempts/s", "tasks", "", ""]])
            write("metric_task_throughput.csv", [
                "time", "bucket_seconds", "successful", "failed",
                "success_rate_per_second", "failure_rate_per_second",
            ], [[10, 10, 2, 0, 0.2, 0], [30, 10, 4, 0, 0.4, 0]])
            payload = metric_payload(SimpleNamespace(csv_files_dir=str(csv_dir)), "task-throughput")
            self.assertEqual(payload["axes"]["x"]["domain"], [0.0, 40.0])
            self.assertEqual(
                payload["series"][0]["points"],
                [[0.0, 0.0], [10.0, 0.2], [20.0, 0.0], [30.0, 0.4], [40.0, 0.4]],
            )


if __name__ == "__main__":
    unittest.main()
