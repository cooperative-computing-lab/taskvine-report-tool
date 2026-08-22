import csv
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from werkzeug.datastructures import MultiDict

from src.vine_parse.file_info import FileInfo, UnindexedTransferEvent
from src.vine_report.chart_service import (
    ChartQueryError,
    _sample_points,
    cache_occupancy_peak_bytes,
    chart_payload,
    normalize_query,
)
from src.vine_report.metric_registry import METRIC_SPECS, metric_catalog


class ChartServiceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.csv_dir = Path(self.temp.name) / "csv-files"
        self.csv_dir.mkdir()

    def tearDown(self):
        self.temp.cleanup()

    def write(self, name, fields, rows):
        with (self.csv_dir / name).open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(fields)
            writer.writerows(rows)

    def snapshot(self):
        return SimpleNamespace(csv_files_dir=str(self.csv_dir))

    def test_catalog_has_one_unique_definition_per_metric(self):
        catalog = metric_catalog()
        self.assertEqual(len(catalog), len(METRIC_SPECS))
        self.assertEqual(len({item["id"] for item in catalog}), len(catalog))
        storage = METRIC_SPECS["worker-storage-consumption"]
        self.assertEqual(storage.title, "Worker Cache Occupancy")
        self.assertEqual(storage.y_unit, "bytes")
        self.assertIn("total", storage.options["views"])

    def test_cache_occupancy_uses_event_size_and_exposes_evidence(self):
        self.write(
            "metric_worker_cache_occupancy.csv",
            ["time", "worker_id", "delta_bytes", "file_id", "file_name", "residency_id", "reason", "evidence"],
            [
                [0, "host:1:1", 10, 1, "a", "1:1", "cache_update", "observed"],
                [1, "host:2:1", 20, 2, "b", "2:1", "cache_update", "observed"],
                [2, "host:1:1", -10, 1, "a", "1:1", "unlink", "observed"],
                [3, "host:2:1", -20, 2, "b", "2:1", "synthetic_manager_end", "inferred"],
            ],
        )
        self.write(
            "metric_contracts.csv",
            ["metric_id", "semantics_version", "status", "source", "coverage", "unit", "inclusion", "exclusions", "caveat"],
            [["worker-cache-occupancy", 1, "PARTIAL", "debug", 1, "bytes", "cache", "sandbox", "one censored"]],
        )
        query = normalize_query(MultiDict([("view", "total"), ("max_points", "100")]))
        payload = chart_payload(self.snapshot(), "worker-storage-consumption", query)
        self.assertEqual(payload["contract"]["status"], "PARTIAL")
        self.assertEqual(max(point[1] for point in payload["series"][0]["points"]), 30)
        self.assertEqual(cache_occupancy_peak_bytes(self.csv_dir), 30)

        observed = chart_payload(
            self.snapshot(), "worker-storage-consumption",
            normalize_query(MultiDict([("view", "total"), ("evidence", "observed")])),
        )
        self.assertEqual(observed["series"][0]["points"][-1][1], 20)

        default_view = chart_payload(
            self.snapshot(), "worker-storage-consumption",
            normalize_query(MultiDict()),
        )
        self.assertEqual([item["id"] for item in default_view["series"]], ["total"])

    def test_legacy_sparse_peak_forward_fills_each_worker(self):
        self.write(
            "worker_storage_consumption.csv",
            ["time", "workflow_completion_percentage", "host:1:1", "host:2:1"],
            [[0, 0, 10, ""], [1, 50, "", 20], [2, 100, 0, 0]],
        )
        self.assertEqual(cache_occupancy_peak_bytes(self.csv_dir), 30 * 2**20)

    def test_transfer_size_is_immutable_per_residency(self):
        file = FileInfo("same-name", 1, 0)
        first = UnindexedTransferEvent("same-name", ("h", 1, 1), 0, size_mb=1)
        second = UnindexedTransferEvent("same-name", ("h", 1, 1), 2, size_mb=2)
        file.unindexed_transfers[("h", 1, 1)].extend([first, second])
        file.set_size_mb(9)
        self.assertEqual([item.size_mb for item in file.get_flattened_transfers()], [1, 2])

    def test_query_validation_is_typed_and_fail_closed(self):
        with self.assertRaises(ChartQueryError):
            normalize_query(MultiDict([("unknown", "x")]))
        with self.assertRaises(ChartQueryError):
            normalize_query(MultiDict([("start", "2"), ("end", "1")]))
        with self.assertRaises(ChartQueryError):
            normalize_query(MultiDict([("max_points", "999999")]))

    def test_state_sampling_preserves_global_extrema_and_reports_loss(self):
        points = [[index, 1000 if index == 73 else index % 11] for index in range(200)]
        sampled = _sample_points(points, 10, state=True)
        self.assertLessEqual(len(sampled), 10)
        self.assertIn([0, 0], sampled)
        self.assertIn([73, 1000], sampled)
        self.assertIn([199, 1], sampled)

        self.write(
            "worker_concurrency.csv",
            ["time", "Active Workers (count)"],
            [[index, 1000 if index == 73 else index % 11] for index in range(200)],
        )
        payload = chart_payload(
            self.snapshot(), "worker-concurrency",
            normalize_query(MultiDict([("max_points", "10")])),
        )
        self.assertEqual(payload["sampling"]["input_points"], 200)
        self.assertLessEqual(payload["sampling"]["output_points"], 10)
        self.assertTrue(payload["sampling"]["lossy"])
        self.assertEqual(max(point[1] for point in payload["series"][0]["points"]), 1000)

    def test_wide_metrics_bound_default_series_without_hiding_selection(self):
        workers = [f"worker-{index}:9000:1" for index in range(30)]
        self.write(
            "worker_outgoing_transfers.csv",
            ["time", "workflow_completion_percentage", *workers],
            [[0, 0, *range(30)], [1, 100, *range(30)]],
        )
        payload = chart_payload(
            self.snapshot(), "worker-outgoing-transfers",
            normalize_query(MultiDict()),
        )
        self.assertEqual(len(payload["series"]), 24)
        self.assertEqual(payload["metadata"]["available_series"], 30)
        self.assertEqual(payload["metadata"]["omitted_series"], 6)

        selected = chart_payload(
            self.snapshot(), "worker-outgoing-transfers",
            normalize_query(MultiDict([("series", workers[0]), ("series", workers[1])])),
        )
        self.assertEqual({item["id"] for item in selected["series"]}, set(workers[:2]))
        self.assertNotIn("omitted_series", selected["metadata"])

    def test_sparse_lifecycle_series_cover_canonical_domain(self):
        self.write(
            "metadata.csv", ["key", "value"],
            [["manager_start_time", 1000], ["manager_end_time", 1100], ["manager_duration", 100]],
        )
        self.write("time_domain.csv", ["MIN_TIME", "MAX_TIME"], [[0, 90]])
        self.write(
            "worker_executing_tasks.csv",
            ["time", "worker-a", "worker-b"],
            [[10, 0, ""], [20, 1, ""], [30, 0, 0], [40, "", 2], [50, "", 0]],
        )
        payload = chart_payload(
            self.snapshot(), "worker-executing-tasks", normalize_query(MultiDict()),
        )
        self.assertEqual(payload["axes"]["x"]["domain"], [0.0, 100.0])
        for series in payload["series"]:
            self.assertEqual(series["points"][0], [0.0, 0.0])
            self.assertEqual(series["points"][-1], [100.0, 0.0])

        windowed = chart_payload(
            self.snapshot(), "worker-executing-tasks",
            normalize_query(MultiDict([("start", "15"), ("end", "45")])),
        )
        self.assertEqual(windowed["axes"]["x"]["domain"], [15.0, 45.0])
        self.assertTrue(all(item["points"][0][0] == 15 for item in windowed["series"]))
        self.assertTrue(all(item["points"][-1][0] == 45 for item in windowed["series"]))

    def test_standard_time_linked_lines_declare_temporal_semantics(self):
        for spec in METRIC_SPECS.values():
            if spec.time_linked and spec.mode != "custom":
                self.assertIn(spec.temporal_policy, {"state", "bucket_zero"}, spec.id)


if __name__ == "__main__":
    unittest.main()
