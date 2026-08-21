import tempfile
import unittest
from pathlib import Path

import polars as pl

from src.utils import (
    cumulative_size_frame,
    parse_worker_time_list,
    read_csv_frame,
    rows_to_frame,
    file_list_formatter,
    file_size_column,
)
from src.vine_report.routes.worker_storage_consumption import aggregate_storage_data


class PolarsBoundaryTest(unittest.TestCase):
    def test_csv_boundary_returns_polars(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.csv"
            path.write_text("time,value\n0,1\n1,2\n", encoding="utf-8")
            frame = read_csv_frame(path)

        self.assertIsInstance(frame, pl.DataFrame)
        self.assertEqual(frame.to_dicts(), [{"time": 0, "value": 1}, {"time": 1, "value": 2}])

    def test_legacy_size_deltas_are_sorted_grouped_and_clamped(self):
        frame = pl.DataFrame(
            {"time": [2, 1, 1, 3], "delta_size_mb": [4, 10, -3, -20]}
        )
        result = cumulative_size_frame(frame)
        self.assertEqual(result["time"].to_list(), [1.0, 2.0, 3.0])
        self.assertEqual(result["cumulative_size_mb"].to_list(), [7.0, 11.0, 0.0])

    def test_worker_storage_forward_fills_each_worker_before_sum(self):
        frame = pl.DataFrame(
            {
                "time": [0.0, 1.0, 2.0],
                "worker-a": [1.0, None, 3.0],
                "worker-b": [None, 2.0, None],
            }
        )
        self.assertEqual(
            aggregate_storage_data(frame),
            [(0.0, 1.0), (1.0, 3.0), (2.0, 5.0)],
        )

    def test_worker_time_csv_contract_is_a_safe_literal_list(self):
        self.assertEqual(parse_worker_time_list("[0.0, 1.25]"), [0.0, 1.25])
        self.assertEqual(parse_worker_time_list("__import__('os').getcwd()"), [])

    def test_sparse_rows_scan_all_values_before_fixing_numeric_schema(self):
        rows = [{"time": float(i), "worker": 0} for i in range(100)]
        rows.append({"time": 100.0, "worker": 0.5})
        frame = rows_to_frame(rows)
        self.assertEqual(frame.schema["worker"], pl.Float64)
        self.assertEqual(frame["worker"][-1], 0.5)

    def test_file_lists_are_filtered_and_deterministic(self):
        self.assertEqual(
            file_list_formatter({"z", "a", "file-meta-hidden"}),
            "a, z",
        )

    def test_file_size_column_preserves_declared_unit(self):
        self.assertEqual(
            file_size_column(pl.DataFrame({"file_size_kb": [1.0]})),
            ("file_size_kb", "KB"),
        )

    def test_runtime_and_packaging_use_one_dataframe_engine(self):
        root = Path(__file__).resolve().parents[1]
        forbidden_engine = "pan" + "das"
        files = [*root.joinpath("src").rglob("*.py")]
        files.extend(
            [root / "requirements.txt", root / "setup.py", root / "conda-recipe/meta.yaml"]
        )
        offenders = [
            str(path.relative_to(root))
            for path in files
            if forbidden_engine in path.read_text(encoding="utf-8").lower()
        ]
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
