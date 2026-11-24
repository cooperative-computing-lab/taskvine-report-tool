import csv
import json
import tempfile
import unittest
from pathlib import Path

from src.vine_report.temporal_series import (
    TimeContext,
    normalize_bucket_points,
    normalize_state_points,
    normalize_temporal_points,
    time_context,
)


class TemporalSeriesTest(unittest.TestCase):
    def test_state_policies_cover_both_boundaries(self):
        domain = (0.0, 20.0)
        points = [[5.0, 0.0], [7.0, 2.0], [12.0, 3.0]]
        held = normalize_state_points(points, domain)
        self.assertEqual((held[0], held[-1]), ([0.0, 0.0], [20.0, 3.0]))

    def test_bucket_policy_materializes_empty_intervals(self):
        points = normalize_bucket_points([[10, 3], [30, 4]], (0, 40), 10)
        self.assertEqual(points, [[0, 0.0], [10, 3], [20, 0.0], [30, 4], [40, 4]])

    def test_debug_points_shift_to_manager_clock(self):
        context = TimeContext((0.0, 100.0), debug_offset=12.0)
        points = normalize_temporal_points(
            [[0, 1], [80, 0]], context, origin="debug", policy="state",
        )
        self.assertEqual(points[0], [0.0, 0.0])
        self.assertIn([12.0, 1.0], points)
        self.assertEqual(points[-1], [100.0, 0.0])

    def test_time_context_prefers_manager_duration_and_epoch_offset(self):
        with tempfile.TemporaryDirectory() as directory:
            csv_dir = Path(directory)
            with (csv_dir / "metadata.csv").open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(["key", "value"])
                writer.writerows([
                    ["manager_start_time", json.dumps(1_000_000_000.0)],
                    ["manager_end_time", json.dumps(1_000_000_100.0)],
                    ["manager_duration", json.dumps(100.0)],
                ])
            with (csv_dir / "time_domain.csv").open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(["MIN_TIME", "MAX_TIME"])
                writer.writerow([1_000_000_012.0, 1_000_000_092.0])
            context = time_context(str(csv_dir))
            self.assertEqual(context.domain, (0.0, 100.0))
            self.assertEqual(context.debug_offset, 12.0)


if __name__ == "__main__":
    unittest.main()
