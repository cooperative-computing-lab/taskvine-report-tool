import unittest

from src.vine_report.metric_service import _bounded


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


if __name__ == "__main__":
    unittest.main()
