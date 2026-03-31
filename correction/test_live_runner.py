from __future__ import annotations

import unittest

from correction_live import compute_order_quantity, recalculate_targets_from_model, recalculate_targets_from_rr


class LiveRunnerTests(unittest.TestCase):
    def test_recalculate_targets_from_model_long(self) -> None:
        tp1, tp2 = recalculate_targets_from_model("long", 100.0, 95.0, 104.0, 108.0, 101.0)
        self.assertAlmostEqual(tp1, 105.8)
        self.assertAlmostEqual(tp2, 110.6)

    def test_recalculate_targets_from_model_short(self) -> None:
        tp1, tp2 = recalculate_targets_from_model("short", 100.0, 105.0, 96.0, 92.0, 99.0)
        self.assertAlmostEqual(tp1, 94.2)
        self.assertAlmostEqual(tp2, 89.4)

    def test_recalculate_targets_from_rr(self) -> None:
        tp1, tp2 = recalculate_targets_from_rr("long", 100.0, 95.0, 0.5, 1.0)
        self.assertAlmostEqual(tp1, 102.5)
        self.assertAlmostEqual(tp2, 105.0)

    def test_compute_order_quantity(self) -> None:
        quantity = compute_order_quantity(10.0, 0.01, 100.0, 99.0, 6)
        self.assertAlmostEqual(quantity, 0.1)


if __name__ == "__main__":
    unittest.main()
