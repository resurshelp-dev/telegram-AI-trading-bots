from __future__ import annotations

import os
import shutil
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from correction_regime import DataCache


class FakeClient:
    def __init__(self, frame: pd.DataFrame | None = None) -> None:
        self.frame = frame
        self.calls = 0

    def fetch_klines(self, symbol: str, interval: str, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        self.calls += 1
        if self.frame is None:
            raise AssertionError("fetch_klines should not be called")
        return self.frame.copy()


def sample_frame(start: str = "2026-03-20T00:00:00Z") -> pd.DataFrame:
    index = pd.date_range(start=start, periods=3, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "open": [1.0, 2.0, 3.0],
            "high": [1.5, 2.5, 3.5],
            "low": [0.5, 1.5, 2.5],
            "close": [1.2, 2.2, 3.2],
            "volume": [10.0, 11.0, 12.0],
        },
        index=index,
    )


class DataCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_root = Path(__file__).with_name(".tmp_test_cache")
        self.tmp_root.mkdir(exist_ok=True)
        self.case_root = self.tmp_root / self._testMethodName
        if self.case_root.exists():
            shutil.rmtree(self.case_root)
        self.case_root.mkdir()

    def tearDown(self) -> None:
        if self.case_root.exists():
            shutil.rmtree(self.case_root)

    def test_load_or_fetch_uses_legacy_cache_without_network_when_end_time_is_omitted(self) -> None:
        root = self.case_root
        cached = sample_frame()
        cached.index.name = "time"
        cached.reset_index().to_csv(root / "ETH_USDT_5m_30d.csv", index=False)

        cache = DataCache(root)
        client = FakeClient()

        loaded = cache.load_or_fetch(client, "ETH-USDT", "5m", 30)

        self.assertEqual(client.calls, 0)
        pd.testing.assert_frame_equal(loaded, cached, check_freq=False)

    def test_load_or_fetch_uses_latest_matching_snapshot_when_only_timestamped_cache_exists(self) -> None:
        root = self.case_root
        older = sample_frame("2026-03-18T00:00:00Z")
        newer = sample_frame("2026-03-19T00:00:00Z")
        older.index.name = "time"
        newer.index.name = "time"
        older_path = root / "ETH_USDT_5m_30d_20260318T235959Z.csv"
        newer_path = root / "ETH_USDT_5m_30d_20260319T235959Z.csv"
        older.reset_index().to_csv(older_path, index=False)
        newer.reset_index().to_csv(newer_path, index=False)
        os.utime(older_path, (1, 1))
        os.utime(newer_path, (2, 2))

        cache = DataCache(root)
        client = FakeClient()

        loaded = cache.load_or_fetch(client, "ETH-USDT", "5m", 30)

        self.assertEqual(client.calls, 0)
        pd.testing.assert_frame_equal(loaded, newer, check_freq=False)

    def test_load_or_fetch_persists_unsuffixed_cache_for_live_window(self) -> None:
        root = self.case_root
        fetched = sample_frame()
        cache = DataCache(root)
        client = FakeClient(fetched)

        loaded = cache.load_or_fetch(client, "ETH-USDT", "5m", 30)

        self.assertEqual(client.calls, 1)
        self.assertTrue((root / "ETH_USDT_5m_30d.csv").exists())
        pd.testing.assert_frame_equal(loaded, fetched, check_freq=False)

    def test_load_or_fetch_keeps_exact_timestamped_cache_for_fixed_end_time(self) -> None:
        root = self.case_root
        end_time = datetime(2026, 1, 20, 23, 59, 59, tzinfo=timezone.utc)
        cached = sample_frame("2026-01-20T00:00:00Z")
        cached.index.name = "time"
        exact_path = root / "ETH_USDT_5m_30d_20260120T235959Z.csv"
        cached.reset_index().to_csv(exact_path, index=False)

        cache = DataCache(root)
        client = FakeClient()

        loaded = cache.load_or_fetch(client, "ETH-USDT", "5m", 30, end_time=end_time)

        self.assertEqual(client.calls, 0)
        pd.testing.assert_frame_equal(loaded, cached, check_freq=False)


if __name__ == "__main__":
    unittest.main()
