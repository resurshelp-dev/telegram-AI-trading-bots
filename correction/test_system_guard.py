from __future__ import annotations

import shutil
import unittest
from pathlib import Path

import correction_system_daemon as system_daemon


class SystemGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_root = Path(__file__).with_name(".tmp_test_system")
        self.tmp_root.mkdir(exist_ok=True)
        self.case_root = self.tmp_root / self._testMethodName
        if self.case_root.exists():
            shutil.rmtree(self.case_root)
        self.case_root.mkdir()
        self.original_lock_path = system_daemon.LOCK_PATH
        system_daemon.LOCK_PATH = self.case_root / "system.lock"

    def tearDown(self) -> None:
        system_daemon.LOCK_PATH = self.original_lock_path
        if self.case_root.exists():
            shutil.rmtree(self.case_root)

    def test_acquire_and_release_lock(self) -> None:
        system_daemon.acquire_lock()
        self.assertTrue(system_daemon.LOCK_PATH.exists())
        system_daemon.release_lock()
        self.assertFalse(system_daemon.LOCK_PATH.exists())

    def test_second_lock_raises(self) -> None:
        system_daemon.acquire_lock()
        with self.assertRaises(RuntimeError):
            system_daemon.acquire_lock()
        system_daemon.release_lock()


if __name__ == "__main__":
    unittest.main()
