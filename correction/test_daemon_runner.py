from __future__ import annotations

import json
import os
import shutil
import unittest
from pathlib import Path

from correction_daemon import (
    TelegramNotifier,
    append_event,
    default_paper_state,
    format_telegram_message,
    maybe_close_paper_trade,
    open_paper_trade,
    paper_summary,
    save_json,
)


class DaemonRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_root = Path(__file__).with_name(".tmp_test_daemon")
        self.tmp_root.mkdir(exist_ok=True)
        self.case_root = self.tmp_root / self._testMethodName
        if self.case_root.exists():
            shutil.rmtree(self.case_root)
        self.case_root.mkdir()

    def tearDown(self) -> None:
        if self.case_root.exists():
            shutil.rmtree(self.case_root)

    def test_append_event_writes_jsonl_line(self) -> None:
        path = self.case_root / "events.jsonl"
        append_event(path, {"type": "heartbeat", "ok": True})
        lines = path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["type"], "heartbeat")

    def test_save_json_writes_payload(self) -> None:
        path = self.case_root / "state.json"
        save_json(path, {"status": "running"})
        payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(payload["status"], "running")

    def test_format_telegram_message_includes_bot_tag_and_fields(self) -> None:
        previous_tag = os.environ.get("BOT_TAG")
        os.environ["BOT_TAG"] = "correction-live"
        try:
            message = format_telegram_message("heartbeat", {"symbol": "ETH-USDT", "quantity": 0.01, "msg": ""})
        finally:
            if previous_tag is None:
                os.environ.pop("BOT_TAG", None)
            else:
                os.environ["BOT_TAG"] = previous_tag
        self.assertIn("[correction-live] heartbeat", message)
        self.assertIn("symbol: ETH-USDT", message)
        self.assertIn("quantity: 0.0100", message)
        self.assertNotIn("msg:", message)

    def test_telegram_notifier_uses_send_message_endpoint(self) -> None:
        calls = []

        def fake_sender(url: str, payload: dict) -> dict:
            calls.append((url, payload))
            return {"ok": True}

        notifier = TelegramNotifier("token123", "chat456", "correction-live", sender=fake_sender)
        result = notifier.send("hello")

        self.assertTrue(result["ok"])
        self.assertEqual(len(calls), 1)
        url, payload = calls[0]
        self.assertEqual(url, "https://api.telegram.org/bottoken123/sendMessage")
        self.assertEqual(payload["chat_id"], "chat456")
        self.assertEqual(payload["text"], "hello")

    def test_open_paper_trade_creates_open_position(self) -> None:
        state = default_paper_state(10000.0)
        execution_payload = {
            "quantity": 2.0,
            "plan": {
                "source": "trend",
                "strategy_name": "profit_max_locked",
                "module_name": "hq_rule",
                "direction": "long",
                "signal_time": "2026-03-21T00:00:00+00:00",
                "symbol": "ETH-USDT",
                "entry_price": 100.0,
                "stop_price": 95.0,
                "tp1_price": 105.0,
                "tp2_price": 110.0,
            },
        }
        trade = open_paper_trade(state, execution_payload, max_hold_minutes=60, fee_per_side=0.0005)
        self.assertIsNotNone(trade)
        self.assertEqual(state["open_trade"]["symbol"], "ETH-USDT")
        self.assertEqual(state["open_trade"]["quantity_initial"], 2.0)

    def test_maybe_close_paper_trade_hits_tp2_and_updates_summary(self) -> None:
        trades_path = self.case_root / "paper_trades.jsonl"
        state = default_paper_state(10000.0)
        open_paper_trade(
            state,
            {
                "quantity": 2.0,
                "plan": {
                    "source": "trend",
                    "strategy_name": "profit_max_locked",
                    "module_name": "hq_rule",
                    "direction": "long",
                    "signal_time": "2026-03-21T00:00:00+00:00",
                    "symbol": "ETH-USDT",
                    "entry_price": 100.0,
                    "stop_price": 95.0,
                    "tp1_price": 105.0,
                    "tp2_price": 110.0,
                },
            },
            max_hold_minutes=60,
            fee_per_side=0.0,
        )
        closed = maybe_close_paper_trade(state, {"high": 111.0, "low": 106.0, "close": 109.0}, trades_path)
        self.assertIsNotNone(closed)
        self.assertEqual(closed["exit_reason"], "tp2")
        self.assertIsNone(state["open_trade"])
        summary = paper_summary(state)
        self.assertEqual(summary["trades"], 1)
        self.assertGreater(summary["net_pnl"], 0.0)


if __name__ == "__main__":
    unittest.main()
