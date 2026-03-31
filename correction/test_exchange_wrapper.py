from __future__ import annotations

import unittest

from correction_exchange import (
    BingXExchange,
    ExchangeConfig,
    OrderRequest,
    ProtectionRequest,
    require_live_confirmation,
)


class ExchangeWrapperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.exchange = BingXExchange(
            ExchangeConfig(
                api_key="demo",
                secret_key="demo",
                paper_trading=True,
                default_symbol="ETH-USDT",
                qty_precision=6,
                price_precision=2,
            )
        )

    def test_normalize_symbol_accepts_dash_and_plain_usdt(self) -> None:
        self.assertEqual(BingXExchange.normalize_symbol("ETHUSDT"), "ETH-USDT")
        self.assertEqual(BingXExchange.normalize_symbol("eth-usdt"), "ETH-USDT")

    def test_paper_market_order_returns_fake_id(self) -> None:
        payload = self.exchange.place_market_order(OrderRequest(symbol="ETH-USDT", side="BUY", quantity=0.01))
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["position_side"], "LONG")
        self.assertTrue(str(payload["order_id"]).startswith("paper_"))

    def test_paper_close_position_returns_fake_id(self) -> None:
        payload = self.exchange.close_position_market("ETH-USDT", "long", quantity=0.01)
        self.assertTrue(payload["ok"])
        self.assertTrue(str(payload["order_id"]).startswith("paper_"))

    def test_paper_protection_orders_are_allowed(self) -> None:
        payload = self.exchange.set_protection_orders(
            ProtectionRequest(
                symbol="ETH-USDT",
                direction="long",
                stop_price=1800.0,
                take_profit_price=1900.0,
                quantity=0.01,
            )
        )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["position_side"], "LONG")

    def test_live_confirmation_is_required_for_real_actions(self) -> None:
        live_exchange = BingXExchange(ExchangeConfig(api_key="demo", secret_key="demo", paper_trading=False))
        with self.assertRaises(RuntimeError):
            require_live_confirmation(live_exchange, False, "buy")


if __name__ == "__main__":
    unittest.main()
