from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests


BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"


def load_env_file(file_path: Path) -> None:
    if not file_path.exists():
        return
    for raw_line in file_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


load_env_file(ENV_PATH)


@dataclass
class ExchangeConfig:
    api_key: str = os.getenv("BINGX_API_KEY", "")
    secret_key: str = os.getenv("BINGX_SECRET_KEY", "")
    paper_trading: bool = parse_bool(os.getenv("PAPER", "true"))
    default_symbol: str = os.getenv("SYMBOL", "ETH-USDT")
    qty_precision: int = int(os.getenv("QTY_PRECISION", "6"))
    price_precision: int = int(os.getenv("PRICE_PRECISION", "2"))
    recv_window: int = int(os.getenv("RECV_WINDOW", "5000"))


@dataclass
class OrderRequest:
    symbol: str
    side: str
    quantity: float


@dataclass
class ProtectionRequest:
    symbol: str
    direction: str
    stop_price: float
    take_profit_price: float
    quantity: Optional[float] = None
    price_precision: Optional[int] = None


class BingXExchange:
    base_url = "https://open-api.bingx.com"

    def __init__(self, config: Optional[ExchangeConfig] = None) -> None:
        self.config = config or ExchangeConfig()
        self.session = requests.Session()

    @property
    def paper_trading(self) -> bool:
        return self.config.paper_trading

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        normalized = symbol.strip().upper()
        if "-" in normalized:
            return normalized
        if normalized.endswith("USDT") and len(normalized) > 4:
            return f"{normalized[:-4]}-USDT"
        return normalized

    @staticmethod
    def direction_to_position_side(direction: str) -> str:
        normalized = direction.strip().lower()
        if normalized not in {"long", "short"}:
            raise ValueError(f"Unsupported direction: {direction}")
        return "LONG" if normalized == "long" else "SHORT"

    @staticmethod
    def side_to_position_side(side: str) -> str:
        normalized = side.strip().upper()
        if normalized not in {"BUY", "SELL"}:
            raise ValueError(f"Unsupported side: {side}")
        return "LONG" if normalized == "BUY" else "SHORT"

    def _generate_signature(self, params: Dict[str, Any]) -> str:
        query_string = "&".join(f"{key}={value}" for key, value in sorted(params.items()))
        return hmac.new(
            self.config.secret_key.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _request(self, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None, private: bool = False) -> Dict[str, Any]:
        params = dict(params or {})
        headers: Dict[str, str] = {}

        if private:
            if self.paper_trading:
                if "balance" in endpoint:
                    return {"code": 0, "data": {"balance": {"balance": "0"}}}
                if "positions" in endpoint:
                    return {"code": 0, "data": []}
                if "openOrders" in endpoint:
                    return {"code": 0, "data": {"orders": []}}
                if "allOpenOrders" in endpoint or "trade/order" in endpoint:
                    return {"code": 0, "data": {"orderId": f"paper_{int(time.time() * 1000)}"}}
                return {"code": 0, "data": {}}

            params["timestamp"] = int(time.time() * 1000)
            params["recvWindow"] = self.config.recv_window
            signature = self._generate_signature(params)
            query = "&".join(f"{key}={value}" for key, value in sorted(params.items()))
            url = f"{self.base_url}{endpoint}?{query}&signature={signature}"
            headers["X-BX-APIKEY"] = self.config.api_key
        else:
            url = f"{self.base_url}{endpoint}"

        try:
            if method == "GET":
                response = self.session.get(url, params=None if private else params, headers=headers, timeout=(10, 20))
            elif method == "POST":
                response = self.session.post(url, params=None if private else params, headers=headers, timeout=(10, 20))
            elif method == "DELETE":
                response = self.session.delete(url, params=None if private else params, headers=headers, timeout=(10, 20))
            else:
                return {"code": -1, "msg": f"Unsupported method: {method}"}
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            return {"code": -1, "msg": str(exc)}

    def health(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        target_symbol = self.normalize_symbol(symbol or self.config.default_symbol)
        if self.paper_trading:
            return {
                "ok": True,
                "mode": "paper",
                "symbol": target_symbol,
            }
        price = self.get_last_price(target_symbol)
        return {
            "ok": price > 0,
            "mode": "live",
            "symbol": target_symbol,
            "last_price": price,
        }

    def get_klines(self, symbol: str, interval: str = "1m", limit: int = 1) -> pd.DataFrame:
        response = self._request(
            "GET",
            "/openApi/swap/v3/quote/klines",
            {
                "symbol": self.normalize_symbol(symbol),
                "interval": interval,
                "limit": limit,
            },
            private=False,
        )
        if response.get("code") != 0:
            return pd.DataFrame()
        rows = response.get("data", [])
        if not rows:
            return pd.DataFrame()
        frame = pd.DataFrame(rows)
        frame["time"] = pd.to_datetime(frame["time"].astype("int64"), unit="ms", utc=True)
        for column in ["open", "high", "low", "close", "volume"]:
            frame[column] = frame[column].astype(float)
        return frame.set_index("time").sort_index()

    def get_last_price(self, symbol: str) -> float:
        frame = self.get_klines(symbol, interval="1m", limit=1)
        if frame.empty:
            return 0.0
        return float(frame["close"].iloc[-1])

    def get_balance(self) -> float:
        response = self._request("GET", "/openApi/swap/v2/user/balance", private=True)
        if response.get("code") != 0:
            return 0.0
        data = response.get("data", {})
        if isinstance(data, dict):
            balance = data.get("balance", {})
            if isinstance(balance, dict):
                for key in ("availableMargin", "balance", "equity"):
                    raw_value = balance.get(key)
                    if raw_value not in (None, "", "0", 0):
                        return float(raw_value)
            if "balance" in data and not isinstance(data["balance"], dict):
                return float(data["balance"] or 0.0)
        if isinstance(data, list) and data:
            return float(data[0].get("balance", 0) or 0.0)
        return 0.0

    def get_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        response = self._request("GET", "/openApi/swap/v2/user/positions", private=True)
        if response.get("code") != 0:
            return []
        positions = response.get("data", [])
        if symbol is None:
            return positions
        normalized_symbol = self.normalize_symbol(symbol)
        return [
            position
            for position in positions
            if self.normalize_symbol(str(position.get("symbol", ""))) == normalized_symbol
        ]

    def get_open_position(self, symbol: str, direction: str) -> Optional[Dict[str, Any]]:
        position_side = self.direction_to_position_side(direction)
        for position in self.get_positions(symbol):
            if str(position.get("positionSide", "")).upper() != position_side:
                continue
            amount = abs(float(position.get("positionAmt", 0) or 0))
            if amount > 0:
                return position
        return None

    def get_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        response = self._request(
            "GET",
            "/openApi/swap/v2/trade/openOrders",
            {"symbol": self.normalize_symbol(symbol)},
            private=True,
        )
        if response.get("code") != 0:
            return []
        data = response.get("data", {})
        if isinstance(data, dict):
            return data.get("orders", [])
        if isinstance(data, list):
            return data
        return []

    def place_market_order(self, request: OrderRequest) -> Dict[str, Any]:
        if request.quantity <= 0:
            raise ValueError("quantity must be positive")
        side = request.side.strip().upper()
        position_side = self.side_to_position_side(side)
        response = self._request(
            "POST",
            "/openApi/swap/v2/trade/order",
            {
                "symbol": self.normalize_symbol(request.symbol),
                "side": side,
                "positionSide": position_side,
                "type": "MARKET",
                "quantity": round(float(request.quantity), self.config.qty_precision),
            },
            private=True,
        )
        order_id = None
        data = response.get("data", {})
        if isinstance(data, dict):
            order_id = data.get("orderId")
            if order_id is None and isinstance(data.get("order"), dict):
                order_id = data["order"].get("orderId")
        return {
            "ok": response.get("code") == 0,
            "symbol": self.normalize_symbol(request.symbol),
            "side": side,
            "position_side": position_side,
            "quantity": round(float(request.quantity), self.config.qty_precision),
            "order_id": order_id,
            "paper_trading": self.paper_trading,
            "raw": response,
        }

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        response = self._request(
            "DELETE",
            "/openApi/swap/v2/trade/order",
            {
                "symbol": self.normalize_symbol(symbol),
                "orderId": order_id,
            },
            private=True,
        )
        return response.get("code") == 0

    def cancel_all_orders(self, symbol: str) -> bool:
        response = self._request(
            "DELETE",
            "/openApi/swap/v2/trade/allOpenOrders",
            {"symbol": self.normalize_symbol(symbol)},
            private=True,
        )
        return response.get("code") == 0

    def close_position_market(self, symbol: str, direction: str, quantity: Optional[float] = None) -> Dict[str, Any]:
        position = self.get_open_position(symbol, direction)
        if self.paper_trading and quantity is None:
            quantity = 0.0
        if position is None and not self.paper_trading:
            return {"ok": False, "msg": "No open position found"}
        resolved_quantity = quantity
        if resolved_quantity is None and position is not None:
            resolved_quantity = abs(float(position.get("positionAmt", 0) or 0))
        if resolved_quantity is None or resolved_quantity <= 0:
            if self.paper_trading:
                resolved_quantity = 0.0
            else:
                return {"ok": False, "msg": "Position size is zero"}
        side = "SELL" if direction.strip().lower() == "long" else "BUY"
        response = self._request(
            "POST",
            "/openApi/swap/v2/trade/order",
            {
                "symbol": self.normalize_symbol(symbol),
                "side": side,
                "positionSide": self.direction_to_position_side(direction),
                "type": "MARKET",
                "quantity": round(float(resolved_quantity), self.config.qty_precision),
            },
            private=True,
        )
        data = response.get("data", {})
        order_id = data.get("orderId") if isinstance(data, dict) else None
        return {
            "ok": response.get("code") == 0,
            "symbol": self.normalize_symbol(symbol),
            "direction": direction,
            "quantity": round(float(resolved_quantity), self.config.qty_precision),
            "order_id": order_id,
            "paper_trading": self.paper_trading,
            "raw": response,
        }

    def place_exit_order(self, symbol: str, position_side: str, quantity: float, trigger_price: float, order_type: str, price_precision: int) -> bool:
        response = self._request(
            "POST",
            "/openApi/swap/v2/trade/order",
            {
                "symbol": self.normalize_symbol(symbol),
                "side": "SELL" if position_side == "LONG" else "BUY",
                "positionSide": position_side,
                "type": order_type,
                "stopPrice": round(float(trigger_price), price_precision),
                "quantity": round(float(quantity), self.config.qty_precision),
                "workingType": "MARK_PRICE",
            },
            private=True,
        )
        return response.get("code") == 0

    def cancel_protection_orders(self, symbol: str, position_side: str) -> bool:
        success = True
        for order in self.get_open_orders(symbol):
            order_side = str(order.get("positionSide", "")).upper()
            order_type = str(order.get("type", "")).upper()
            reduce_only = bool(order.get("reduceOnly", False))
            if order_side != position_side or not reduce_only:
                continue
            if order_type not in {"STOP_MARKET", "TAKE_PROFIT_MARKET"}:
                continue
            order_id = str(order.get("orderId", "")).strip()
            if order_id:
                success = self.cancel_order(symbol, order_id) and success
        return success

    def set_protection_orders(self, request: ProtectionRequest) -> Dict[str, Any]:
        position = self.get_open_position(request.symbol, request.direction)
        if position is None and not self.paper_trading:
            return {"ok": False, "msg": "No open position found for protection orders"}
        position_side = self.direction_to_position_side(request.direction)
        if request.quantity is not None:
            final_qty = request.quantity
        elif position is not None:
            final_qty = abs(float(position.get("positionAmt", 0) or 0))
        else:
            final_qty = 0.0
        if final_qty <= 0 and not self.paper_trading:
            return {"ok": False, "msg": "Position size is zero"}
        price_precision = request.price_precision or self.config.price_precision
        cleared = self.cancel_protection_orders(request.symbol, position_side)
        stop_ok = self.place_exit_order(request.symbol, position_side, final_qty, request.stop_price, "STOP_MARKET", price_precision)
        take_ok = self.place_exit_order(
            request.symbol,
            position_side,
            final_qty,
            request.take_profit_price,
            "TAKE_PROFIT_MARKET",
            price_precision,
        )
        return {
            "ok": cleared and stop_ok and take_ok,
            "symbol": self.normalize_symbol(request.symbol),
            "direction": request.direction,
            "position_side": position_side,
            "quantity": round(float(final_qty), self.config.qty_precision),
            "stop_price": round(float(request.stop_price), price_precision),
            "take_profit_price": round(float(request.take_profit_price), price_precision),
            "paper_trading": self.paper_trading,
        }


def require_live_confirmation(exchange: BingXExchange, confirm_live: bool, action: str) -> None:
    if exchange.paper_trading:
        return
    if not confirm_live:
        raise RuntimeError(f"{action} requires --confirm-live when PAPER=false")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Safe BingX exchange wrapper for correction/live integration.")
    parser.add_argument("--paper", choices=["true", "false"], default=None, help="Override PAPER mode from .env")
    parser.add_argument("--confirm-live", action="store_true", help="Required for live order-changing actions")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name in ("health", "balance", "positions", "orders", "price"):
        sub = subparsers.add_parser(name)
        sub.add_argument("--symbol", default=None)

    trade_buy = subparsers.add_parser("buy")
    trade_buy.add_argument("--symbol", default=None)
    trade_buy.add_argument("--qty", type=float, required=True)

    trade_sell = subparsers.add_parser("sell")
    trade_sell.add_argument("--symbol", default=None)
    trade_sell.add_argument("--qty", type=float, required=True)

    close = subparsers.add_parser("close")
    close.add_argument("--symbol", default=None)
    close.add_argument("--direction", choices=["long", "short"], required=True)
    close.add_argument("--qty", type=float, default=None)

    cancel_all = subparsers.add_parser("cancel-all")
    cancel_all.add_argument("--symbol", default=None)

    protect = subparsers.add_parser("protect")
    protect.add_argument("--symbol", default=None)
    protect.add_argument("--direction", choices=["long", "short"], required=True)
    protect.add_argument("--stop-price", type=float, required=True)
    protect.add_argument("--take-profit-price", type=float, required=True)
    protect.add_argument("--qty", type=float, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = ExchangeConfig()
    if args.paper is not None:
        config.paper_trading = parse_bool(args.paper)
    exchange = BingXExchange(config)
    symbol = BingXExchange.normalize_symbol(args.symbol or config.default_symbol)

    if args.command == "health":
        payload = exchange.health(symbol)
    elif args.command == "balance":
        payload = {"ok": True, "paper_trading": exchange.paper_trading, "balance": exchange.get_balance()}
    elif args.command == "positions":
        payload = {"ok": True, "paper_trading": exchange.paper_trading, "positions": exchange.get_positions(symbol)}
    elif args.command == "orders":
        payload = {"ok": True, "paper_trading": exchange.paper_trading, "orders": exchange.get_open_orders(symbol)}
    elif args.command == "price":
        payload = {"ok": True, "paper_trading": exchange.paper_trading, "symbol": symbol, "price": exchange.get_last_price(symbol)}
    elif args.command == "buy":
        require_live_confirmation(exchange, args.confirm_live, "buy")
        payload = exchange.place_market_order(OrderRequest(symbol=symbol, side="BUY", quantity=args.qty))
    elif args.command == "sell":
        require_live_confirmation(exchange, args.confirm_live, "sell")
        payload = exchange.place_market_order(OrderRequest(symbol=symbol, side="SELL", quantity=args.qty))
    elif args.command == "close":
        require_live_confirmation(exchange, args.confirm_live, "close")
        payload = exchange.close_position_market(symbol=symbol, direction=args.direction, quantity=args.qty)
    elif args.command == "cancel-all":
        require_live_confirmation(exchange, args.confirm_live, "cancel-all")
        payload = {"ok": exchange.cancel_all_orders(symbol), "paper_trading": exchange.paper_trading, "symbol": symbol}
    elif args.command == "protect":
        require_live_confirmation(exchange, args.confirm_live, "protect")
        payload = exchange.set_protection_orders(
            ProtectionRequest(
                symbol=symbol,
                direction=args.direction,
                stop_price=args.stop_price,
                take_profit_price=args.take_profit_price,
                quantity=args.qty,
            )
        )
    else:
        raise RuntimeError(f"Unsupported command: {args.command}")

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
