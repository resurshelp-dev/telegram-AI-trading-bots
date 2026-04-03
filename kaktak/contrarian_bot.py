import argparse
import hashlib
import hmac
import json
import logging
import os
import time
import warnings
from dataclasses import dataclass, fields
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

warnings.filterwarnings("ignore")

try:
    from tqdm import tqdm

    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


ROOT_DIR = Path(__file__).resolve().parents[1]
APP_DIR = Path(__file__).resolve().parent
DEFAULT_RUNTIME_DIR = APP_DIR / "runtime"
RUNTIME_DIR = Path(os.getenv("KAKTAK_RUNTIME_ROOT", str(DEFAULT_RUNTIME_DIR))).resolve()
DEFAULT_CONFIG_PATH = Path(os.getenv("KAKTAK_CONFIG_PATH", str(APP_DIR / "bot_config.json"))).resolve()
PATH_FIELDS = {
    "DATA_FILE",
    "LOG_FILE",
    "SUMMARY_FILE",
    "APP_LOG_FILE",
    "EVENT_LOG_FILE",
    "STATE_FILE",
    "FAILED_NOTIFICATIONS_FILE",
}


def _parse_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _coerce_config_value(raw_value: Any, default_value: Any) -> Any:
    if isinstance(default_value, bool):
        return raw_value if isinstance(raw_value, bool) else _parse_bool(raw_value)
    if isinstance(default_value, int) and not isinstance(default_value, bool):
        return int(raw_value)
    if isinstance(default_value, float):
        return float(raw_value)
    return raw_value


@dataclass
class Config:
    SYMBOL: str = "BTCUSDT"
    BINGX_SYMBOL: str = "BTC-USDT"
    TIMEFRAME: str = "15min"
    TREND_TIMEFRAME: str = "60min"

    RISK_PER_TRADE: float = 0.015
    INITIAL_CAPITAL: float = 10000.0
    MAX_CONSECUTIVE_LOSSES: int = 5
    RISK_REWARD_RATIO: float = 2.5

    LEVEL_LOOKBACK: int = 120
    LEVEL_PROXIMITY: float = 0.002
    TREND_EMA_PERIOD: int = 50

    BREAKEVEN_TRIGGER_R: float = 1.0
    TRAIL_TRIGGER_R: float = 1.5
    TRAIL_DISTANCE_R: float = 1.0
    STRONG_TREND_THRESHOLD_R: float = 1.2
    HIGH_VOL_THRESHOLD: float = 1.2
    LOW_VOL_THRESHOLD: float = 0.85

    MIN_QTY: float = 0.0001
    QTY_PRECISION: int = 6
    PRICE_PRECISION: int = 1

    DATA_FILE: str = str(ROOT_DIR / "data_cache" / "BTC_USDT_5m_60d.csv")
    LOG_FILE: str = str(RUNTIME_DIR / "trade_log.json")
    SUMMARY_FILE: str = str(RUNTIME_DIR / "backtest_summary.json")
    APP_LOG_FILE: str = str(RUNTIME_DIR / "bot.log")
    EVENT_LOG_FILE: str = str(RUNTIME_DIR / "events.jsonl")
    STATE_FILE: str = str(RUNTIME_DIR / "runtime_state.json")
    FAILED_NOTIFICATIONS_FILE: str = str(RUNTIME_DIR / "notifications_fallback.log")

    VERBOSE: bool = True
    SHOW_PROGRESS: bool = True
    PAPER_TRADING: bool = True
    POLL_SECONDS: int = 5
    COOLDOWN_AFTER_MAX_LOSSES_HOURS: int = 24
    CLEANUP_EVERY_LOOPS: int = 12
    API_CONNECT_TIMEOUT_SECONDS: float = 10.0
    API_READ_TIMEOUT_SECONDS: float = 30.0
    KLINES_MAX_RETRIES: int = 3
    KLINES_RETRY_DELAY_SECONDS: float = 1.5
    STARTUP_SYNC: bool = True
    CLEAN_STALE_ORDERS: bool = True
    BOT_TAG: str = "kaktak"
    HEARTBEAT_MINUTES: int = 15
    STALL_ALERT_MINUTES: int = 20
    SMOKE_TEST_QTY: float = 0.0001
    SMOKE_TEST_SIDE: str = "BUY"
    ENABLE_STATE_RECOVERY: bool = True

    API_KEY: str = ""
    SECRET_KEY: str = ""
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""
    TELEGRAM_CONNECT_TIMEOUT_SECONDS: float = 5.0
    TELEGRAM_READ_TIMEOUT_SECONDS: float = 10.0
    CONFIG_FILE: str = str(DEFAULT_CONFIG_PATH)

    @classmethod
    def from_file(cls, file_path: str) -> "Config":
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        raw_data = json.loads(path.read_text(encoding="utf-8"))
        default_config = cls()
        normalized: Dict[str, Any] = {}
        for field_info in fields(cls):
            key = field_info.name
            if key not in raw_data:
                continue
            value = _coerce_config_value(raw_data[key], getattr(default_config, key))
            if key in PATH_FIELDS and isinstance(value, str):
                candidate = Path(value)
                value = str(candidate if candidate.is_absolute() else (path.parent / candidate).resolve())
            normalized[key] = value

        config = cls(**normalized)
        config.CONFIG_FILE = str(path.resolve())
        config.BINGX_SYMBOL = BingXTrader.normalize_symbol(config.SYMBOL)
        return config


def ensure_runtime_dirs(config: Config) -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    for value in (
        config.LOG_FILE,
        config.SUMMARY_FILE,
        config.APP_LOG_FILE,
        config.EVENT_LOG_FILE,
        config.STATE_FILE,
        config.FAILED_NOTIFICATIONS_FILE,
    ):
        Path(value).parent.mkdir(parents=True, exist_ok=True)


def setup_logging(config: Config) -> logging.Logger:
    ensure_runtime_dirs(config)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO if config.VERBOSE else logging.WARNING)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO if config.VERBOSE else logging.WARNING)

    file_handler = RotatingFileHandler(
        config.APP_LOG_FILE,
        maxBytes=2_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    root.addHandler(console_handler)
    root.addHandler(file_handler)
    return logging.getLogger("kaktak_bot")


class TelegramNotifier:
    def __init__(
        self,
        token: str,
        chat_id: str,
        logger: logging.Logger,
        connect_timeout: float = 5.0,
        read_timeout: float = 10.0,
    ):
        self.token = token.strip()
        self.chat_id = chat_id.strip()
        self.logger = logger.getChild("telegram")
        self.enabled = bool(self.token and self.chat_id)
        self.disabled_reason = ""
        self.session = requests.Session()
        self.api_url = f"https://api.telegram.org/bot{self.token}" if self.token else ""
        self.chat_validated = False
        self.request_timeout = (connect_timeout, read_timeout)
        self.network_failures = 0

        if not self.enabled and (self.token or self.chat_id):
            missing_parts = []
            if not self.token:
                missing_parts.append("TELEGRAM_BOT_TOKEN")
            if not self.chat_id:
                missing_parts.append("TELEGRAM_CHAT_ID")
            self.logger.warning(
                "Telegram notifier is disabled because %s is missing.",
                ", ".join(missing_parts),
            )

    @staticmethod
    def _mask_chat_id(chat_id: str) -> str:
        if len(chat_id) <= 4:
            return chat_id
        return f"{chat_id[:2]}***{chat_id[-4:]}"

    def _disable(self, reason: str) -> None:
        if not self.enabled:
            return
        self.enabled = False
        self.disabled_reason = reason
        self.logger.warning("Telegram notifier disabled: %s", reason)

    @staticmethod
    def _extract_error(response: requests.Response) -> Tuple[int, str]:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        error_code = int(payload.get("error_code") or response.status_code or 0)
        description = str(payload.get("description") or response.text or "").strip()
        return error_code, description

    def _call_api(
        self,
        method: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        timeout: Optional[Tuple[float, float]] = None,
    ) -> requests.Response:
        return self.session.post(
            f"{self.api_url}/{method}",
            params=params,
            json=payload,
            timeout=timeout or self.request_timeout,
        )

    @staticmethod
    def _is_permanent_failure(status_code: int, error_code: int, description: str) -> bool:
        description_lower = description.lower()
        if status_code == 429 or error_code == 429:
            return False
        if status_code == 401:
            return True
        if status_code == 403:
            return True
        if status_code == 404:
            return True
        permanent_markers = (
            "chat not found",
            "bot was blocked",
            "bot was kicked",
            "user is deactivated",
            "user not found",
            "have no rights to send a message",
            "group chat was upgraded",
            "forbidden",
            "unauthorized",
        )
        if any(marker in description_lower for marker in permanent_markers):
            return True
        if status_code == 400 and description_lower:
            return True
        return False

    def _build_disable_reason(self, error_code: int, description: str) -> str:
        description_lower = description.lower()
        if "chat not found" in description_lower:
            return (
                "Telegram chat_id was not found. Check TELEGRAM_CHAT_ID in bot_config.json "
                f"(current: {self._mask_chat_id(self.chat_id)}) and make sure the bot is added to that chat."
            )
        if "bot was blocked" in description_lower or "forbidden" in description_lower:
            return "Telegram bot cannot post to the target chat. Unblock it or add it back to the chat, then restart the bot."
        if "unauthorized" in description_lower or error_code == 401:
            return "Telegram bot token is invalid. Update TELEGRAM_BOT_TOKEN in bot_config.json and restart the bot."
        return f"Telegram rejected the configuration ({description or f'error_code={error_code}'}). Fix the Telegram settings and restart the bot."

    def validate_chat(self) -> bool:
        if not self.enabled or self.chat_validated:
            return self.enabled
        try:
            response = self._call_api("getChat", params={"chat_id": self.chat_id})
            ok = response.status_code == 200 and response.json().get("ok") is True
            if ok:
                self.chat_validated = True
                self.network_failures = 0
                return True

            error_code, description = self._extract_error(response)
            if self._is_permanent_failure(response.status_code, error_code, description):
                self._disable(self._build_disable_reason(error_code, description))
            else:
                self.logger.warning("Telegram chat validation failed: %s", description or response.text)
            return False
        except Exception as exc:
            self.network_failures += 1
            if self.network_failures >= 3:
                self._disable(f"Telegram network access is failing repeatedly: {exc}")
            self.logger.warning("Telegram chat validation error: %s", exc)
            return False

    def send(self, text: str) -> bool:
        if not self.enabled:
            return False
        if not self.chat_validated and not self.validate_chat():
            return False
        try:
            response = self._call_api("sendMessage", payload={"chat_id": self.chat_id, "text": text})
            ok = response.status_code == 200 and response.json().get("ok") is True
            if not ok:
                error_code, description = self._extract_error(response)
                if self._is_permanent_failure(response.status_code, error_code, description):
                    self._disable(self._build_disable_reason(error_code, description))
                else:
                    self.logger.warning("Telegram send failed: %s", description or response.text)
            else:
                self.network_failures = 0
            return ok
        except Exception as exc:
            self.network_failures += 1
            if self.network_failures >= 3:
                self._disable(f"Telegram network access is failing repeatedly: {exc}")
            self.logger.warning("Telegram send error: %s", exc)
            return False


def _ensure_parent_dir(file_path: str) -> None:
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)


def _to_api_interval(interval: str) -> str:
    mapping = {
        "1min": "1m",
        "3min": "3m",
        "5min": "5m",
        "15min": "15m",
        "30min": "30m",
        "60min": "1h",
        "120min": "2h",
        "240min": "4h",
        "1d": "1d",
    }
    return mapping.get(interval, interval)


def load_and_prepare_data(
    file_path: str,
    target_tf: str = "15min",
    lookback_days: Optional[int] = None,
) -> pd.DataFrame:
    candidates = [
        Path(file_path),
        Path(f"{file_path}.csv"),
        ROOT_DIR / "btc_usdt_1m_3months.csv",
        ROOT_DIR / "btc_chunk_001.csv",
        ROOT_DIR / "btc_chunk_002.csv",
        ROOT_DIR / "data_cache" / "BTC_USDT_5m_60d.csv",
    ]

    source_path: Optional[Path] = None
    for candidate in candidates:
        if candidate.exists():
            source_path = candidate
            break

    if source_path is None:
        available_csv = [p.name for p in ROOT_DIR.glob("*.csv")]
        raise FileNotFoundError(
            f"CSV file not found. Available root CSV files: {available_csv}"
        )

    df = pd.read_csv(source_path)
    time_cols = [col for col in df.columns if any(x in col.lower() for x in ["time", "date", "timestamp"])]
    time_col = time_cols[0] if time_cols else df.columns[0]

    if pd.api.types.is_numeric_dtype(df[time_col]):
        first_value = float(df[time_col].iloc[0])
        unit = "ms" if first_value > 1e11 else "s"
        df[time_col] = pd.to_datetime(df[time_col], unit=unit, utc=True, errors="coerce")
    else:
        df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")

    df = df.dropna(subset=[time_col]).copy()
    df.set_index(time_col, inplace=True)

    price_map: Dict[str, str] = {}
    for col in df.columns:
        col_lower = col.lower()
        if "open" in col_lower:
            price_map["open"] = col
        elif "high" in col_lower:
            price_map["high"] = col
        elif "low" in col_lower:
            price_map["low"] = col
        elif "close" in col_lower:
            price_map["close"] = col
        elif "volume" in col_lower or col_lower == "vol":
            price_map["volume"] = col

    for required in ["open", "high", "low", "close", "volume"]:
        if required not in price_map and required in df.columns:
            price_map[required] = required

    missing = [col for col in ["open", "high", "low", "close", "volume"] if col not in price_map]
    if missing:
        raise ValueError(f"Missing OHLCV columns: {missing}")

    df = df[[price_map["open"], price_map["high"], price_map["low"], price_map["close"], price_map["volume"]]].copy()
    df.columns = ["open", "high", "low", "close", "volume"]
    df = df.astype(float)
    df = df[~df.index.duplicated(keep="first")]
    df.sort_index(inplace=True)

    df = df.resample(target_tf).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    ).dropna()

    if lookback_days:
        cutoff = df.index.max() - pd.Timedelta(days=lookback_days)
        df = df[df.index >= cutoff].copy()

    return df


def calculate_ema(df: pd.DataFrame, period: int) -> pd.Series:
    return df["close"].ewm(span=period, adjust=False).mean()


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def detect_pinbar(candle: pd.Series) -> Optional[str]:
    open_, high, low, close = candle["open"], candle["high"], candle["low"], candle["close"]
    body = abs(close - open_)
    total_range = high - low

    if total_range == 0:
        return None

    body_ratio = body / total_range
    lower_shadow = min(open_, close) - low
    upper_shadow = high - max(open_, close)

    if body_ratio < 0.3:
        if lower_shadow > 2 * body and upper_shadow < body:
            return "bullish"
        if upper_shadow > 2 * body and lower_shadow < body:
            return "bearish"

    return None


def find_support_resistance_levels(df: pd.DataFrame, lookback: int = 120, proximity: float = 0.002) -> List[float]:
    levels: List[float] = []
    data = df.tail(lookback).copy()

    if len(data) < 10:
        return levels

    for i in range(2, len(data) - 2):
        if (
            data["high"].iloc[i] > data["high"].iloc[i - 1]
            and data["high"].iloc[i] > data["high"].iloc[i - 2]
            and data["high"].iloc[i] > data["high"].iloc[i + 1]
            and data["high"].iloc[i] > data["high"].iloc[i + 2]
        ):
            levels.append(round(data["high"].iloc[i], 1))

    for i in range(2, len(data) - 2):
        if (
            data["low"].iloc[i] < data["low"].iloc[i - 1]
            and data["low"].iloc[i] < data["low"].iloc[i - 2]
            and data["low"].iloc[i] < data["low"].iloc[i + 1]
            and data["low"].iloc[i] < data["low"].iloc[i + 2]
        ):
            levels.append(round(data["low"].iloc[i], 1))

    levels = sorted(set(levels))
    merged: List[float] = []
    threshold = proximity * df["close"].iloc[-1]

    for level in levels:
        if not merged or abs(level - merged[-1]) > threshold:
            merged.append(level)

    return merged


def is_near_level(price: float, levels: List[float], proximity: float) -> Tuple[bool, float]:
    for level in levels:
        if abs(price - level) / price <= proximity:
            return True, level
    return False, 0.0


class BingXTrader:
    BASE_URL = "https://open-api.bingx.com"

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        paper_trading: bool = True,
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
        klines_max_retries: int = 3,
        klines_retry_delay: float = 1.5,
    ):
        self.api_key = api_key
        self.secret_key = secret_key
        self.paper_trading = paper_trading
        self.session = requests.Session()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.request_timeout = (connect_timeout, read_timeout)
        self.klines_max_retries = max(1, int(klines_max_retries))
        self.klines_retry_delay = max(0.0, float(klines_retry_delay))

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        if "-" in symbol:
            return symbol
        if symbol.endswith("USDT") and len(symbol) > 4:
            return f"{symbol[:-4]}-USDT"
        return symbol

    def _generate_signature(self, params: Dict[str, Any]) -> str:
        query_string = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        return hmac.new(
            self.secret_key.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[Tuple[float, float]] = None,
    ) -> Dict[str, Any]:
        if self.paper_trading:
            if "order" in endpoint:
                return {"code": 0, "data": {"orderId": f"paper_{int(time.time() * 1000)}"}}
            if "positions" in endpoint:
                return {"code": 0, "data": []}
            return {"code": 0, "data": {}}

        params = dict(params or {})
        params["timestamp"] = int(time.time() * 1000)
        signature = self._generate_signature(params)
        query_string = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        headers = {"X-BX-APIKEY": self.api_key}
        url = f"{self.BASE_URL}{endpoint}?{query_string}&signature={signature}"
        request_timeout = timeout or self.request_timeout

        try:
            if method == "GET":
                response = self.session.get(url, headers=headers, timeout=request_timeout)
            elif method == "POST":
                response = self.session.post(url, headers=headers, timeout=request_timeout)
            elif method == "DELETE":
                response = self.session.delete(url, headers=headers, timeout=request_timeout)
            else:
                return {"code": -1, "msg": f"Unsupported method: {method}"}

            return response.json() if response.status_code == 200 else {"code": -1, "msg": response.text}
        except Exception as exc:
            return {"code": -1, "msg": str(exc)}

    def get_klines(self, symbol: str, interval: str, limit: int = 500) -> pd.DataFrame:
        params = {
            "symbol": self.normalize_symbol(symbol),
            "interval": _to_api_interval(interval),
            "limit": limit,
        }
        last_response: Dict[str, Any] = {"code": -1, "msg": "klines request was not attempted"}

        for attempt in range(1, self.klines_max_retries + 1):
            if self.paper_trading:
                try:
                    response = self.session.get(
                        f"{self.BASE_URL}/openApi/swap/v3/quote/klines",
                        params=params,
                        timeout=self.request_timeout,
                    )
                    payload = response.json() if response.status_code == 200 else {"code": -1, "msg": response.text}
                except Exception as exc:
                    payload = {"code": -1, "msg": str(exc)}
            else:
                payload = self._request("GET", "/openApi/swap/v3/quote/klines", params)

            response = payload
            if response.get("code") == 0:
                rows = response.get("data", [])
                if not rows:
                    return pd.DataFrame()

                frame = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "volume", "close_time"])
                frame["time"] = pd.to_datetime(frame["time"], unit="ms", utc=True)
                frame.set_index("time", inplace=True)
                frame[["open", "high", "low", "close", "volume"]] = frame[
                    ["open", "high", "low", "close", "volume"]
                ].astype(float)
                return frame

            last_response = response
            if attempt < self.klines_max_retries:
                self.logger.warning(
                    "Klines request failed, retrying | attempt=%s/%s | response=%s",
                    attempt,
                    self.klines_max_retries,
                    response,
                )
                if self.klines_retry_delay > 0:
                    time.sleep(self.klines_retry_delay * attempt)

        self.logger.error(
            "Failed to load klines after %s attempts: %s",
            self.klines_max_retries,
            last_response,
        )
        return pd.DataFrame()

    def get_balance(self) -> float:
        if self.paper_trading:
            return 0.0

        response = self._request("GET", "/openApi/swap/v2/user/balance")
        if response.get("code") != 0:
            self.logger.error("Failed to fetch balance: %s", response)
            return 0.0

        data = response.get("data", {})
        if isinstance(data, list) and data:
            return float(data[0].get("balance", 0) or 0)
        if isinstance(data, dict):
            if "balance" in data:
                balance_value = data.get("balance", 0)
                if isinstance(balance_value, dict):
                    for key in ("availableMargin", "balance", "equity"):
                        raw_value = balance_value.get(key)
                        if raw_value not in (None, "", 0, "0"):
                            return float(raw_value)
                return float(balance_value or 0)
            assets = data.get("balance", []) or data.get("balances", [])
            if isinstance(assets, list):
                for asset in assets:
                    if str(asset.get("asset", "")).upper() == "USDT":
                        return float(asset.get("balance", 0) or 0)
        return 0.0

    def get_all_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        if self.paper_trading:
            return []
        response = self._request("GET", "/openApi/swap/v2/user/positions")
        if response.get("code") != 0:
            self.logger.error("Failed to fetch positions: %s", response)
            return []

        positions = response.get("data", [])
        if symbol is None:
            return positions

        normalized = self.normalize_symbol(symbol)
        filtered: List[Dict[str, Any]] = []
        for position in positions:
            position_symbol = self.normalize_symbol(str(position.get("symbol", "")))
            position_amt = abs(float(position.get("positionAmt", 0) or 0))
            if position_symbol == normalized and position_amt > 0:
                filtered.append(position)
        return filtered

    def place_order(self, symbol: str, side: str, quantity: float) -> Optional[str]:
        if quantity <= 0:
            return None

        if self.paper_trading:
            return f"paper_{int(time.time() * 1000)}"

        params = {
            "symbol": self.normalize_symbol(symbol),
            "side": side,
            "positionSide": "LONG" if side == "BUY" else "SHORT",
            "type": "MARKET",
            "quantity": round(float(quantity), 6),
        }
        response = self._request("POST", "/openApi/swap/v2/trade/order", params)
        if response.get("code") == 0:
            data = response.get("data", {})
            order_id = data.get("orderId")
            if not order_id and isinstance(data.get("order"), dict):
                order_id = data["order"].get("orderId")
            return order_id

        self.logger.error("Failed to place order: %s", response)
        return None

    def get_open_position(self, symbol: str, direction: str) -> Optional[Dict[str, Any]]:
        if self.paper_trading:
            return None

        response = self._request("GET", "/openApi/swap/v2/user/positions")
        if response.get("code") != 0:
            self.logger.error("Failed to fetch positions: %s", response)
            return None

        target_symbol = self.normalize_symbol(symbol)
        target_side = "LONG" if direction == "long" else "SHORT"
        for position in response.get("data", []):
            position_symbol = self.normalize_symbol(str(position.get("symbol", "")))
            position_side = str(position.get("positionSide", "")).upper()
            position_amt = abs(float(position.get("positionAmt", 0) or 0))
            if position_symbol == target_symbol and position_side == target_side and position_amt > 0:
                return position
        return None

    @staticmethod
    def extract_entry_price(position: Dict[str, Any]) -> float:
        for key in ("avgPrice", "avgOpenPrice", "entryPrice"):
            value = position.get(key)
            if value not in (None, "", 0, "0"):
                try:
                    return float(value)
                except (TypeError, ValueError):
                    continue
        return 0.0

    def get_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        if self.paper_trading:
            return []

        response = self._request(
            "GET",
            "/openApi/swap/v2/trade/openOrders",
            {"symbol": self.normalize_symbol(symbol)},
        )
        if response.get("code") != 0:
            self.logger.error("Failed to fetch open orders: %s", response)
            return []
        data = response.get("data", {})
        if isinstance(data, dict):
            return data.get("orders", [])
        if isinstance(data, list):
            return data
        return []

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        if self.paper_trading:
            return True

        response = self._request(
            "DELETE",
            "/openApi/swap/v2/trade/order",
            {"symbol": self.normalize_symbol(symbol), "orderId": order_id},
        )
        return response.get("code") == 0

    @staticmethod
    def is_protection_order(order: Dict[str, Any], position_side: Optional[str] = None) -> bool:
        order_type = str(order.get("type", "")).upper()
        order_position_side = str(order.get("positionSide", "")).upper()
        if order_type not in {"STOP_MARKET", "TAKE_PROFIT_MARKET"}:
            return False
        if not order_position_side:
            return False
        if position_side and order_position_side != str(position_side).upper():
            return False
        return True

    def cancel_protection_orders(self, symbol: str, position_side: str) -> bool:
        if self.paper_trading:
            return True

        success = True
        for order in self.get_open_orders(symbol):
            if not self.is_protection_order(order, position_side):
                continue
            order_id = str(order.get("orderId", "")).strip()
            if order_id:
                success = self.cancel_order(symbol, order_id) and success
        return success

    def place_exit_order(
        self,
        symbol: str,
        position_side: str,
        quantity: float,
        trigger_price: float,
        order_type: str,
        price_precision: int,
    ) -> bool:
        if self.paper_trading:
            return True

        params = {
            "symbol": self.normalize_symbol(symbol),
            "side": "SELL" if position_side == "LONG" else "BUY",
            "positionSide": position_side,
            "type": order_type,
            "stopPrice": round(float(trigger_price), price_precision),
            "quantity": round(float(quantity), 6),
            "workingType": "MARK_PRICE",
        }
        response = self._request("POST", "/openApi/swap/v2/trade/order", params)
        ok = response.get("code") == 0
        if not ok:
            self.logger.error("Failed to place %s: %s | params=%s", order_type, response, params)
        return ok

    def set_protection_orders(
        self,
        symbol: str,
        direction: str,
        stop_price: float,
        tp_price: float,
        quantity: Optional[float],
        price_precision: int,
    ) -> bool:
        if self.paper_trading:
            return True

        position = None
        for _ in range(10):
            position = self.get_open_position(symbol, direction)
            if position:
                break
            time.sleep(0.5)

        if not position:
            self.logger.error("Could not find open position for protection orders.")
            return False

        position_side = str(position.get("positionSide", "")).upper()
        position_qty = abs(float(position.get("positionAmt", 0) or 0))
        final_qty = quantity if quantity and quantity > 0 else position_qty

        if final_qty <= 0:
            self.logger.error("Position size is zero, cannot place protection orders.")
            return False

        cleared = self.cancel_protection_orders(symbol, position_side)
        sl_ok = self.place_exit_order(symbol, position_side, final_qty, stop_price, "STOP_MARKET", price_precision)
        tp_ok = self.place_exit_order(symbol, position_side, final_qty, tp_price, "TAKE_PROFIT_MARKET", price_precision)
        if not (cleared and sl_ok and tp_ok):
            self.logger.error(
                "Protection placement failed | cleared=%s sl_ok=%s tp_ok=%s side=%s qty=%s stop=%s tp=%s",
                cleared,
                sl_ok,
                tp_ok,
                position_side,
                final_qty,
                stop_price,
                tp_price,
            )
        return cleared and sl_ok and tp_ok

    def cancel_all_orders(self, symbol: str) -> bool:
        if self.paper_trading:
            return True
        response = self._request(
            "DELETE",
            "/openApi/swap/v2/trade/allOpenOrders",
            {"symbol": self.normalize_symbol(symbol)},
        )
        return response.get("code") == 0

    def get_last_price(self, symbol: str) -> float:
        frame = self.get_klines(symbol, "1m", limit=1)
        if frame.empty:
            return 0.0
        return float(frame["close"].iloc[-1])

    def close_position_market(self, symbol: str, direction: str, quantity: Optional[float] = None) -> Optional[str]:
        if self.paper_trading:
            return f"paper_close_{int(time.time() * 1000)}"

        position = self.get_open_position(symbol, direction)
        if not position:
            return None

        position_qty = abs(float(position.get("positionAmt", 0) or 0))
        final_qty = quantity if quantity and quantity > 0 else position_qty
        if final_qty <= 0:
            return None

        params = {
            "symbol": self.normalize_symbol(symbol),
            "side": "SELL" if direction == "long" else "BUY",
            "positionSide": "LONG" if direction == "long" else "SHORT",
            "type": "MARKET",
            "quantity": round(float(final_qty), 6),
        }
        response = self._request("POST", "/openApi/swap/v2/trade/order", params)
        if response.get("code") == 0:
            data = response.get("data", {})
            order_id = data.get("orderId")
            if not order_id and isinstance(data.get("order"), dict):
                order_id = data["order"].get("orderId")
            return order_id

        self.logger.error("Failed to close position: %s", response)
        return None


class ContrarianBot:
    def __init__(self, config: Config):
        self.config = config
        self.capital = config.INITIAL_CAPITAL
        self.position: Optional[Dict[str, Any]] = None
        self.trades: List[Dict[str, Any]] = []
        self.consecutive_losses = 0
        self.levels: List[float] = []
        self.last_trade_time: Optional[pd.Timestamp] = None
        self.last_processed_signal_candle: Optional[pd.Timestamp] = None
        self.loop_counter = 0
        self.last_heartbeat_at: Optional[datetime] = None
        self.last_market_data_at: Optional[datetime] = None
        self.last_stall_alert_at: Optional[datetime] = None
        self.empty_market_streak = 0

        self.logger = setup_logging(config).getChild(self.__class__.__name__)
        self.notifier = TelegramNotifier(
            config.TELEGRAM_BOT_TOKEN,
            config.TELEGRAM_CHAT_ID,
            self.logger,
            connect_timeout=config.TELEGRAM_CONNECT_TIMEOUT_SECONDS,
            read_timeout=config.TELEGRAM_READ_TIMEOUT_SECONDS,
        )
        self.notifier.validate_chat()

        self.trader = BingXTrader(
            api_key=config.API_KEY,
            secret_key=config.SECRET_KEY,
            paper_trading=config.PAPER_TRADING,
            connect_timeout=config.API_CONNECT_TIMEOUT_SECONDS,
            read_timeout=config.API_READ_TIMEOUT_SECONDS,
            klines_max_retries=config.KLINES_MAX_RETRIES,
            klines_retry_delay=config.KLINES_RETRY_DELAY_SECONDS,
        )

        if self.config.ENABLE_STATE_RECOVERY:
            self._load_state()

        if not config.PAPER_TRADING:
            balance = self.trader.get_balance()
            if balance > 0:
                self.capital = balance
                self.logger.info("Loaded live balance: %.2f USDT", self.capital)
            else:
                self.logger.warning("Live balance is unavailable, keeping configured capital %.2f", self.capital)

            if self.config.STARTUP_SYNC and self.config.ENABLE_STATE_RECOVERY:
                self.reconcile_runtime_state()
                self.cleanup_stale_orders(reason="startup")

    def _emit_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "symbol": self.config.SYMBOL,
            "paper_trading": self.config.PAPER_TRADING,
            **payload,
        }
        _ensure_parent_dir(self.config.EVENT_LOG_FILE)
        with open(self.config.EVENT_LOG_FILE, "a", encoding="utf-8") as file_handle:
            file_handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    def _serialize_state(self) -> Dict[str, Any]:
        return {
            "capital": self.capital,
            "consecutive_losses": self.consecutive_losses,
            "last_trade_time": str(self.last_trade_time) if self.last_trade_time is not None else None,
            "last_processed_signal_candle": str(self.last_processed_signal_candle) if self.last_processed_signal_candle is not None else None,
            "position": self.position,
            "trades_count": len(self.trades),
        }

    def _save_state(self) -> None:
        _ensure_parent_dir(self.config.STATE_FILE)
        with open(self.config.STATE_FILE, "w", encoding="utf-8") as file_handle:
            json.dump(self._serialize_state(), file_handle, indent=2, ensure_ascii=False, default=str)

    def _load_state(self) -> None:
        state_path = Path(self.config.STATE_FILE)
        if not state_path.exists():
            return
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception as exc:
            self.logger.warning("State load failed: %s", exc)
            return

        self.capital = float(state.get("capital", self.capital) or self.capital)
        self.consecutive_losses = int(state.get("consecutive_losses", self.consecutive_losses) or 0)
        last_trade_time = state.get("last_trade_time")
        if last_trade_time:
            self.last_trade_time = pd.Timestamp(last_trade_time)
        last_candle = state.get("last_processed_signal_candle")
        if last_candle:
            self.last_processed_signal_candle = pd.Timestamp(last_candle)
        position = state.get("position")
        if isinstance(position, dict):
            self.position = position
            if "entry_time" in self.position and self.position["entry_time"]:
                self.position["entry_time"] = pd.Timestamp(self.position["entry_time"])

    def _notify(self, text: str) -> None:
        delivered = self.notifier.send(text)
        if delivered:
            return

        _ensure_parent_dir(self.config.FAILED_NOTIFICATIONS_FILE)
        with open(self.config.FAILED_NOTIFICATIONS_FILE, "a", encoding="utf-8") as file_handle:
            file_handle.write(
                json.dumps(
                    {
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "delivered": False,
                        "telegram_enabled": self.notifier.enabled,
                        "disabled_reason": self.notifier.disabled_reason,
                        "message": text,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

    def _maybe_warn_stall(self, latest_signal_candle: pd.Timestamp, now_dt: datetime) -> None:
        if self.last_market_data_at is None:
            self.last_market_data_at = now_dt
            return
        elapsed_minutes = (now_dt - self.last_market_data_at).total_seconds() / 60
        if elapsed_minutes < self.config.STALL_ALERT_MINUTES:
            return
        if self.last_stall_alert_at and (now_dt - self.last_stall_alert_at).total_seconds() < 300:
            return

        self.last_stall_alert_at = now_dt
        message = (
            f"[{self.config.BOT_TAG}] still waiting for a new closed candle\n"
            f"last_candle: {latest_signal_candle}\n"
            f"idle_minutes: {elapsed_minutes:.1f}"
        )
        self.logger.warning(message.replace("\n", " | "))
        self._emit_event(
            "candle_stall_warning",
            {"last_candle": str(latest_signal_candle), "idle_minutes": round(elapsed_minutes, 2)},
        )
        self._notify(message)

    def update_trend(self, df: pd.DataFrame) -> str:
        if len(df) < self.config.TREND_EMA_PERIOD:
            return "sideways"

        ema = calculate_ema(df, self.config.TREND_EMA_PERIOD)
        current_price = df["close"].iloc[-1]
        current_ema = ema.iloc[-1]

        if current_price > current_ema:
            return "uptrend"
        if current_price < current_ema:
            return "downtrend"
        return "sideways"

    def check_contrarian_signals(self, df: pd.DataFrame, idx: int) -> Optional[Dict[str, Any]]:
        if self.consecutive_losses >= self.config.MAX_CONSECUTIVE_LOSSES:
            return None

        if idx < 50:
            return None

        current = df.iloc[idx]
        prev = df.iloc[idx - 1]

        trend = self.update_trend(df[: idx + 1])
        self.levels = find_support_resistance_levels(
            df[: idx + 1],
            lookback=self.config.LEVEL_LOOKBACK,
            proximity=self.config.LEVEL_PROXIMITY,
        )

        if len(self.levels) < 2:
            return None

        atr_series = calculate_atr(df[: idx + 1])
        atr = atr_series.iloc[-1] if len(df[: idx + 1]) > 20 and not pd.isna(atr_series.iloc[-1]) else current["close"] * 0.01
        near_level, level_price = is_near_level(current["close"], self.levels, self.config.LEVEL_PROXIMITY)

        if not near_level:
            return None

        pinbar = detect_pinbar(current)
        if pinbar:
            if pinbar == "bullish" and trend in ["downtrend", "sideways"]:
                stop = max(current["high"], prev["high"]) + atr * 0.5
                risk = stop - current["close"]
                return {
                    "type": "contrarian_short_on_bullish_pinbar",
                    "entry": current["close"],
                    "stop": stop,
                    "tp": current["close"] - risk * self.config.RISK_REWARD_RATIO,
                    "direction": "short",
                    "time": df.index[idx],
                    "trend": trend,
                    "level_price": level_price,
                    "atr": float(atr),
                }

            if pinbar == "bearish" and trend in ["uptrend", "sideways"]:
                stop = min(current["low"], prev["low"]) - atr * 0.5
                risk = current["close"] - stop
                return {
                    "type": "contrarian_long_on_bearish_pinbar",
                    "entry": current["close"],
                    "stop": stop,
                    "tp": current["close"] + risk * self.config.RISK_REWARD_RATIO,
                    "direction": "long",
                    "time": df.index[idx],
                    "trend": trend,
                    "level_price": level_price,
                    "atr": float(atr),
                }

        if current["close"] > level_price and prev["close"] < level_price:
            stop = max(current["high"], level_price + atr)
            risk = stop - current["close"]
            return {
                "type": "contrarian_short_on_fake_breakout",
                "entry": current["close"],
                "stop": stop,
                "tp": current["close"] - risk * self.config.RISK_REWARD_RATIO,
                "direction": "short",
                "time": df.index[idx],
                "trend": trend,
                "level_price": level_price,
                "atr": float(atr),
            }

        if current["close"] < level_price and prev["close"] > level_price:
            stop = min(current["low"], level_price - atr)
            risk = current["close"] - stop
            return {
                "type": "contrarian_long_on_fake_breakout",
                "entry": current["close"],
                "stop": stop,
                "tp": current["close"] + risk * self.config.RISK_REWARD_RATIO,
                "direction": "long",
                "time": df.index[idx],
                "trend": trend,
                "level_price": level_price,
                "atr": float(atr),
            }

        if current["close"] > level_price and prev["close"] < level_price and current["close"] > current["open"]:
            stop = level_price + atr * 1.5
            risk = stop - current["close"]
            return {
                "type": "contrarian_short_on_bounce",
                "entry": current["close"],
                "stop": stop,
                "tp": current["close"] - risk * self.config.RISK_REWARD_RATIO,
                "direction": "short",
                "time": df.index[idx],
                "trend": trend,
                "level_price": level_price,
                "atr": float(atr),
            }

        if current["close"] < level_price and prev["close"] > level_price and current["close"] < current["open"]:
            stop = level_price - atr * 1.5
            risk = current["close"] - stop
            return {
                "type": "contrarian_long_on_bounce",
                "entry": current["close"],
                "stop": stop,
                "tp": current["close"] + risk * self.config.RISK_REWARD_RATIO,
                "direction": "long",
                "time": df.index[idx],
                "trend": trend,
                "level_price": level_price,
                "atr": float(atr),
            }

        return None

    def _build_exit_context(self, df: pd.DataFrame, signal: Dict[str, Any]) -> Dict[str, float]:
        atr_series = calculate_atr(df)
        valid_atr = atr_series.dropna()
        current_atr = float(signal.get("atr") or (valid_atr.iloc[-1] if not valid_atr.empty else df["close"].iloc[-1] * 0.01))
        baseline_atr = float(valid_atr.tail(80).median()) if not valid_atr.empty else current_atr

        ema = calculate_ema(df, self.config.TREND_EMA_PERIOD)
        current_close = float(df["close"].iloc[-1])
        current_ema = float(ema.iloc[-1]) if not ema.empty else current_close
        trend_strength_r = abs(current_close - current_ema) / current_atr if current_atr > 0 else 0.0
        vol_ratio = current_atr / baseline_atr if baseline_atr > 0 else 1.0

        return {
            "current_atr": current_atr,
            "baseline_atr": baseline_atr,
            "trend_strength_r": trend_strength_r,
            "vol_ratio": vol_ratio,
        }

    def _apply_market_exit_tuning(self, signal: Dict[str, Any], df: pd.DataFrame) -> Dict[str, Any]:
        tuned = dict(signal)
        context = self._build_exit_context(df, signal)

        entry = float(tuned["entry"])
        stop = float(tuned["stop"])
        direction = tuned["direction"]
        signal_type = str(tuned["type"])
        trend = str(tuned.get("trend", "sideways"))

        risk = abs(entry - stop)
        rr = self.config.RISK_REWARD_RATIO
        breakeven_trigger_r = self.config.BREAKEVEN_TRIGGER_R
        trail_trigger_r = self.config.TRAIL_TRIGGER_R
        trail_distance_r = self.config.TRAIL_DISTANCE_R

        strong_trend = context["trend_strength_r"] >= self.config.STRONG_TREND_THRESHOLD_R
        high_vol = context["vol_ratio"] >= self.config.HIGH_VOL_THRESHOLD
        low_vol = context["vol_ratio"] <= self.config.LOW_VOL_THRESHOLD
        mid_vol = not high_vol and not low_vol
        sideways = trend == "sideways"

        if "fake_breakout" in signal_type:
            if high_vol:
                risk *= 1.10
            if strong_trend and not sideways:
                rr = 2.2
                breakeven_trigger_r = 0.9
                trail_trigger_r = 1.3
                trail_distance_r = 0.9
            elif low_vol or sideways:
                rr = 2.7
                breakeven_trigger_r = 1.1
                trail_trigger_r = 1.7
                trail_distance_r = 1.0
            elif mid_vol:
                rr = 2.45
                breakeven_trigger_r = 0.85
                trail_trigger_r = 1.2
                trail_distance_r = 0.85
        elif "pinbar" in signal_type:
            if high_vol:
                risk *= 1.05
            if strong_trend and not sideways:
                risk *= 0.94
                rr = 2.15
                breakeven_trigger_r = 0.8
                trail_trigger_r = 1.15
                trail_distance_r = 0.8
            elif low_vol or sideways:
                rr = 2.8
                breakeven_trigger_r = 1.15
                trail_trigger_r = 1.8
                trail_distance_r = 1.05

        if direction == "long":
            stop = entry - risk
            tp = entry + risk * rr
        else:
            stop = entry + risk
            tp = entry - risk * rr

        tuned["stop"] = float(stop)
        tuned["tp"] = float(tp)
        tuned["rr"] = float(rr)
        tuned["breakeven_trigger_r"] = float(breakeven_trigger_r)
        tuned["trail_trigger_r"] = float(trail_trigger_r)
        tuned["trail_distance_r"] = float(trail_distance_r)
        tuned["vol_ratio"] = float(context["vol_ratio"])
        tuned["trend_strength_r"] = float(context["trend_strength_r"])
        return tuned

    def _round_price(self, price: float) -> float:
        return round(float(price), self.config.PRICE_PRECISION)

    def _calculate_size(self, entry: float, stop: float) -> float:
        risk_per_unit = abs(entry - stop)
        if risk_per_unit <= 0:
            return 0.0
        risk_amount = self.capital * self.config.RISK_PER_TRADE
        size = risk_amount / risk_per_unit
        if size < self.config.MIN_QTY:
            return 0.0
        return round(size, self.config.QTY_PRECISION)

    def execute_signal(self, signal: Dict[str, Any], live_order: bool = False) -> bool:
        size = self._calculate_size(signal["entry"], signal["stop"])
        if size <= 0:
            self.logger.warning(
                "Calculated position size is below exchange minimum %.6f, skipping signal.",
                self.config.MIN_QTY,
            )
            return False

        entry = float(signal["entry"])
        stop = float(signal["stop"])
        tp = float(signal["tp"])
        initial_risk = abs(entry - stop)
        side = "BUY" if signal["direction"] == "long" else "SELL"

        order_id = None
        if live_order:
            order_id = self.trader.place_order(self.config.SYMBOL, side, size)
            if not order_id:
                self.logger.error("Exchange rejected entry order.")
                return False

            time.sleep(2)
            exchange_position = self.trader.get_open_position(self.config.SYMBOL, signal["direction"])
            if exchange_position:
                real_entry = self.trader.extract_entry_price(exchange_position)
                exchange_size = abs(float(exchange_position.get("positionAmt", 0) or 0))
                if real_entry > 0:
                    entry = real_entry
                    if signal["direction"] == "long":
                        stop = real_entry - initial_risk
                        tp = real_entry + initial_risk * self.config.RISK_REWARD_RATIO
                    else:
                        stop = real_entry + initial_risk
                        tp = real_entry - initial_risk * self.config.RISK_REWARD_RATIO
                if exchange_size > 0:
                    size = round(exchange_size, self.config.QTY_PRECISION)

        self.position = {
            "type": signal["direction"],
            "entry": float(entry),
            "size": float(size),
            "stop": float(stop),
            "tp": float(tp),
            "entry_time": signal["time"],
            "signal_type": signal["type"],
            "order_id": order_id or f"backtest_{int(time.time() * 1000)}",
            "initial_risk": initial_risk,
            "rr": float(signal.get("rr", self.config.RISK_REWARD_RATIO)),
            "breakeven_trigger_r": float(signal.get("breakeven_trigger_r", self.config.BREAKEVEN_TRIGGER_R)),
            "trail_trigger_r": float(signal.get("trail_trigger_r", self.config.TRAIL_TRIGGER_R)),
            "trail_distance_r": float(signal.get("trail_distance_r", self.config.TRAIL_DISTANCE_R)),
            "trend": signal.get("trend"),
            "vol_ratio": signal.get("vol_ratio"),
            "trend_strength_r": signal.get("trend_strength_r"),
        }
        self.last_trade_time = pd.Timestamp(signal["time"])

        if live_order:
            protection_ok = self.trader.set_protection_orders(
                symbol=self.config.SYMBOL,
                direction=signal["direction"],
                stop_price=self._round_price(self.position["stop"]),
                tp_price=self._round_price(self.position["tp"]),
                quantity=self.position["size"],
                price_precision=self.config.PRICE_PRECISION,
            )
            if not protection_ok:
                self.logger.warning("Protection orders were not confirmed by exchange.")
                self._emit_event("protection_order_warning", {"order_id": order_id, "signal_type": signal["type"]})

        if self.config.VERBOSE:
            self.logger.info(
                "Opened %s | signal=%s | entry=%.2f | stop=%.2f | tp=%.2f | size=%.6f | rr=%.2f",
                self.position["type"],
                self.position["signal_type"],
                self.position["entry"],
                self.position["stop"],
                self.position["tp"],
                self.position["size"],
                self.position["rr"],
            )

        self._emit_event(
            "position_opened",
            {
                "side": self.position["type"],
                "entry": self.position["entry"],
                "stop": self.position["stop"],
                "tp": self.position["tp"],
                "size": self.position["size"],
                "signal_type": self.position["signal_type"],
                "rr": self.position["rr"],
                "order_id": self.position["order_id"],
            },
        )
        self._notify(
            f"[{self.config.BOT_TAG}] OPEN {self.position['type'].upper()}\n"
            f"signal: {self.position['signal_type']}\n"
            f"entry: {self.position['entry']:.2f}\n"
            f"stop: {self.position['stop']:.2f}\n"
            f"tp: {self.position['tp']:.2f}\n"
            f"size: {self.position['size']:.6f}"
        )
        self._save_state()

        return True

    def _maybe_cooldown_active(self, current_time: pd.Timestamp) -> bool:
        if self.consecutive_losses < self.config.MAX_CONSECUTIVE_LOSSES:
            return False
        if self.last_trade_time is None:
            return False
        cooldown = timedelta(hours=self.config.COOLDOWN_AFTER_MAX_LOSSES_HOURS)
        if current_time.to_pydatetime() - self.last_trade_time.to_pydatetime() < cooldown:
            return True
        self.consecutive_losses = 0
        return False

    def cleanup_stale_orders(self, reason: str = "loop") -> int:
        if self.config.PAPER_TRADING or not self.config.CLEAN_STALE_ORDERS:
            return 0

        positions = self.trader.get_all_positions(self.config.SYMBOL)
        active_sides = {
            str(position.get("positionSide", "")).upper()
            for position in positions
            if abs(float(position.get("positionAmt", 0) or 0)) > 0
        }
        canceled = 0

        for order in self.trader.get_open_orders(self.config.SYMBOL):
            order_id = str(order.get("orderId", "")).strip()
            if not order_id:
                continue
            order_side = str(order.get("positionSide", "")).upper()
            is_protection = self.trader.is_protection_order(order)
            should_cancel = False

            if is_protection and order_side not in active_sides:
                should_cancel = True
            if not is_protection:
                should_cancel = True

            if should_cancel and self.trader.cancel_order(self.config.SYMBOL, order_id):
                canceled += 1

        if canceled:
            self.logger.info("Canceled %s stale open orders (%s).", canceled, reason)
            self._emit_event("stale_orders_canceled", {"count": canceled, "reason": reason})
        return canceled

    def reconcile_runtime_state(self) -> None:
        if self.config.PAPER_TRADING:
            return

        positions = self.trader.get_all_positions(self.config.SYMBOL)
        if not positions:
            if self.position:
                self.logger.warning("No live position on exchange, local state was cleared.")
                self.position = None
                self._save_state()
            return

        position = positions[0]
        direction = "long" if str(position.get("positionSide", "")).upper() == "LONG" else "short"
        entry = self.trader.extract_entry_price(position)
        size = abs(float(position.get("positionAmt", 0) or 0))
        open_orders = self.trader.get_open_orders(self.config.SYMBOL)

        stop_price = None
        tp_price = None
        for order in open_orders:
            if str(order.get("positionSide", "")).upper() != str(position.get("positionSide", "")).upper():
                continue
            order_type = str(order.get("type", "")).upper()
            trigger_raw = order.get("stopPrice", order.get("price"))
            if trigger_raw in (None, "", 0, "0"):
                continue
            trigger_price = float(trigger_raw)
            if order_type == "STOP_MARKET":
                stop_price = trigger_price
            elif order_type == "TAKE_PROFIT_MARKET":
                tp_price = trigger_price

        if entry > 0 and size > 0:
            inferred_stop = stop_price if stop_price is not None else entry
            inferred_tp = tp_price if tp_price is not None else entry
            initial_risk = abs(entry - inferred_stop)
            self.position = {
                "type": direction,
                "entry": entry,
                "size": size,
                "stop": inferred_stop,
                "tp": inferred_tp,
                "entry_time": pd.Timestamp(datetime.now(timezone.utc)),
                "signal_type": "recovered_from_exchange",
                "order_id": "exchange_recovered",
                "initial_risk": initial_risk,
                "rr": abs(inferred_tp - entry) / initial_risk if initial_risk > 0 else self.config.RISK_REWARD_RATIO,
                "breakeven_trigger_r": self.config.BREAKEVEN_TRIGGER_R,
                "trail_trigger_r": self.config.TRAIL_TRIGGER_R,
                "trail_distance_r": self.config.TRAIL_DISTANCE_R,
                "trend": None,
                "vol_ratio": None,
                "trend_strength_r": None,
            }
            self.logger.info(
                "Recovered live position from exchange | side=%s | entry=%.2f | size=%.6f",
                direction,
                entry,
                size,
            )
            self._emit_event("position_recovered", {"side": direction, "entry": entry, "size": size})
            self._save_state()

    def _maybe_send_heartbeat(self, current_time: datetime, current_price: float) -> None:
        if self.config.HEARTBEAT_MINUTES <= 0:
            return
        if self.last_heartbeat_at and current_time - self.last_heartbeat_at < timedelta(minutes=self.config.HEARTBEAT_MINUTES):
            return
        self.last_heartbeat_at = current_time
        mode = "paper" if self.config.PAPER_TRADING else "live"
        pos = self.position["type"] if self.position else "flat"
        self._notify(
            f"[{self.config.BOT_TAG}] heartbeat\n"
            f"mode: {mode}\n"
            f"symbol: {self.config.SYMBOL}\n"
            f"price: {current_price:.2f}\n"
            f"position: {pos}\n"
            f"capital: {self.capital:.2f}"
        )

    def _close_position(self, exit_time: pd.Timestamp, exit_price: float, reason: str) -> Dict[str, Any]:
        if not self.position:
            raise RuntimeError("No open position to close.")

        if self.position["type"] == "long":
            pnl = (exit_price - self.position["entry"]) * self.position["size"]
        else:
            pnl = (self.position["entry"] - exit_price) * self.position["size"]

        trade = {
            "entry_time": str(self.position["entry_time"]),
            "entry_price": self.position["entry"],
            "exit_time": str(exit_time),
            "exit_price": float(exit_price),
            "type": self.position["type"],
            "signal_type": self.position["signal_type"],
            "size": self.position["size"],
            "pnl": float(pnl),
            "pnl_percent": float((pnl / (self.position["entry"] * self.position["size"])) * 100),
            "exit_reason": reason,
            "rr": self.position.get("rr"),
            "trend": self.position.get("trend"),
            "vol_ratio": self.position.get("vol_ratio"),
            "trend_strength_r": self.position.get("trend_strength_r"),
        }

        self.trades.append(trade)
        self.capital += pnl
        self.consecutive_losses = self.consecutive_losses + 1 if pnl <= 0 else 0

        if self.config.VERBOSE:
            self.logger.info(
                "Closed %s | exit=%s | price=%.2f | pnl=%+.2f | capital=%.2f",
                trade["signal_type"],
                reason,
                exit_price,
                pnl,
                self.capital,
            )

        self._emit_event(
            "position_closed",
            {
                "side": trade["type"],
                "exit_reason": reason,
                "entry_price": trade["entry_price"],
                "exit_price": trade["exit_price"],
                "pnl": trade["pnl"],
                "pnl_percent": trade["pnl_percent"],
                "signal_type": trade["signal_type"],
            },
        )
        sign = "+" if pnl >= 0 else "-"
        self._notify(
            f"[{self.config.BOT_TAG}] CLOSE {trade['type'].upper()}\n"
            f"signal: {trade['signal_type']}\n"
            f"reason: {reason}\n"
            f"exit: {trade['exit_price']:.2f}\n"
            f"pnl: {sign}{abs(trade['pnl']):.2f} USDT\n"
            f"capital: {self.capital:.2f}"
        )
        self.position = None
        if not self.config.PAPER_TRADING:
            self.cleanup_stale_orders(reason=f"post_close:{reason}")
        self.save_trade_log()
        self._save_state()
        return trade

    def check_exit_on_candle(self, candle: pd.Series, candle_time: pd.Timestamp) -> Optional[Dict[str, Any]]:
        if not self.position:
            return None

        if self.position["type"] == "long":
            if candle["high"] >= self.position["tp"]:
                return self._close_position(candle_time, self.position["tp"], "take_profit")
            if candle["low"] <= self.position["stop"]:
                exit_price = min(candle["open"], self.position["stop"])
                return self._close_position(candle_time, exit_price, "stop_loss")
        else:
            if candle["low"] <= self.position["tp"]:
                return self._close_position(candle_time, self.position["tp"], "take_profit")
            if candle["high"] >= self.position["stop"]:
                exit_price = max(candle["open"], self.position["stop"])
                return self._close_position(candle_time, exit_price, "stop_loss")

        return None

    def check_exit_on_price(self, current_price: float, current_time: pd.Timestamp) -> Optional[Dict[str, Any]]:
        if not self.position:
            return None

        if self.position["type"] == "long":
            if current_price >= self.position["tp"]:
                return self._close_position(current_time, self.position["tp"], "take_profit")
            if current_price <= self.position["stop"]:
                return self._close_position(current_time, self.position["stop"], "stop_loss")
        else:
            if current_price <= self.position["tp"]:
                return self._close_position(current_time, self.position["tp"], "take_profit")
            if current_price >= self.position["stop"]:
                return self._close_position(current_time, self.position["stop"], "stop_loss")

        return None

    def update_trailing_stop(
        self,
        current_price: float,
        current_time: pd.Timestamp,
        sync_exchange: bool,
    ) -> bool:
        if not self.position:
            return False

        initial_risk = self.position["initial_risk"]
        if initial_risk <= 0:
            return False

        stop_before = self.position["stop"]

        if self.position["type"] == "long":
            if current_price >= self.position["entry"] + initial_risk * self.position["breakeven_trigger_r"]:
                self.position["stop"] = max(self.position["stop"], self.position["entry"])
            if current_price >= self.position["entry"] + initial_risk * self.position["trail_trigger_r"]:
                self.position["stop"] = max(
                    self.position["stop"],
                    current_price - initial_risk * self.position["trail_distance_r"],
                )
        else:
            if current_price <= self.position["entry"] - initial_risk * self.position["breakeven_trigger_r"]:
                self.position["stop"] = min(self.position["stop"], self.position["entry"])
            if current_price <= self.position["entry"] - initial_risk * self.position["trail_trigger_r"]:
                self.position["stop"] = min(
                    self.position["stop"],
                    current_price + initial_risk * self.position["trail_distance_r"],
                )

        updated = abs(self.position["stop"] - stop_before) > 10 ** (-self.config.PRICE_PRECISION)
        if updated and self.config.VERBOSE:
            self.logger.info(
                "Trailing updated | time=%s | new_stop=%.2f",
                current_time,
                self.position["stop"],
            )
            self._emit_event(
                "trailing_updated",
                {
                    "side": self.position["type"],
                    "new_stop": self.position["stop"],
                    "time": str(current_time),
                },
            )

        if updated and sync_exchange and not self.config.PAPER_TRADING:
            protection_ok = self.trader.set_protection_orders(
                symbol=self.config.SYMBOL,
                direction=self.position["type"],
                stop_price=self._round_price(self.position["stop"]),
                tp_price=self._round_price(self.position["tp"]),
                quantity=self.position["size"],
                price_precision=self.config.PRICE_PRECISION,
            )
            if not protection_ok:
                self.logger.warning("Failed to update trailing stop on exchange.")
        if updated:
            self._save_state()

        return updated

    def sync_position_with_exchange(self, current_time: pd.Timestamp, current_price: float) -> bool:
        if not self.position or self.config.PAPER_TRADING:
            return bool(self.position)

        exchange_position = self.trader.get_open_position(self.config.SYMBOL, self.position["type"])
        if not exchange_position:
            self._close_position(current_time, current_price, "exchange_closed")
            return False

        exchange_entry = self.trader.extract_entry_price(exchange_position)
        exchange_size = abs(float(exchange_position.get("positionAmt", 0) or 0))

        if exchange_entry > 0:
            self.position["entry"] = exchange_entry
        if exchange_size > 0:
            self.position["size"] = exchange_size

        return True

    def run_backtest(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if df.empty:
            return []

        start_idx = max(200, min(len(df) - 1, 200))
        iterator = range(start_idx, len(df))
        if HAS_TQDM and self.config.SHOW_PROGRESS:
            iterator = tqdm(iterator, desc="Contrarian backtest")

        for i in iterator:
            current_time = pd.Timestamp(df.index[i])

            if self._maybe_cooldown_active(current_time):
                continue

            candle = df.iloc[i]

            if self.position:
                closed_trade = self.check_exit_on_candle(candle, current_time)
                if closed_trade:
                    continue

                self.update_trailing_stop(
                    current_price=float(candle["close"]),
                    current_time=current_time,
                    sync_exchange=False,
                )

            if not self.position:
                signal = self.check_contrarian_signals(df.iloc[: i + 1], i)
                if signal:
                    tuned_signal = self._apply_market_exit_tuning(signal, df.iloc[: i + 1])
                    self.execute_signal(tuned_signal, live_order=False)

        return self.trades

    def run_live(self, limit: int = 500) -> None:
        self.logger.info(
            "Starting live loop | mode=%s | symbol=%s | timeframe=%s",
            "paper" if self.config.PAPER_TRADING else "live",
            self.config.SYMBOL,
            self.config.TIMEFRAME,
        )

        while True:
            try:
                self.loop_counter += 1
                market_df = self.trader.get_klines(self.config.SYMBOL, self.config.TIMEFRAME, limit=limit)
                if market_df.empty:
                    self.empty_market_streak += 1
                    if self.empty_market_streak in {1, 3} or self.empty_market_streak % 12 == 0:
                        self._emit_event("market_data_unavailable", {"streak": self.empty_market_streak})
                        self._notify(
                            f"[{self.config.BOT_TAG}] market data unavailable\n"
                            f"streak: {self.empty_market_streak}\n"
                            "reason: BingX klines request returned no data"
                        )
                    self.logger.warning("No market data from exchange, retrying.")
                    time.sleep(self.config.POLL_SECONDS)
                    continue

                self.empty_market_streak = 0
                now_dt = datetime.now(timezone.utc)
                current_time = pd.Timestamp(now_dt)
                current_price = float(market_df["close"].iloc[-1])
                self._maybe_send_heartbeat(now_dt, current_price)

                if self.loop_counter % self.config.CLEANUP_EVERY_LOOPS == 0:
                    self.cleanup_stale_orders(reason="scheduled")

                if self.position:
                    active = self.sync_position_with_exchange(current_time, current_price)
                    if active:
                        maybe_closed = self.check_exit_on_price(current_price, current_time)
                        if maybe_closed is None and self.position:
                            self.update_trailing_stop(current_price, current_time, sync_exchange=True)
                            self.save_trade_log()

                signal_df = market_df.iloc[:-1].copy()
                if len(signal_df) < 60:
                    time.sleep(self.config.POLL_SECONDS)
                    continue

                latest_signal_candle = pd.Timestamp(signal_df.index[-1])
                if self.last_processed_signal_candle == latest_signal_candle:
                    self._maybe_warn_stall(latest_signal_candle, now_dt)
                    time.sleep(self.config.POLL_SECONDS)
                    continue

                self.last_processed_signal_candle = latest_signal_candle
                self.last_market_data_at = now_dt
                self.last_stall_alert_at = None
                if self._maybe_cooldown_active(latest_signal_candle):
                    time.sleep(self.config.POLL_SECONDS)
                    continue

                if not self.position:
                    signal = self.check_contrarian_signals(signal_df, len(signal_df) - 1)
                    if signal:
                        tuned_signal = self._apply_market_exit_tuning(signal, signal_df)
                        self.execute_signal(tuned_signal, live_order=True)
                        self.save_trade_log()

                self._save_state()
                time.sleep(self.config.POLL_SECONDS)
            except KeyboardInterrupt:
                self.logger.info("Live loop stopped by user.")
                self._notify(f"[{self.config.BOT_TAG}] live loop stopped by user.")
                break
            except Exception as exc:
                self.logger.exception("Live loop error: %s", exc)
                self._emit_event("loop_error", {"error": str(exc)})
                self._notify(f"[{self.config.BOT_TAG}] loop error: {exc}")
                time.sleep(self.config.POLL_SECONDS)

    def save_trade_log(self, file_path: Optional[str] = None) -> None:
        target = file_path or self.config.LOG_FILE
        _ensure_parent_dir(target)
        with open(target, "w", encoding="utf-8") as file_handle:
            json.dump(self.trades, file_handle, indent=2, default=str)

    def build_summary(self) -> Dict[str, Any]:
        if not self.trades:
            return {
                "trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": 0.0,
                "total_pnl": 0.0,
                "final_capital": self.capital,
                "return_percent": ((self.capital / self.config.INITIAL_CAPITAL) - 1) * 100,
                "signal_breakdown": {},
            }

        trades_df = pd.DataFrame(self.trades)
        winning = trades_df[trades_df["pnl"] > 0]
        losing = trades_df[trades_df["pnl"] <= 0]
        signal_breakdown = (
            trades_df.groupby("signal_type")["pnl"].agg(["count", "sum", "mean"]).round(2).to_dict(orient="index")
        )

        summary = {
            "trades": int(len(trades_df)),
            "winning_trades": int(len(winning)),
            "losing_trades": int(len(losing)),
            "win_rate": float((len(winning) / len(trades_df)) * 100),
            "total_pnl": float(trades_df["pnl"].sum()),
            "final_capital": float(self.capital),
            "return_percent": float(((self.capital / self.config.INITIAL_CAPITAL) - 1) * 100),
            "avg_win": float(winning["pnl"].mean()) if not winning.empty else 0.0,
            "avg_loss": float(losing["pnl"].mean()) if not losing.empty else 0.0,
            "max_win": float(winning["pnl"].max()) if not winning.empty else 0.0,
            "max_loss": float(losing["pnl"].min()) if not losing.empty else 0.0,
            "signal_breakdown": signal_breakdown,
        }

        if not winning.empty and not losing.empty and losing["pnl"].sum() != 0:
            summary["profit_factor"] = float(abs(winning["pnl"].sum() / losing["pnl"].sum()))
        else:
            summary["profit_factor"] = 0.0

        return summary

    def print_summary(self) -> None:
        summary = self.build_summary()
        print("=" * 60)
        print("CONTRARIAN BOT SUMMARY")
        print("=" * 60)
        for key in [
            "trades",
            "winning_trades",
            "losing_trades",
            "win_rate",
            "total_pnl",
            "final_capital",
            "return_percent",
            "avg_win",
            "avg_loss",
            "max_win",
            "max_loss",
            "profit_factor",
        ]:
            if key in summary:
                print(f"{key}: {summary[key]}")
        print("signal_breakdown:")
        print(json.dumps(summary["signal_breakdown"], indent=2, ensure_ascii=False))

    def run_live_smoke_test(self, side: str, quantity: float) -> Dict[str, Any]:
        if self.config.PAPER_TRADING:
            raise RuntimeError("Smoke test must run with --paper false to verify exchange plumbing.")

        side = side.upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError("Smoke test side must be BUY or SELL.")

        direction = "long" if side == "BUY" else "short"
        cleanup_before = self.cleanup_stale_orders(reason="smoke_test_before")
        price = self.trader.get_last_price(self.config.SYMBOL)
        if price <= 0:
            raise RuntimeError("Failed to fetch last price for smoke test.")

        entry_order_id = self.trader.place_order(self.config.SYMBOL, side, quantity)
        if not entry_order_id:
            raise RuntimeError("Failed to place smoke test entry order.")

        time.sleep(2)
        position = self.trader.get_open_position(self.config.SYMBOL, direction)
        if not position:
            raise RuntimeError("Smoke test position did not appear on exchange.")

        entry = self.trader.extract_entry_price(position) or price
        offset = max(entry * 0.002, 25.0)
        stop_price = entry - offset if direction == "long" else entry + offset
        tp_price = entry + offset if direction == "long" else entry - offset
        size = abs(float(position.get("positionAmt", 0) or 0))

        protection_ok = self.trader.set_protection_orders(
            symbol=self.config.SYMBOL,
            direction=direction,
            stop_price=self._round_price(stop_price),
            tp_price=self._round_price(tp_price),
            quantity=size,
            price_precision=self.config.PRICE_PRECISION,
        )
        open_orders_after_protection = self.trader.get_open_orders(self.config.SYMBOL)

        close_order_id = self.trader.close_position_market(self.config.SYMBOL, direction, quantity=size)
        if not close_order_id:
            raise RuntimeError("Failed to close smoke test position.")

        time.sleep(2)
        cleanup_after = self.cleanup_stale_orders(reason="smoke_test_after")
        residual_orders = self.trader.get_open_orders(self.config.SYMBOL)
        live_position_after = self.trader.get_open_position(self.config.SYMBOL, direction)

        result = {
            "entry_side": side,
            "entry_order_id": entry_order_id,
            "close_order_id": close_order_id,
            "entry_price": entry,
            "position_size": size,
            "protection_ok": protection_ok,
            "open_orders_after_protection": len(open_orders_after_protection),
            "cleanup_before": cleanup_before,
            "cleanup_after": cleanup_after,
            "residual_open_orders": len(residual_orders),
            "position_closed": live_position_after is None,
        }
        self._emit_event("smoke_test_completed", result)
        self._notify(
            f"[{self.config.BOT_TAG}] smoke test complete\n"
            f"side: {side}\n"
            f"entry_order: {entry_order_id}\n"
            f"close_order: {close_order_id}\n"
            f"protection_ok: {protection_ok}\n"
            f"residual_orders: {len(residual_orders)}"
        )
        return result


def run_last_month_backtest(config: Config, data_file: str, days: int) -> Dict[str, Any]:
    df = load_and_prepare_data(data_file, target_tf=config.TIMEFRAME, lookback_days=days)
    bot = ContrarianBot(config)
    bot.run_backtest(df)
    bot.save_trade_log()
    summary = bot.build_summary()
    summary["data_file"] = str(data_file)
    summary["bars"] = int(len(df))
    summary["period_start"] = str(df.index.min()) if not df.empty else None
    summary["period_end"] = str(df.index.max()) if not df.empty else None
    summary["days"] = days

    _ensure_parent_dir(config.SUMMARY_FILE)
    with open(config.SUMMARY_FILE, "w", encoding="utf-8") as file_handle:
        json.dump(summary, file_handle, indent=2, ensure_ascii=False, default=str)

    bot.print_summary()
    print(f"Saved trades to: {config.LOG_FILE}")
    print(f"Saved summary to: {config.SUMMARY_FILE}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Kaktak contrarian bot with BingX live trading wrapper.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--mode", choices=["backtest", "live", "smoke-test"], default="backtest")
    parser.add_argument("--data-file")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--paper")
    parser.add_argument("--symbol")
    parser.add_argument("--timeframe")
    parser.add_argument("--risk", type=float)
    parser.add_argument("--verbose")
    parser.add_argument("--show-progress")
    parser.add_argument("--smoke-side")
    parser.add_argument("--smoke-qty", type=float)
    args = parser.parse_args()

    config = Config.from_file(args.config)
    if args.data_file:
        config.DATA_FILE = args.data_file
    if args.paper is not None:
        config.PAPER_TRADING = _parse_bool(args.paper)
    if args.symbol:
        config.SYMBOL = args.symbol
        config.BINGX_SYMBOL = BingXTrader.normalize_symbol(args.symbol)
    if args.timeframe:
        config.TIMEFRAME = args.timeframe
    if args.risk is not None:
        config.RISK_PER_TRADE = args.risk
    if args.verbose is not None:
        config.VERBOSE = _parse_bool(args.verbose)
    if args.show_progress is not None:
        config.SHOW_PROGRESS = _parse_bool(args.show_progress)
    if args.smoke_side:
        config.SMOKE_TEST_SIDE = args.smoke_side
    if args.smoke_qty is not None:
        config.SMOKE_TEST_QTY = args.smoke_qty
    config.ENABLE_STATE_RECOVERY = args.mode != "backtest"

    print("=" * 60)
    print("CONTRARIAN TRADING BOT")
    print("=" * 60)
    print(f"config: {config.CONFIG_FILE}")
    print(f"mode: {args.mode}")
    print(f"paper_trading: {config.PAPER_TRADING}")
    print(f"symbol: {config.SYMBOL}")
    print(f"timeframe: {config.TIMEFRAME}")
    print(f"risk_per_trade: {config.RISK_PER_TRADE}")
    print("=" * 60)

    if args.mode == "backtest":
        run_last_month_backtest(config, config.DATA_FILE, args.days)
    elif args.mode == "live":
        bot = ContrarianBot(config)
        bot.run_live()
    else:
        if config.PAPER_TRADING:
            raise RuntimeError("Smoke test requires --paper false because it places real exchange orders.")
        bot = ContrarianBot(config)
        result = bot.run_live_smoke_test(side=args.smoke_side, quantity=args.smoke_qty)
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
