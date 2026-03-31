from __future__ import annotations

import hashlib
import hmac
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import requests


TIMEFRAME_RULES = {
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1D",
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_end_time(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if len(normalized) == 10:
        normalized = f"{normalized}T23:59:59+00:00"
    elif normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    return (
        df.resample(TIMEFRAME_RULES[timeframe], label="right", closed="right")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna()
    )


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period).mean()


def efficiency_ratio(series: pd.Series, period: int) -> pd.Series:
    change = (series - series.shift(period)).abs()
    volatility = series.diff().abs().rolling(period).sum()
    return change.divide(volatility.replace(0.0, np.nan))


def realized_vol(series: pd.Series, period: int) -> pd.Series:
    return np.log(series.astype(float)).diff().rolling(period).std() * np.sqrt(period)


def linreg_t_stat(series: pd.Series, window: int) -> pd.Series:
    values = np.log(series.astype(float).replace(0.0, np.nan).to_numpy())
    result = np.full(len(series), np.nan, dtype=float)
    x = np.arange(window, dtype=float)
    x_centered = x - x.mean()
    denom = np.sum(x_centered**2)
    if denom == 0:
        return pd.Series(result, index=series.index)

    for idx in range(window - 1, len(series)):
        y = values[idx - window + 1 : idx + 1]
        if np.isnan(y).any():
            continue
        y_centered = y - y.mean()
        slope = np.sum(x_centered * y_centered) / denom
        intercept = y.mean() - slope * x.mean()
        fitted = intercept + slope * x
        resid = y - fitted
        sigma = np.sqrt(np.sum(resid**2) / max(window - 2, 1))
        se = sigma / np.sqrt(denom) if sigma > 0 else 0.0
        result[idx] = slope / se if se > 0 else 0.0
    return pd.Series(result, index=series.index)


def find_pivots(df: pd.DataFrame, width: int = 2) -> pd.DataFrame:
    highs = df["high"]
    lows = df["low"]
    pivot_high = pd.Series(True, index=df.index)
    pivot_low = pd.Series(True, index=df.index)
    for shift in range(1, width + 1):
        pivot_high &= highs > highs.shift(shift)
        pivot_high &= highs >= highs.shift(-shift)
        pivot_low &= lows < lows.shift(shift)
        pivot_low &= lows <= lows.shift(-shift)
    out = df.copy()
    out["pivot_high"] = np.where(pivot_high, out["high"], np.nan)
    out["pivot_low"] = np.where(pivot_low, out["low"], np.nan)
    return out


def candle_metrics(candle: pd.Series) -> Dict[str, float]:
    full_range = max(float(candle["high"] - candle["low"]), 1e-12)
    return {
        "range": full_range,
        "body": abs(float(candle["close"] - candle["open"])),
        "lower_wick": float(min(candle["open"], candle["close"]) - candle["low"]),
        "upper_wick": float(candle["high"] - max(candle["open"], candle["close"])),
        "close_position": float((candle["close"] - candle["low"]) / full_range),
    }


def max_drawdown(equity_curve: List[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    drawdown = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        if peak > 0:
            drawdown = max(drawdown, (peak - value) / peak)
    return drawdown


@dataclass
class StrategyConfig:
    days: int = 365
    base_interval: str = "5m"
    signal_interval: str = "15m"
    context_interval: str = "1h"
    structure_interval: str = "4h"
    bias_interval: str = "1d"
    fee_per_side: float = 0.0005
    risk_per_trade: float = 0.01
    initial_capital: float = 10000.0
    entry_timeout_bars_5m: int = 18
    max_hold_bars_5m: int = 576


class BingXClient:
    base_url = "https://open-api.bingx.com"

    def __init__(self, api_key: str = "", secret_key: str = "") -> None:
        self.api_key = api_key or os.getenv("BINGX_API_KEY", "")
        self.secret_key = secret_key or os.getenv("BINGX_SECRET_KEY", "")
        self.session = requests.Session()

    def _sign(self, query: str) -> str:
        return hmac.new(self.secret_key.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 1000,
        pause_seconds: float = 0.03,
    ) -> pd.DataFrame:
        interval_ms_map = {
            "5m": 5 * 60 * 1000,
            "15m": 15 * 60 * 1000,
            "30m": 30 * 60 * 1000,
            "1h": 60 * 60 * 1000,
            "4h": 4 * 60 * 60 * 1000,
            "1d": 24 * 60 * 60 * 1000,
        }
        if interval not in interval_ms_map:
            raise ValueError(f"Unsupported interval: {interval}")

        rows: List[Dict] = []
        interval_ms = interval_ms_map[interval]
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        cursor = start_ms
        step_ms = interval_ms * limit

        while cursor <= end_ms:
            chunk_end = min(end_ms, cursor + step_ms - interval_ms)
            response = self.session.get(
                f"{self.base_url}/openApi/swap/v3/quote/klines",
                params={
                    "symbol": symbol,
                    "interval": interval,
                    "limit": limit,
                    "startTime": cursor,
                    "endTime": chunk_end,
                },
                timeout=(10, 45),
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("code") != 0:
                raise RuntimeError(payload)
            data = payload.get("data", [])
            if not data:
                cursor = chunk_end + interval_ms
                continue
            rows.extend(data)
            cursor = max(int(item["time"]) for item in data) + interval_ms
            time.sleep(pause_seconds)

        if not rows:
            raise RuntimeError(f"No data returned for {symbol} {interval}")

        df = pd.DataFrame(rows)
        df["time"] = pd.to_datetime(df["time"].astype("int64"), unit="ms", utc=True)
        for column in ["open", "high", "low", "close", "volume"]:
            df[column] = df[column].astype(float)
        return (
            df[["time", "open", "high", "low", "close", "volume"]]
            .drop_duplicates("time")
            .sort_values("time")
            .set_index("time")
        )


class DataCache:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _cache_prefix(symbol: str, interval: str, days: int) -> str:
        return f"{symbol.replace('-', '_')}_{interval}_{days}d"

    def cache_path(self, symbol: str, interval: str, days: int, end_time: datetime | None = None) -> Path:
        prefix = self._cache_prefix(symbol, interval, days)
        suffix = ""
        if end_time is not None:
            suffix = "_" + end_time.strftime("%Y%m%dT%H%M%SZ")
        return self.root / f"{prefix}{suffix}.csv"

    def matching_cache_paths(self, symbol: str, interval: str, days: int) -> List[Path]:
        prefix = self._cache_prefix(symbol, interval, days)
        return sorted(
            self.root.glob(f"{prefix}*.csv"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )

    @staticmethod
    def _load_frame(path: Path) -> pd.DataFrame:
        df = pd.read_csv(path, parse_dates=["time"])
        df["time"] = pd.to_datetime(df["time"], utc=True)
        return df.set_index("time").sort_index()

    def load_or_fetch(self, client: BingXClient, symbol: str, interval: str, days: int, end_time: datetime | None = None) -> pd.DataFrame:
        if end_time is None:
            candidate_paths = self.matching_cache_paths(symbol, interval, days)
        else:
            candidate_paths = [self.cache_path(symbol, interval, days, end_time)]

        for path in candidate_paths:
            if not path.exists():
                continue
            try:
                return self._load_frame(path)
            except (pd.errors.EmptyDataError, KeyError, ValueError):
                path.unlink(missing_ok=True)

        resolved_end_time = end_time or now_utc()
        start_time = resolved_end_time - timedelta(days=days)
        df = client.fetch_klines(symbol, interval, start_time, resolved_end_time)
        path = self.cache_path(symbol, interval, days, end_time)
        df.reset_index().to_csv(path, index=False)
        return df
