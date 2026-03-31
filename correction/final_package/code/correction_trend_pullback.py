from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from correction_regime import BingXClient, DataCache, atr, candle_metrics, efficiency_ratio, linreg_t_stat, max_drawdown, resample_ohlcv


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def anchored_vwap(df: pd.DataFrame) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    return (typical * df["volume"]).cumsum().divide(df["volume"].cumsum().replace(0.0, np.nan))


@dataclass
class PullbackConfig:
    days: int = 90
    end_time: Optional[datetime] = None
    fee_per_side: float = 0.0005
    initial_capital: float = 10.0
    risk_per_trade: float = 0.01
    signal_cooldown_bars_15m: int = 4
    max_hold_bars_5m: int = 96
    context_trend_t_1h: float = 0.28
    min_ema_gap_atr_1h: float = 0.10
    stop_buffer_atr_15m: float = 0.18
    breakeven_buffer_r: float = 0.04


@dataclass
class PullbackVariant:
    name: str
    min_er_15m: float
    min_tstat_15m: float
    require_avwap: bool
    require_ema_touch: bool
    pullback_bars: int
    zone_tolerance_atr: float
    min_resume_body_ratio: float
    min_stop_atr: float
    max_stop_atr: float
    tp1_r: float
    tp2_r: float


@dataclass
class PullbackSignal:
    symbol: str
    time: pd.Timestamp
    direction: str
    variant_name: str
    planned_entry: float
    stop_price: float
    tp1_r: float
    tp2_r: float
    note: str


@dataclass
class PullbackTrade:
    symbol: str
    direction: str
    variant_name: str
    setup_time: str
    entry_time: str
    exit_time: str
    entry_price: float
    stop_price: float
    tp1_price: float
    tp2_price: float
    exit_price: float
    r_multiple: float
    net_pnl: float
    hold_hours: float
    exit_reason: str
    note: str


def default_variants() -> List[PullbackVariant]:
    return [
        PullbackVariant(
            name="ema_pullback_15m",
            min_er_15m=0.28,
            min_tstat_15m=0.10,
            require_avwap=False,
            require_ema_touch=True,
            pullback_bars=3,
            zone_tolerance_atr=0.18,
            min_resume_body_ratio=0.28,
            min_stop_atr=0.35,
            max_stop_atr=1.80,
            tp1_r=0.75,
            tp2_r=1.55,
        ),
        PullbackVariant(
            name="ema_avwap_pullback_15m",
            min_er_15m=0.24,
            min_tstat_15m=0.08,
            require_avwap=True,
            require_ema_touch=True,
            pullback_bars=4,
            zone_tolerance_atr=0.22,
            min_resume_body_ratio=0.24,
            min_stop_atr=0.35,
            max_stop_atr=1.90,
            tp1_r=0.72,
            tp2_r=1.45,
        ),
        PullbackVariant(
            name="micro_pullback_15m",
            min_er_15m=0.22,
            min_tstat_15m=0.06,
            require_avwap=False,
            require_ema_touch=False,
            pullback_bars=2,
            zone_tolerance_atr=0.28,
            min_resume_body_ratio=0.22,
            min_stop_atr=0.30,
            max_stop_atr=1.60,
            tp1_r=0.68,
            tp2_r=1.35,
        ),
    ]


class EthTrendPullbackLab:
    def __init__(self, config: PullbackConfig, variant: PullbackVariant, client: BingXClient, cache: DataCache, output_root: Path) -> None:
        self.config = config
        self.variant = variant
        self.client = client
        self.cache = cache
        self.output_root = output_root
        self.output_root.mkdir(parents=True, exist_ok=True)

    def prepare_frames(self, symbol: str) -> Dict[str, pd.DataFrame]:
        base = self.cache.load_or_fetch(self.client, symbol, "5m", self.config.days, self.config.end_time)
        frames = {"5m": base, "15m": resample_ohlcv(base, "15m"), "1h": resample_ohlcv(base, "1h")}
        frames["15m"]["atr"] = atr(frames["15m"], 14)
        frames["15m"]["er"] = efficiency_ratio(frames["15m"]["close"], 12)
        frames["15m"]["trend_t"] = linreg_t_stat(frames["15m"]["close"], 12)
        frames["15m"]["ema20"] = ema(frames["15m"]["close"], 20)
        frames["1h"]["atr"] = atr(frames["1h"], 14)
        frames["1h"]["trend_t"] = linreg_t_stat(frames["1h"]["close"], 20)
        frames["1h"]["ema20"] = ema(frames["1h"]["close"], 20)
        frames["1h"]["ema50"] = ema(frames["1h"]["close"], 50)
        return frames

    @staticmethod
    def slice_frame(frame: pd.DataFrame, timestamp: pd.Timestamp, length: int) -> Optional[pd.DataFrame]:
        view = frame.loc[:timestamp]
        if len(view) < length:
            return None
        return view.iloc[-length:]

    def trend_direction(self, h1: pd.DataFrame) -> Optional[str]:
        current = h1.iloc[-1]
        prev = h1.iloc[-2]
        atr_now = float(current["atr"])
        if np.isnan(atr_now) or atr_now <= 0:
            return None
        ema_gap = abs(float(current["ema20"]) - float(current["ema50"])) / atr_now
        if ema_gap < self.config.min_ema_gap_atr_1h:
            return None
        trend_t = float(current["trend_t"])
        if (
            float(current["ema20"]) > float(current["ema50"])
            and float(current["ema20"]) >= float(prev["ema20"])
            and float(current["close"]) > float(current["ema20"])
            and trend_t >= self.config.context_trend_t_1h
        ):
            return "long"
        if (
            float(current["ema20"]) < float(current["ema50"])
            and float(current["ema20"]) <= float(prev["ema20"])
            and float(current["close"]) < float(current["ema20"])
            and trend_t <= -self.config.context_trend_t_1h
        ):
            return "short"
        return None

    def build_signal(self, symbol: str, frames: Dict[str, pd.DataFrame], timestamp: pd.Timestamp) -> Optional[PullbackSignal]:
        h1 = self.slice_frame(frames["1h"], timestamp, 60)
        confirm = self.slice_frame(frames["15m"], timestamp, 80)
        if h1 is None or confirm is None:
            return None
        direction = self.trend_direction(h1)
        if direction is None:
            return None
        recent = confirm.iloc[-(self.variant.pullback_bars + 2) :]
        if len(recent) < self.variant.pullback_bars + 2:
            return None
        current = recent.iloc[-1]
        prev = recent.iloc[-2]
        pullback = recent.iloc[-(self.variant.pullback_bars + 1) : -1]
        atr_now = float(current["atr"])
        if np.isnan(atr_now) or atr_now <= 0:
            return None
        if float(current["er"]) < self.variant.min_er_15m:
            return None
        trend_t = float(current["trend_t"])
        if direction == "long" and trend_t < self.variant.min_tstat_15m:
            return None
        if direction == "short" and trend_t > -self.variant.min_tstat_15m:
            return None

        avwap_now = float(anchored_vwap(recent).iloc[-1])
        metrics = candle_metrics(current)
        if direction == "long":
            pullback_low = float(pullback["low"].min())
            had_red = bool((pullback["close"] < pullback["open"]).any())
            touched_ema = pullback_low <= float(pullback["ema20"].iloc[-1]) + self.variant.zone_tolerance_atr * atr_now
            avwap_ok = (not self.variant.require_avwap) or (pullback_low <= avwap_now + self.variant.zone_tolerance_atr * atr_now and float(current["close"]) >= avwap_now)
            resume_ok = (
                float(current["close"]) > float(current["open"])
                and float(current["close"]) > float(prev["high"])
                and metrics["body"] / max(metrics["range"], 1e-9) >= self.variant.min_resume_body_ratio
            )
            if not (had_red and avwap_ok and resume_ok and ((not self.variant.require_ema_touch) or touched_ema)):
                return None
            planned_entry = float(current["close"])
            stop_price = pullback_low - self.config.stop_buffer_atr_15m * atr_now
        else:
            pullback_high = float(pullback["high"].max())
            had_green = bool((pullback["close"] > pullback["open"]).any())
            touched_ema = pullback_high >= float(pullback["ema20"].iloc[-1]) - self.variant.zone_tolerance_atr * atr_now
            avwap_ok = (not self.variant.require_avwap) or (pullback_high >= avwap_now - self.variant.zone_tolerance_atr * atr_now and float(current["close"]) <= avwap_now)
            resume_ok = (
                float(current["close"]) < float(current["open"])
                and float(current["close"]) < float(prev["low"])
                and metrics["body"] / max(metrics["range"], 1e-9) >= self.variant.min_resume_body_ratio
            )
            if not (had_green and avwap_ok and resume_ok and ((not self.variant.require_ema_touch) or touched_ema)):
                return None
            planned_entry = float(current["close"])
            stop_price = pullback_high + self.config.stop_buffer_atr_15m * atr_now

        stop_atr = abs(planned_entry - stop_price) / atr_now
        if stop_atr < self.variant.min_stop_atr or stop_atr > self.variant.max_stop_atr:
            return None
        return PullbackSignal(symbol, timestamp, direction, self.variant.name, planned_entry, float(stop_price), self.variant.tp1_r, self.variant.tp2_r, f"trend_pullback:{self.variant.name}")


def execute_trade(config: PullbackConfig, signal: PullbackSignal, base: pd.DataFrame) -> Optional[PullbackTrade]:
    start_position = base.index.searchsorted(signal.time, side="right")
    if start_position >= len(base):
        return None
    entry_candle = base.iloc[start_position]
    entry_time = base.index[start_position]
    entry_price = float(entry_candle["open"])
    risk_points = abs(entry_price - signal.stop_price)
    if risk_points <= 0:
        return None
    tp1_price = entry_price + signal.tp1_r * risk_points if signal.direction == "long" else entry_price - signal.tp1_r * risk_points
    tp2_price = entry_price + signal.tp2_r * risk_points if signal.direction == "long" else entry_price - signal.tp2_r * risk_points
    quantity_initial = max((config.initial_capital * config.risk_per_trade) / risk_points, 0.0001)
    quantity_open = quantity_initial
    stop_price = signal.stop_price
    realized = 0.0
    took_tp1 = False
    exit_price: Optional[float] = None
    exit_time: Optional[pd.Timestamp] = None
    exit_reason: Optional[str] = None
    last_idx = min(start_position + config.max_hold_bars_5m, len(base) - 1)
    for idx in range(start_position, last_idx + 1):
        candle = base.iloc[idx]
        high = float(candle["high"])
        low = float(candle["low"])
        close = float(candle["close"])
        if signal.direction == "long":
            if (not took_tp1) and high >= tp1_price:
                realized += quantity_initial * 0.5 * (tp1_price - entry_price)
                quantity_open = quantity_initial * 0.5
                stop_price = max(stop_price, entry_price + config.breakeven_buffer_r * risk_points)
                took_tp1 = True
            if low <= stop_price:
                exit_price, exit_time, exit_reason = stop_price, base.index[idx], "stop"
                break
            if high >= tp2_price:
                exit_price, exit_time, exit_reason = tp2_price, base.index[idx], "tp2"
                break
        else:
            if (not took_tp1) and low <= tp1_price:
                realized += quantity_initial * 0.5 * (entry_price - tp1_price)
                quantity_open = quantity_initial * 0.5
                stop_price = min(stop_price, entry_price - config.breakeven_buffer_r * risk_points)
                took_tp1 = True
            if high >= stop_price:
                exit_price, exit_time, exit_reason = stop_price, base.index[idx], "stop"
                break
            if low <= tp2_price:
                exit_price, exit_time, exit_reason = tp2_price, base.index[idx], "tp2"
                break
        if idx == last_idx:
            exit_price, exit_time, exit_reason = close, base.index[idx], "time_stop"
    if exit_price is None or exit_time is None or exit_reason is None:
        return None
    gross = realized + quantity_open * ((exit_price - entry_price) if signal.direction == "long" else (entry_price - exit_price))
    fees = quantity_initial * entry_price * config.fee_per_side * 2.0
    net_pnl = gross - fees
    theoretical_risk = quantity_initial * risk_points
    r_multiple = net_pnl / theoretical_risk if theoretical_risk > 0 else 0.0
    return PullbackTrade(
        signal.symbol,
        signal.direction,
        signal.variant_name,
        signal.time.isoformat(),
        entry_time.isoformat(),
        exit_time.isoformat(),
        float(entry_price),
        float(signal.stop_price),
        float(tp1_price),
        float(tp2_price),
        float(exit_price),
        float(r_multiple),
        float(net_pnl),
        (exit_time - entry_time).total_seconds() / 3600.0,
        exit_reason,
        signal.note,
    )


def summarize(initial_capital: float, trades: List[PullbackTrade], days: int) -> Dict[str, float]:
    if not trades:
        return {"trades": 0, "trades_per_month": 0.0, "win_rate": 0.0, "expectancy_r": 0.0, "profit_factor": 0.0, "net_pnl": 0.0, "max_drawdown_pct": 0.0, "avg_hold_hours": 0.0}
    pnl = np.array([trade.net_pnl for trade in trades], dtype=float)
    r_values = np.array([trade.r_multiple for trade in trades], dtype=float)
    winners = pnl[pnl > 0]
    losers = pnl[pnl <= 0]
    profit_factor = float(winners.sum() / abs(losers.sum())) if losers.size and abs(losers.sum()) > 0 else (float("inf") if winners.size else 0.0)
    balance = initial_capital
    equity = [balance]
    for trade in trades:
        balance += trade.net_pnl
        equity.append(balance)
    return {
        "trades": int(len(trades)),
        "trades_per_month": float(len(trades) / max(days / 30.0, 1.0)),
        "win_rate": float((r_values > 0).mean() * 100.0),
        "expectancy_r": float(r_values.mean()),
        "profit_factor": profit_factor,
        "net_pnl": float(pnl.sum()),
        "max_drawdown_pct": float(max_drawdown(equity) * 100.0),
        "avg_hold_hours": float(np.mean([trade.hold_hours for trade in trades])),
    }


def run_variant(lab: EthTrendPullbackLab, config: PullbackConfig, symbol: str) -> Dict[str, object]:
    frames = lab.prepare_frames(symbol)
    trades: List[PullbackTrade] = []
    next_available: Optional[pd.Timestamp] = None
    last_signal: Optional[pd.Timestamp] = None
    for timestamp in frames["15m"].index[120:]:
        if next_available is not None and timestamp <= next_available:
            continue
        if last_signal is not None:
            bars_since = int((timestamp - last_signal).total_seconds() // (15 * 60))
            if bars_since < config.signal_cooldown_bars_15m:
                continue
        signal = lab.build_signal(symbol, frames, timestamp)
        if signal is None:
            continue
        trade = execute_trade(config, signal, frames["5m"])
        last_signal = timestamp
        if trade is None:
            continue
        trades.append(trade)
        next_available = pd.Timestamp(trade.exit_time)
    return {"trades": trades, "summary": summarize(config.initial_capital, trades, config.days)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recent ETH trend pullback lab.")
    parser.add_argument("--days", nargs="+", type=int, default=[60, 90])
    parser.add_argument("--symbol", default="ETH-USDT")
    parser.add_argument("--cache-dir", default="correction/data_cache")
    parser.add_argument("--output-dir", default="correction/reports/trend_pullback_lab")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    client = BingXClient()
    cache = DataCache(Path(args.cache_dir))
    results: List[Dict[str, object]] = []
    for days in args.days:
        config = PullbackConfig(days=days)
        for variant in default_variants():
            lab = EthTrendPullbackLab(config, variant, client, cache, output_root)
            run = run_variant(lab, config, args.symbol)
            variant_dir = output_root / f"{variant.name}_{days}d"
            variant_dir.mkdir(parents=True, exist_ok=True)
            with (variant_dir / "summary.json").open("w", encoding="utf-8") as handle:
                json.dump({"days": days, "symbol": args.symbol, "variant": asdict(variant), "summary": run["summary"], "trades": [asdict(item) for item in run["trades"]]}, handle, indent=2, ensure_ascii=False)
            results.append({"days": days, "variant": variant.name, "symbol": args.symbol, "summary": run["summary"]})
    results.sort(key=lambda item: (item["days"], item["summary"]["net_pnl"], item["summary"]["win_rate"]), reverse=True)
    with (output_root / "comparison.json").open("w", encoding="utf-8") as handle:
        json.dump(results, handle, indent=2, ensure_ascii=False)
    print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
