from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

from correction_regime import BingXClient, DataCache, atr, candle_metrics, find_pivots, max_drawdown, parse_end_time, resample_ohlcv


def resample_weekly(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.resample("W-MON", label="right", closed="right")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna()
    )


def round_level_nearest(price: float, step: float) -> float:
    return round(price / step) * step


def round_level_floor(price: float, step: float) -> float:
    return math.floor(price / step) * step


def round_level_ceil(price: float, step: float) -> float:
    return math.ceil(price / step) * step


@dataclass
class SweepConfig:
    days: int = 30
    end_time: Optional[datetime] = None
    fee_per_side: float = 0.0005
    initial_capital: float = 10.0
    risk_per_trade: float = 0.01
    max_hold_bars_5m: int = 96
    breakeven_buffer_r: float = 0.08
    signal_cooldown_bars: int = 4


@dataclass
class SweepVariant:
    name: str
    signal_interval: str
    swing_width: int
    round_step: float
    use_prev_day: bool
    use_prev_week: bool
    use_last_swing: bool
    use_round: bool
    min_sweep_atr: float
    min_reclaim_atr: float
    min_wick_ratio: float
    min_close_position_long: float
    max_close_position_short: float
    min_body_ratio: float
    min_volume_rel: float
    stop_buffer_atr: float
    tp1_r: float
    tp2_r: float
    min_score: float
    max_stop_atr: float
    long_sources: Optional[tuple[str, ...]] = None
    short_sources: Optional[tuple[str, ...]] = None


@dataclass
class SweepSignal:
    symbol: str
    time: pd.Timestamp
    direction: str
    variant_name: str
    level_source: str
    level_price: float
    planned_entry: float
    stop_price: float
    tp1_r: float
    tp2_r: float
    score: float
    note: str


@dataclass
class SweepTrade:
    symbol: str
    direction: str
    variant_name: str
    level_source: str
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
    score: float
    note: str


def default_sweep_profile() -> str:
    return "sweep_15m_round_reclaim_combo"


def default_variants() -> List[SweepVariant]:
    variants: List[SweepVariant] = []
    for interval in ("15m", "30m"):
        for round_step in (25.0, 50.0):
            for sweep_atr, reclaim_atr, wick_ratio, volume_rel, tp2_r in (
                (0.04, 0.03, 0.42, 1.05, 2.2),
                (0.07, 0.05, 0.48, 1.10, 2.4),
                (0.10, 0.07, 0.55, 1.18, 2.6),
            ):
                variants.append(
                    SweepVariant(
                        name=f"sweep_{interval}_rs{int(round_step)}_sa{int(sweep_atr * 100):02d}_rv{int(volume_rel * 100)}",
                        signal_interval=interval,
                        swing_width=2 if interval == "15m" else 3,
                        round_step=round_step,
                        use_prev_day=True,
                        use_prev_week=interval == "30m",
                        use_last_swing=True,
                        use_round=True,
                        min_sweep_atr=sweep_atr,
                        min_reclaim_atr=reclaim_atr,
                        min_wick_ratio=wick_ratio,
                        min_close_position_long=0.56,
                        max_close_position_short=0.44,
                        min_body_ratio=0.12,
                        min_volume_rel=volume_rel,
                        stop_buffer_atr=0.12 if interval == "15m" else 0.15,
                        tp1_r=1.0,
                        tp2_r=tp2_r,
                        min_score=1.65 if interval == "15m" else 1.75,
                        max_stop_atr=1.55 if interval == "15m" else 1.70,
                    )
                )
    variants.extend(
        [
            SweepVariant(
                name="sweep_15m_round_reclaim_long",
                signal_interval="15m",
                swing_width=2,
                round_step=50.0,
                use_prev_day=True,
                use_prev_week=False,
                use_last_swing=False,
                use_round=True,
                min_sweep_atr=0.07,
                min_reclaim_atr=0.05,
                min_wick_ratio=0.48,
                min_close_position_long=0.56,
                max_close_position_short=0.44,
                min_body_ratio=0.12,
                min_volume_rel=1.10,
                stop_buffer_atr=0.10,
                tp1_r=1.0,
                tp2_r=2.4,
                min_score=1.80,
                max_stop_atr=1.45,
                long_sources=("round_low", "prev_day_low"),
                short_sources=tuple(),
            ),
            SweepVariant(
                name="sweep_15m_round_reclaim_combo",
                signal_interval="15m",
                swing_width=2,
                round_step=50.0,
                use_prev_day=True,
                use_prev_week=False,
                use_last_swing=False,
                use_round=True,
                min_sweep_atr=0.07,
                min_reclaim_atr=0.05,
                min_wick_ratio=0.48,
                min_close_position_long=0.56,
                max_close_position_short=0.44,
                min_body_ratio=0.12,
                min_volume_rel=1.10,
                stop_buffer_atr=0.10,
                tp1_r=1.0,
                tp2_r=2.2,
                min_score=1.75,
                max_stop_atr=1.45,
                long_sources=("round_low", "prev_day_low"),
                short_sources=("round_high",),
            ),
            SweepVariant(
                name="sweep_15m_round_reclaim_fast",
                signal_interval="15m",
                swing_width=2,
                round_step=25.0,
                use_prev_day=True,
                use_prev_week=False,
                use_last_swing=False,
                use_round=True,
                min_sweep_atr=0.07,
                min_reclaim_atr=0.05,
                min_wick_ratio=0.48,
                min_close_position_long=0.56,
                max_close_position_short=0.44,
                min_body_ratio=0.12,
                min_volume_rel=1.10,
                stop_buffer_atr=0.10,
                tp1_r=1.0,
                tp2_r=2.2,
                min_score=1.75,
                max_stop_atr=1.45,
                long_sources=("round_low", "prev_day_low"),
                short_sources=("round_high",),
            ),
        ]
    )
    return variants


def variant_by_name(name: str) -> SweepVariant:
    for variant in default_variants():
        if variant.name == name:
            return variant
    raise KeyError(f"Unknown sweep profile: {name}")


class LiquiditySweepLab:
    def __init__(self, config: SweepConfig, variant: SweepVariant, client: BingXClient, cache: DataCache, output_root: Path) -> None:
        self.config = config
        self.variant = variant
        self.client = client
        self.cache = cache
        self.output_root = output_root
        self.output_root.mkdir(parents=True, exist_ok=True)

    def prepare_frames(self, symbol: str) -> Dict[str, pd.DataFrame]:
        base = self.cache.load_or_fetch(self.client, symbol, "5m", self.config.days, self.config.end_time)
        signal = resample_ohlcv(base, self.variant.signal_interval)
        signal["atr"] = atr(signal, 14)
        signal["volume_ma"] = signal["volume"].rolling(20).mean()
        signal["pivot_high_raw"] = find_pivots(signal, self.variant.swing_width)["pivot_high"]
        signal["pivot_low_raw"] = find_pivots(signal, self.variant.swing_width)["pivot_low"]
        signal["last_pivot_high"] = signal["pivot_high_raw"].ffill()
        signal["last_pivot_low"] = signal["pivot_low_raw"].ffill()

        daily = resample_ohlcv(base, "1d")
        daily_levels = daily[["high", "low"]].rename(columns={"high": "prev_day_high", "low": "prev_day_low"}).shift(1)
        signal = signal.join(daily_levels.reindex(signal.index, method="ffill"))

        weekly = resample_weekly(base)
        weekly_levels = weekly[["high", "low"]].rename(columns={"high": "prev_week_high", "low": "prev_week_low"}).shift(1)
        signal = signal.join(weekly_levels.reindex(signal.index, method="ffill"))

        return {"5m": base, self.variant.signal_interval: signal}

    def _level_candidates(self, candle: pd.Series, direction: str) -> Iterable[tuple[str, float, float]]:
        atr_now = float(candle["atr"])
        if atr_now <= 0 or np.isnan(atr_now):
            return []
        price = float(candle["close"])
        candidates: List[tuple[str, float, float]] = []
        if direction == "short":
            if self.variant.use_prev_day and not np.isnan(float(candle.get("prev_day_high", np.nan))):
                candidates.append(("prev_day_high", float(candle["prev_day_high"]), 1.0))
            if self.variant.use_prev_week and not np.isnan(float(candle.get("prev_week_high", np.nan))):
                candidates.append(("prev_week_high", float(candle["prev_week_high"]), 1.2))
            if self.variant.use_last_swing and not np.isnan(float(candle.get("last_pivot_high", np.nan))):
                candidates.append(("last_swing_high", float(candle["last_pivot_high"]), 1.05))
            if self.variant.use_round:
                round_ref = round_level_nearest(price, self.variant.round_step)
                if abs(price - round_ref) <= 1.2 * atr_now:
                    candidates.append(("round_high", round_ref, 1.0))
                round_up = round_level_ceil(float(candle["high"]), self.variant.round_step)
                if abs(round_up - float(candle["high"])) <= 1.2 * atr_now:
                    candidates.append(("round_high_ext", round_up, 0.95))
        else:
            if self.variant.use_prev_day and not np.isnan(float(candle.get("prev_day_low", np.nan))):
                candidates.append(("prev_day_low", float(candle["prev_day_low"]), 1.0))
            if self.variant.use_prev_week and not np.isnan(float(candle.get("prev_week_low", np.nan))):
                candidates.append(("prev_week_low", float(candle["prev_week_low"]), 1.2))
            if self.variant.use_last_swing and not np.isnan(float(candle.get("last_pivot_low", np.nan))):
                candidates.append(("last_swing_low", float(candle["last_pivot_low"]), 1.05))
            if self.variant.use_round:
                round_ref = round_level_nearest(price, self.variant.round_step)
                if abs(price - round_ref) <= 1.2 * atr_now:
                    candidates.append(("round_low", round_ref, 1.0))
                round_down = round_level_floor(float(candle["low"]), self.variant.round_step)
                if abs(round_down - float(candle["low"])) <= 1.2 * atr_now:
                    candidates.append(("round_low_ext", round_down, 0.95))
        return candidates

    def build_signal(self, symbol: str, frames: Dict[str, pd.DataFrame], timestamp: pd.Timestamp) -> Optional[SweepSignal]:
        signal_frame = frames[self.variant.signal_interval]
        if timestamp not in signal_frame.index:
            return None
        idx = signal_frame.index.get_loc(timestamp)
        if isinstance(idx, slice) or idx < 20:
            return None
        candle = signal_frame.iloc[idx]
        atr_now = float(candle["atr"])
        volume_ma = float(candle["volume_ma"])
        if np.isnan(atr_now) or atr_now <= 0 or np.isnan(volume_ma) or volume_ma <= 0:
            return None

        metrics = candle_metrics(candle)
        volume_rel = float(candle["volume"]) / max(volume_ma, 1e-9)
        body_ratio = metrics["body"] / max(metrics["range"], 1e-9)
        if volume_rel < self.variant.min_volume_rel or body_ratio < self.variant.min_body_ratio:
            return None

        best: Optional[SweepSignal] = None
        for direction in ("long", "short"):
            for source_name, level_price, source_weight in self._level_candidates(candle, direction):
                if direction == "long" and self.variant.long_sources is not None and source_name not in self.variant.long_sources:
                    continue
                if direction == "short" and self.variant.short_sources is not None and source_name not in self.variant.short_sources:
                    continue
                if direction == "short":
                    sweep_distance = float(candle["high"]) - level_price
                    reclaim_distance = level_price - float(candle["close"])
                    wick_ratio = metrics["upper_wick"] / max(metrics["range"], 1e-9)
                    close_position_ok = metrics["close_position"] <= self.variant.max_close_position_short
                    stop_price = float(candle["high"]) + self.variant.stop_buffer_atr * atr_now
                    initiative_bonus = 0.18 if float(candle["close"]) < float(candle["open"]) else 0.0
                else:
                    sweep_distance = level_price - float(candle["low"])
                    reclaim_distance = float(candle["close"]) - level_price
                    wick_ratio = metrics["lower_wick"] / max(metrics["range"], 1e-9)
                    close_position_ok = metrics["close_position"] >= self.variant.min_close_position_long
                    stop_price = float(candle["low"]) - self.variant.stop_buffer_atr * atr_now
                    initiative_bonus = 0.18 if float(candle["close"]) > float(candle["open"]) else 0.0

                if sweep_distance < self.variant.min_sweep_atr * atr_now:
                    continue
                if reclaim_distance < self.variant.min_reclaim_atr * atr_now:
                    continue
                if wick_ratio < self.variant.min_wick_ratio or not close_position_ok:
                    continue

                planned_entry = float(candle["close"])
                stop_atr = abs(planned_entry - stop_price) / atr_now
                if stop_atr <= 0 or stop_atr > self.variant.max_stop_atr:
                    continue

                score = (
                    source_weight
                    + min(sweep_distance / atr_now, 1.5) * 0.55
                    + min(reclaim_distance / atr_now, 1.2) * 0.45
                    + wick_ratio * 0.65
                    + min(volume_rel, 2.0) * 0.25
                    + initiative_bonus
                )
                if score < self.variant.min_score:
                    continue

                signal = SweepSignal(
                    symbol=symbol,
                    time=timestamp,
                    direction=direction,
                    variant_name=self.variant.name,
                    level_source=source_name,
                    level_price=level_price,
                    planned_entry=planned_entry,
                    stop_price=float(stop_price),
                    tp1_r=self.variant.tp1_r,
                    tp2_r=self.variant.tp2_r,
                    score=float(score),
                    note=f"liquidity_sweep:{self.variant.name}:{source_name}",
                )
                if best is None or signal.score > best.score:
                    best = signal
        return best


def execute_trade(config: SweepConfig, signal: SweepSignal, base: pd.DataFrame) -> Optional[SweepTrade]:
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
    return SweepTrade(
        symbol=signal.symbol,
        direction=signal.direction,
        variant_name=signal.variant_name,
        level_source=signal.level_source,
        setup_time=signal.time.isoformat(),
        entry_time=entry_time.isoformat(),
        exit_time=exit_time.isoformat(),
        entry_price=float(entry_price),
        stop_price=float(signal.stop_price),
        tp1_price=float(tp1_price),
        tp2_price=float(tp2_price),
        exit_price=float(exit_price),
        r_multiple=float(r_multiple),
        net_pnl=float(net_pnl),
        hold_hours=(exit_time - entry_time).total_seconds() / 3600.0,
        exit_reason=exit_reason,
        score=float(signal.score),
        note=signal.note,
    )


def summarize(initial_capital: float, trades: List[SweepTrade], days: int) -> Dict[str, float]:
    if not trades:
        return {
            "trades": 0,
            "trades_per_month": 0.0,
            "win_rate": 0.0,
            "expectancy_r": 0.0,
            "profit_factor": 0.0,
            "net_pnl": 0.0,
            "max_drawdown_pct": 0.0,
            "avg_hold_hours": 0.0,
        }
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


def run_variant(lab: LiquiditySweepLab, config: SweepConfig, symbol: str) -> Dict[str, object]:
    frames = lab.prepare_frames(symbol)
    signal_frame = frames[lab.variant.signal_interval]
    trades: List[SweepTrade] = []
    last_signal_time: Optional[pd.Timestamp] = None
    next_available: Optional[pd.Timestamp] = None

    for timestamp in signal_frame.index[40:]:
        if next_available is not None and timestamp <= next_available:
            continue
        if last_signal_time is not None:
            bars_since = int((timestamp - last_signal_time).total_seconds() // (pd.Timedelta(lab.variant.signal_interval).total_seconds()))
            if bars_since < config.signal_cooldown_bars:
                continue
        signal = lab.build_signal(symbol, frames, timestamp)
        if signal is None:
            continue
        last_signal_time = timestamp
        trade = execute_trade(config, signal, frames["5m"])
        if trade is None:
            continue
        trades.append(trade)
        next_available = pd.Timestamp(trade.exit_time)
    return {"trades": trades, "summary": summarize(config.initial_capital, trades, config.days)}


def run_sweep_profile(
    profile_name: str,
    days: int,
    symbol: str,
    client: BingXClient,
    cache: DataCache,
    output_root: Path,
    end_time: Optional[datetime] = None,
) -> List[SweepTrade]:
    config = SweepConfig(days=days, end_time=end_time)
    variant = variant_by_name(profile_name)
    lab = LiquiditySweepLab(config, variant, client, cache, output_root)
    return run_variant(lab, config, symbol)["trades"]


def choose_best_result(results: List[Dict[str, object]], min_trades: int, max_trades: int) -> Dict[str, object]:
    in_band = [item for item in results if min_trades <= int(item["summary"]["trades"]) <= max_trades]
    ranked = in_band if in_band else results
    ranked.sort(
        key=lambda item: (
            item["summary"]["net_pnl"],
            item["summary"]["expectancy_r"],
            item["summary"]["win_rate"],
            -item["summary"]["max_drawdown_pct"],
        ),
        reverse=True,
    )
    return ranked[0]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Liquidity sweep + reclaim ETH signal lab.")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--end-date", default=None, help="UTC end date for the test window, for example 2026-03-20")
    parser.add_argument("--symbol", default="ETH-USDT")
    parser.add_argument("--cache-dir", default="correction/data_cache")
    parser.add_argument("--output-dir", default="correction/reports/liquidity_sweep_lab")
    parser.add_argument("--min-trades", type=int, default=10)
    parser.add_argument("--max-trades", type=int, default=20)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    client = BingXClient()
    cache = DataCache(Path(args.cache_dir))
    end_time = parse_end_time(args.end_date)
    results: List[Dict[str, object]] = []

    for variant in default_variants():
        config = SweepConfig(days=args.days, end_time=end_time)
        lab = LiquiditySweepLab(config, variant, client, cache, output_root)
        run = run_variant(lab, config, args.symbol)
        variant_dir = output_root / f"{variant.name}_{args.days}d"
        variant_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "days": args.days,
            "end_date": args.end_date,
            "symbol": args.symbol,
            "variant": asdict(variant),
            "summary": run["summary"],
            "trades": [asdict(item) for item in run["trades"]],
        }
        with (variant_dir / "summary.json").open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
        results.append({"variant": variant.name, "summary": run["summary"], "path": str((variant_dir / "summary.json").resolve())})

    best = choose_best_result(results, args.min_trades, args.max_trades)
    comparison = {
        "days": args.days,
        "end_date": args.end_date,
        "symbol": args.symbol,
        "target_trade_band": {"min": args.min_trades, "max": args.max_trades},
        "best_variant": best,
        "results": sorted(
            results,
            key=lambda item: (
                item["summary"]["net_pnl"],
                item["summary"]["expectancy_r"],
                item["summary"]["win_rate"],
            ),
            reverse=True,
        ),
    }
    with (output_root / "comparison.json").open("w", encoding="utf-8") as handle:
        json.dump(comparison, handle, indent=2, ensure_ascii=False)
    print(json.dumps(comparison, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
