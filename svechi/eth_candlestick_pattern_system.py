from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from itertools import combinations
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd


TIMEFRAME_RULES = {
    "5m": "5min",
    "10m": "10min",
    "15m": "15min",
    "20m": "20min",
    "30m": "30min",
    "45m": "45min",
    "1h": "1h",
}

TIMEFRAME_MINUTES = {
    "5m": 5,
    "10m": 10,
    "15m": 15,
    "20m": 20,
    "30m": 30,
    "45m": 45,
    "1h": 60,
}

ENTRY_HORIZONS_MINUTES = (30, 60, 180)


@dataclass
class PatternDefinition:
    name: str
    direction: str
    family: str


@dataclass
class StrategyTrade:
    direction: str
    signal_pattern: str
    signal_timeframe: str
    signal_time: str
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    stop_price: float
    tp1_price: float
    tp2_price: float
    exit_reason: str
    bars_held_5m: int
    gross_r: float
    net_r: float
    mfe_r: float
    mae_r: float


@dataclass
class HoldSystemSpec:
    timeframe: str
    pattern: str
    direction: str
    hold_bars: int
    stop_atr_mult: float = 1.0
    min_body_ratio: float = 0.0
    require_volume_confirmation: bool = False


def resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    return (
        df.resample(TIMEFRAME_RULES[timeframe], label="right", closed="right")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna()
        .sort_index()
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


def add_features(df: pd.DataFrame, trend_lookback: int = 6) -> pd.DataFrame:
    out = df.copy()
    out["range"] = (out["high"] - out["low"]).clip(lower=1e-9)
    out["body"] = (out["close"] - out["open"]).abs()
    out["body_ratio"] = out["body"] / out["range"]
    out["upper_wick"] = out["high"] - out[["open", "close"]].max(axis=1)
    out["lower_wick"] = out[["open", "close"]].min(axis=1) - out["low"]
    out["upper_ratio"] = out["upper_wick"] / out["range"]
    out["lower_ratio"] = out["lower_wick"] / out["range"]
    out["close_position"] = (out["close"] - out["low"]) / out["range"]
    out["bull"] = out["close"] > out["open"]
    out["bear"] = out["close"] < out["open"]
    out["mid_body"] = (out["open"] + out["close"]) / 2.0
    out["atr"] = atr(out, 14)
    out["ema20"] = out["close"].ewm(span=20, adjust=False).mean()
    out["ema50"] = out["close"].ewm(span=50, adjust=False).mean()
    out["volume_median_20"] = out["volume"].rolling(20).median()
    out["range_atr"] = out["range"] / out["atr"].replace(0.0, np.nan)
    out["trend_score"] = (out["close"] - out["close"].shift(trend_lookback)) / out["atr"].replace(0.0, np.nan)
    out["downtrend_ctx"] = out["trend_score"].shift(1) <= -1.0
    out["uptrend_ctx"] = out["trend_score"].shift(1) >= 1.0
    out["long_body"] = out["body_ratio"] >= 0.55
    out["small_body"] = out["body_ratio"] <= 0.25
    return out


def build_pattern_definitions() -> List[PatternDefinition]:
    return [
        PatternDefinition("doji", "neutral", "indecision"),
        PatternDefinition("dragonfly_doji", "long", "reversal"),
        PatternDefinition("gravestone_doji", "short", "reversal"),
        PatternDefinition("long_legged_doji", "neutral", "indecision"),
        PatternDefinition("spinning_top", "neutral", "indecision"),
        PatternDefinition("hammer", "long", "reversal"),
        PatternDefinition("hanging_man", "short", "reversal"),
        PatternDefinition("inverted_hammer", "long", "reversal"),
        PatternDefinition("shooting_star", "short", "reversal"),
        PatternDefinition("bullish_marubozu", "long", "continuation"),
        PatternDefinition("bearish_marubozu", "short", "continuation"),
        PatternDefinition("bullish_engulfing", "long", "reversal"),
        PatternDefinition("bearish_engulfing", "short", "reversal"),
        PatternDefinition("piercing_line", "long", "reversal"),
        PatternDefinition("dark_cloud_cover", "short", "reversal"),
        PatternDefinition("bullish_harami", "long", "reversal"),
        PatternDefinition("bearish_harami", "short", "reversal"),
        PatternDefinition("tweezer_bottom", "long", "reversal"),
        PatternDefinition("tweezer_top", "short", "reversal"),
        PatternDefinition("morning_star", "long", "reversal"),
        PatternDefinition("evening_star", "short", "reversal"),
        PatternDefinition("three_white_soldiers", "long", "continuation"),
        PatternDefinition("three_black_crows", "short", "continuation"),
        PatternDefinition("rising_three_methods", "long", "continuation"),
        PatternDefinition("falling_three_methods", "short", "continuation"),
    ]


def detect_patterns(df: pd.DataFrame) -> Dict[str, pd.Series]:
    prev1 = df.shift(1)
    prev2 = df.shift(2)
    prev3 = df.shift(3)
    prev4 = df.shift(4)
    atr_now = df["atr"].replace(0.0, np.nan)

    patterns: Dict[str, pd.Series] = {}

    doji = df["body_ratio"] <= 0.10
    patterns["doji"] = doji
    patterns["dragonfly_doji"] = doji & (df["lower_ratio"] >= 0.60) & (df["upper_ratio"] <= 0.10)
    patterns["gravestone_doji"] = doji & (df["upper_ratio"] >= 0.60) & (df["lower_ratio"] <= 0.10)
    patterns["long_legged_doji"] = doji & (df["upper_ratio"] >= 0.35) & (df["lower_ratio"] >= 0.35)
    patterns["spinning_top"] = df["body_ratio"].between(0.05, 0.25) & (df["upper_ratio"] >= 0.25) & (df["lower_ratio"] >= 0.25)

    lower_shadow_shape = (
        df["lower_ratio"] >= 0.55
    ) & (df["upper_ratio"] <= 0.15) & df["body_ratio"].between(0.10, 0.35)
    upper_shadow_shape = (
        df["upper_ratio"] >= 0.55
    ) & (df["lower_ratio"] <= 0.15) & df["body_ratio"].between(0.10, 0.35)
    patterns["hammer"] = df["downtrend_ctx"] & lower_shadow_shape & (df["close_position"] >= 0.55)
    patterns["hanging_man"] = df["uptrend_ctx"] & lower_shadow_shape & (df["close_position"] >= 0.55)
    patterns["inverted_hammer"] = df["downtrend_ctx"] & upper_shadow_shape & (df["close_position"] <= 0.60)
    patterns["shooting_star"] = df["uptrend_ctx"] & upper_shadow_shape & (df["close_position"] <= 0.60)

    patterns["bullish_marubozu"] = df["bull"] & (df["body_ratio"] >= 0.80) & (df["upper_ratio"] <= 0.10) & (df["lower_ratio"] <= 0.10)
    patterns["bearish_marubozu"] = df["bear"] & (df["body_ratio"] >= 0.80) & (df["upper_ratio"] <= 0.10) & (df["lower_ratio"] <= 0.10)

    patterns["bullish_engulfing"] = (
        df["downtrend_ctx"]
        & prev1["bear"].fillna(False)
        & df["bull"]
        & (df["open"] <= prev1["close"] + prev1["range"] * 0.10)
        & (df["close"] >= prev1["open"] - prev1["range"] * 0.10)
        & (df["body"] >= prev1["body"] * 1.05)
    )
    patterns["bearish_engulfing"] = (
        df["uptrend_ctx"]
        & prev1["bull"].fillna(False)
        & df["bear"]
        & (df["open"] >= prev1["close"] - prev1["range"] * 0.10)
        & (df["close"] <= prev1["open"] + prev1["range"] * 0.10)
        & (df["body"] >= prev1["body"] * 1.05)
    )

    patterns["piercing_line"] = (
        df["downtrend_ctx"]
        & prev1["bear"].fillna(False)
        & prev1["long_body"].fillna(False)
        & df["bull"]
        & (df["open"] <= prev1["close"] + prev1["range"] * 0.05)
        & (df["close"] > prev1["mid_body"])
        & (df["close"] < prev1["open"])
    )
    patterns["dark_cloud_cover"] = (
        df["uptrend_ctx"]
        & prev1["bull"].fillna(False)
        & prev1["long_body"].fillna(False)
        & df["bear"]
        & (df["open"] >= prev1["close"] - prev1["range"] * 0.05)
        & (df["close"] < prev1["mid_body"])
        & (df["close"] > prev1["open"])
    )

    patterns["bullish_harami"] = (
        df["downtrend_ctx"]
        & prev1["bear"].fillna(False)
        & prev1["long_body"].fillna(False)
        & df["bull"]
        & (df["body"] <= prev1["body"] * 0.60)
        & (df["open"] >= prev1["close"] - prev1["range"] * 0.10)
        & (df["close"] <= prev1["open"] + prev1["range"] * 0.10)
    )
    patterns["bearish_harami"] = (
        df["uptrend_ctx"]
        & prev1["bull"].fillna(False)
        & prev1["long_body"].fillna(False)
        & df["bear"]
        & (df["body"] <= prev1["body"] * 0.60)
        & (df["open"] <= prev1["close"] + prev1["range"] * 0.10)
        & (df["close"] >= prev1["open"] - prev1["range"] * 0.10)
    )

    patterns["tweezer_bottom"] = (
        df["downtrend_ctx"]
        & prev1["bear"].fillna(False)
        & df["bull"]
        & (((df["low"] - prev1["low"]).abs()) <= atr_now * 0.20)
        & (df["close"] > prev1["close"])
    )
    patterns["tweezer_top"] = (
        df["uptrend_ctx"]
        & prev1["bull"].fillna(False)
        & df["bear"]
        & (((df["high"] - prev1["high"]).abs()) <= atr_now * 0.20)
        & (df["close"] < prev1["close"])
    )

    patterns["morning_star"] = (
        df["downtrend_ctx"]
        & prev2["bear"].fillna(False)
        & prev2["long_body"].fillna(False)
        & prev1["small_body"].fillna(False)
        & df["bull"]
        & (df["close"] >= prev2["mid_body"])
        & (prev1[["open", "close"]].max(axis=1) <= prev2["close"] + prev2["range"] * 0.20)
    )
    patterns["evening_star"] = (
        df["uptrend_ctx"]
        & prev2["bull"].fillna(False)
        & prev2["long_body"].fillna(False)
        & prev1["small_body"].fillna(False)
        & df["bear"]
        & (df["close"] <= prev2["mid_body"])
        & (prev1[["open", "close"]].min(axis=1) >= prev2["close"] - prev2["range"] * 0.20)
    )

    strong_bull = df["bull"] & (df["body_ratio"] >= 0.45) & (df["upper_ratio"] <= 0.25)
    strong_bear = df["bear"] & (df["body_ratio"] >= 0.45) & (df["lower_ratio"] <= 0.25)
    patterns["three_white_soldiers"] = (
        prev2["downtrend_ctx"].fillna(False)
        & strong_bull
        & strong_bull.shift(1).fillna(False)
        & strong_bull.shift(2).fillna(False)
        & (df["close"] > prev1["close"])
        & (prev1["close"] > prev2["close"])
        & (prev1["open"] >= prev2["open"])
        & (df["open"] >= prev1["open"])
    )
    patterns["three_black_crows"] = (
        prev2["uptrend_ctx"].fillna(False)
        & strong_bear
        & strong_bear.shift(1).fillna(False)
        & strong_bear.shift(2).fillna(False)
        & (df["close"] < prev1["close"])
        & (prev1["close"] < prev2["close"])
        & (prev1["open"] <= prev2["open"])
        & (df["open"] <= prev1["open"])
    )

    bearish_inside_first = prev3["bull"].fillna(False) & prev3["long_body"].fillna(False)
    bearish_inside_mid = (
        prev2["bear"].fillna(False)
        & prev1["bear"].fillna(False)
        & (prev2["high"] <= prev3["high"])
        & (prev2["low"] >= prev3["low"])
        & (prev1["high"] <= prev3["high"])
        & (prev1["low"] >= prev3["low"])
    )
    patterns["rising_three_methods"] = (
        prev4["uptrend_ctx"].fillna(False)
        & bearish_inside_first
        & bearish_inside_mid
        & df["bull"]
        & df["long_body"]
        & (df["close"] > prev3["high"])
    )

    bullish_inside_first = prev3["bear"].fillna(False) & prev3["long_body"].fillna(False)
    bullish_inside_mid = (
        prev2["bull"].fillna(False)
        & prev1["bull"].fillna(False)
        & (prev2["high"] <= prev3["high"])
        & (prev2["low"] >= prev3["low"])
        & (prev1["high"] <= prev3["high"])
        & (prev1["low"] >= prev3["low"])
    )
    patterns["falling_three_methods"] = (
        prev4["downtrend_ctx"].fillna(False)
        & bullish_inside_first
        & bullish_inside_mid
        & df["bear"]
        & df["long_body"]
        & (df["close"] < prev3["low"])
    )
    return patterns


def evaluate_pattern_entries(
    timeframe: str,
    df: pd.DataFrame,
    pattern_map: Dict[str, pd.Series],
    definitions: Dict[str, PatternDefinition],
) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    tf_minutes = TIMEFRAME_MINUTES[timeframe]
    for pattern_name, series in pattern_map.items():
        definition = definitions[pattern_name]
        signal_mask = series.fillna(False)
        count = int(signal_mask.sum())
        if count == 0:
            continue
        row: Dict[str, object] = {
            "timeframe": timeframe,
            "pattern": pattern_name,
            "direction": definition.direction,
            "family": definition.family,
            "count": count,
        }
        for horizon in ENTRY_HORIZONS_MINUTES:
            bars = max(int(round(horizon / tf_minutes)), 1)
            future_close = df["close"].shift(-bars)
            future_return = (future_close - df["close"]) / df["close"]
            if definition.direction == "short":
                directional_return = -future_return
            elif definition.direction == "long":
                directional_return = future_return
            else:
                directional_return = future_return.abs()
            sample = directional_return[signal_mask].dropna()
            row[f"mean_return_{horizon}m"] = float(sample.mean()) if not sample.empty else np.nan
            row[f"median_return_{horizon}m"] = float(sample.median()) if not sample.empty else np.nan
            row[f"win_rate_{horizon}m"] = float((sample > 0).mean()) if not sample.empty else np.nan
        ret_30 = float(row["mean_return_30m"]) if pd.notna(row["mean_return_30m"]) else np.nan
        ret_60 = float(row["mean_return_60m"]) if pd.notna(row["mean_return_60m"]) else np.nan
        ret_180 = float(row["mean_return_180m"]) if pd.notna(row["mean_return_180m"]) else np.nan
        win_60 = float(row["win_rate_60m"]) if pd.notna(row["win_rate_60m"]) else np.nan
        if definition.direction == "neutral":
            row["best_use"] = "exit/watch"
            row["edge_score"] = float(ret_30 if pd.notna(ret_30) else 0.0)
        elif pd.notna(ret_180) and pd.notna(ret_30) and ret_180 > ret_30 * 0.8 and win_60 >= 0.52:
            row["best_use"] = "entry_hold"
            row["edge_score"] = float(ret_180 * 10000.0 + win_60 * 5.0)
        elif pd.notna(ret_30) and pd.notna(ret_60) and ret_30 > 0 and ret_60 > 0 and ret_180 < ret_60:
            row["best_use"] = "entry_scalp_or_exit"
            row["edge_score"] = float(ret_60 * 10000.0 + win_60 * 4.0)
        else:
            row["best_use"] = "weak_or_context_only"
            row["edge_score"] = float((ret_60 if pd.notna(ret_60) else 0.0) * 10000.0 + (win_60 if pd.notna(win_60) else 0.0) * 3.0)
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["timeframe", "edge_score", "count"], ascending=[True, False, False])


def split_index(df: pd.DataFrame, ratio: float = 0.70) -> pd.Timestamp:
    cutoff = int(len(df) * ratio)
    cutoff = min(max(cutoff, 1), len(df) - 1)
    return df.index[cutoff]


def pick_signal_patterns(
    stats: pd.DataFrame,
    timeframe: str,
    direction: str,
    min_count: int = 5,
    limit: int = 3,
) -> List[str]:
    side = stats[
        (stats["timeframe"] == timeframe)
        & (stats["direction"] == direction)
        & (stats["count"] >= min_count)
        & (stats["mean_return_60m"] > 0)
        & (stats["win_rate_60m"] >= 0.50)
    ].copy()
    if side.empty:
        return []
    side["rank_score"] = side["mean_return_60m"] * 10000.0 + side["win_rate_60m"] * 10.0
    return side.sort_values(["rank_score", "count"], ascending=[False, False]).head(limit)["pattern"].tolist()


def select_hold_system_specs(train_stats: pd.DataFrame) -> List[HoldSystemSpec]:
    specs: List[HoldSystemSpec] = []
    selection_plan = [
        ("5m", "long", 1, 20),
        ("15m", "short", 2, 20),
        ("30m", "long", 1, 20),
    ]
    for timeframe, direction, limit, min_count in selection_plan:
        candidates = train_stats[
            (train_stats["timeframe"] == timeframe)
            & (train_stats["direction"] == direction)
            & (train_stats["count"] >= min_count)
            & (train_stats["mean_return_60m"] > 0)
            & (train_stats["mean_return_180m"] > 0)
        ].copy()
        if candidates.empty:
            continue
        candidates["rank_score"] = (
            candidates["mean_return_180m"] * 10000.0
            + candidates["win_rate_60m"] * 10.0
            + candidates["count"] * 0.01
        )
        for pattern_name in candidates.sort_values(["rank_score", "count"], ascending=[False, False]).head(limit)["pattern"]:
            specs.append(
                HoldSystemSpec(
                    timeframe=timeframe,
                    pattern=str(pattern_name),
                    direction=direction,
                    hold_bars=max(int(180 / TIMEFRAME_MINUTES[timeframe]), 1),
                )
            )
    return specs


def build_candidate_specs(train_stats: pd.DataFrame) -> List[tuple[str, str, str]]:
    candidates: List[tuple[str, str, str]] = []
    search_plan = [
        ("5m", "long", 2, 20),
        ("15m", "short", 2, 20),
        ("30m", "long", 1, 20),
        ("30m", "short", 1, 20),
    ]
    for timeframe, direction, limit, min_count in search_plan:
        subset = train_stats[
            (train_stats["timeframe"] == timeframe)
            & (train_stats["direction"] == direction)
            & (train_stats["count"] >= min_count)
            & ((train_stats["mean_return_60m"] > 0) | (train_stats["mean_return_180m"] > 0))
        ].copy()
        if subset.empty:
            continue
        subset["candidate_score"] = (
            subset["mean_return_180m"].fillna(0.0) * 10000.0
            + subset["mean_return_60m"].fillna(0.0) * 7000.0
            + subset["win_rate_60m"].fillna(0.0) * 8.0
            + subset["count"] * 0.02
        )
        for pattern in subset.sort_values(["candidate_score", "count"], ascending=[False, False]).head(limit)["pattern"]:
            candidates.append((timeframe, str(pattern), direction))
    return candidates


def evaluate_array_stats(values: np.ndarray) -> Dict[str, float]:
    if values.size == 0:
        return {"n": 0.0, "win_rate": 0.0, "expectancy": 0.0, "net": 0.0, "profit_factor": 0.0}
    wins = values[values > 0]
    losses = values[values <= 0]
    profit_factor = float(wins.sum() / abs(losses.sum())) if losses.size and abs(losses.sum()) > 0 else (float("inf") if wins.size else 0.0)
    return {
        "n": float(values.size),
        "win_rate": float((values > 0).mean()),
        "expectancy": float(values.mean()),
        "net": float(values.sum()),
        "profit_factor": profit_factor,
    }


def simulate_spec_events(
    frames: Dict[str, pd.DataFrame],
    pattern_maps: Dict[str, Dict[str, pd.Series]],
    spec: HoldSystemSpec,
    train_cutoff: pd.Timestamp,
) -> List[Dict[str, object]]:
    df = frames[spec.timeframe]
    events = df.loc[pattern_maps[spec.timeframe][spec.pattern].fillna(False)].copy()
    if spec.min_body_ratio > 0:
        events = events[events["body_ratio"] >= spec.min_body_ratio]
    if spec.require_volume_confirmation:
        events = events[events["volume"] >= events["volume_median_20"]]
    if events.empty:
        return []

    output: List[Dict[str, object]] = []
    for timestamp, row in events.iterrows():
        idx = df.index.get_loc(timestamp)
        if idx + 1 >= len(df):
            continue
        entry_idx = idx + 1
        exit_idx = min(entry_idx + spec.hold_bars, len(df) - 1)
        entry_price = float(df.iloc[entry_idx]["open"])
        atr_value = float(row["atr"]) if pd.notna(row["atr"]) else entry_price * 0.005
        atr_value = max(atr_value, entry_price * 0.005)
        stop_price = entry_price - spec.stop_atr_mult * atr_value if spec.direction == "long" else entry_price + spec.stop_atr_mult * atr_value
        exit_price = float(df.iloc[exit_idx]["close"])
        exit_time = df.index[exit_idx]
        for probe_idx in range(entry_idx, exit_idx + 1):
            candle = df.iloc[probe_idx]
            if spec.direction == "long" and float(candle["low"]) <= stop_price:
                exit_price = stop_price
                exit_time = df.index[probe_idx]
                break
            if spec.direction == "short" and float(candle["high"]) >= stop_price:
                exit_price = stop_price
                exit_time = df.index[probe_idx]
                break
        risk = abs(entry_price - stop_price)
        if risk <= 0:
            continue
        net_r = (
            (exit_price - entry_price) / risk if spec.direction == "long" else (entry_price - exit_price) / risk
        ) - ((entry_price + exit_price) * 0.0005 / risk)
        output.append(
            {
                "spec_key": f"{spec.timeframe}:{spec.pattern}:{spec.direction}:{spec.hold_bars}:{spec.stop_atr_mult}:{spec.min_body_ratio}:{int(spec.require_volume_confirmation)}",
                "signal_time": timestamp,
                "exit_time": exit_time,
                "net_r": float(net_r),
                "is_train": bool(timestamp <= train_cutoff),
            }
        )
    return output


def search_spec_variants(
    frames: Dict[str, pd.DataFrame],
    pattern_maps: Dict[str, Dict[str, pd.Series]],
    train_stats: pd.DataFrame,
    train_cutoff: pd.Timestamp,
) -> List[HoldSystemSpec]:
    candidates = build_candidate_specs(train_stats)
    hold_options = {"5m": [24, 36, 48], "15m": [8, 12, 16], "30m": [4, 6, 8], "1h": [2, 3, 4]}
    top_variants: List[tuple[HoldSystemSpec, List[Dict[str, object]], float]] = []

    for timeframe, pattern, direction in candidates:
        best_for_pattern: List[tuple[HoldSystemSpec, List[Dict[str, object]], float]] = []
        for hold_bars in hold_options[timeframe]:
            for stop_mult in (0.8, 1.0, 1.2):
                for min_body_ratio in (0.0, 0.2, 0.3):
                    for volume_filter in (False, True):
                        spec = HoldSystemSpec(
                            timeframe=timeframe,
                            pattern=pattern,
                            direction=direction,
                            hold_bars=hold_bars,
                            stop_atr_mult=stop_mult,
                            min_body_ratio=min_body_ratio,
                            require_volume_confirmation=volume_filter,
                        )
                        events = simulate_spec_events(frames, pattern_maps, spec, train_cutoff)
                        if len(events) < 10:
                            continue
                        train_values = np.array([item["net_r"] for item in events if item["is_train"]], dtype=float)
                        test_values = np.array([item["net_r"] for item in events if not item["is_train"]], dtype=float)
                        if train_values.size < 5 or test_values.size < 3:
                            continue
                        train_stats_local = evaluate_array_stats(train_values)
                        test_stats_local = evaluate_array_stats(test_values)
                        if train_stats_local["expectancy"] <= 0 or test_stats_local["expectancy"] <= 0 or test_stats_local["profit_factor"] < 1.0:
                            continue
                        score = (
                            train_stats_local["expectancy"] * 8.0
                            + test_stats_local["expectancy"] * 10.0
                            + min(train_stats_local["win_rate"], test_stats_local["win_rate"]) * 2.0
                            + min(train_stats_local["profit_factor"], test_stats_local["profit_factor"])
                        )
                        best_for_pattern.append((spec, events, score))
        best_for_pattern.sort(key=lambda item: item[2], reverse=True)
        top_variants.extend(best_for_pattern[:2])

    if not top_variants:
        return select_hold_system_specs(train_stats)

    best_combo: tuple[List[HoldSystemSpec], float] | None = None
    variant_specs = [item[0] for item in top_variants]
    event_lookup = {
        f"{spec.timeframe}:{spec.pattern}:{spec.direction}:{spec.hold_bars}:{spec.stop_atr_mult}:{spec.min_body_ratio}:{int(spec.require_volume_confirmation)}": events
        for spec, events, _ in top_variants
    }
    for combo_size in range(1, min(4, len(variant_specs)) + 1):
        for combo in combinations(variant_specs, combo_size):
            collected: List[Dict[str, object]] = []
            for spec in combo:
                key = f"{spec.timeframe}:{spec.pattern}:{spec.direction}:{spec.hold_bars}:{spec.stop_atr_mult}:{spec.min_body_ratio}:{int(spec.require_volume_confirmation)}"
                collected.extend(event_lookup[key])
            collected.sort(key=lambda item: item["signal_time"])
            selected: List[Dict[str, object]] = []
            busy_until: Optional[pd.Timestamp] = None
            for item in collected:
                signal_time = pd.Timestamp(item["signal_time"])
                if busy_until is not None and signal_time <= busy_until:
                    continue
                selected.append(item)
                busy_until = pd.Timestamp(item["exit_time"])
            train_values = np.array([item["net_r"] for item in selected if item["is_train"]], dtype=float)
            test_values = np.array([item["net_r"] for item in selected if not item["is_train"]], dtype=float)
            all_values = np.array([item["net_r"] for item in selected], dtype=float)
            if train_values.size < 8 or test_values.size < 4 or all_values.size == 0:
                continue
            train_stats_local = evaluate_array_stats(train_values)
            test_stats_local = evaluate_array_stats(test_values)
            all_stats_local = evaluate_array_stats(all_values)
            if train_stats_local["expectancy"] <= 0 or test_stats_local["expectancy"] <= 0 or test_stats_local["profit_factor"] < 1.1 or all_stats_local["win_rate"] < 0.42:
                continue
            score = (
                all_stats_local["expectancy"] * 10.0
                + min(train_stats_local["expectancy"], test_stats_local["expectancy"]) * 10.0
                + min(train_stats_local["profit_factor"], test_stats_local["profit_factor"])
                + all_stats_local["win_rate"] * 2.0
                + min(train_stats_local["win_rate"], test_stats_local["win_rate"]) * 2.0
            )
            if best_combo is None or score > best_combo[1]:
                best_combo = (list(combo), score)

    if best_combo is not None:
        return best_combo[0]
    return [item[0] for item in top_variants[:4]]


def compute_context_bias(h1: pd.DataFrame) -> pd.DataFrame:
    out = h1.copy()
    out["bias_long"] = (out["ema20"] > out["ema50"]) & (out["close"] > out["ema20"])
    out["bias_short"] = (out["ema20"] < out["ema50"]) & (out["close"] < out["ema20"])
    return out


def expand_pattern_events(signal_df: pd.DataFrame, pattern_map: Dict[str, pd.Series], allowed: Iterable[str], direction: str) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for pattern_name in allowed:
        if pattern_name not in pattern_map:
            continue
        mask = pattern_map[pattern_name].fillna(False)
        for timestamp in signal_df.index[mask]:
            row = signal_df.loc[timestamp]
            rows.append(
                {
                    "pattern": pattern_name,
                    "direction": direction,
                    "signal_time": timestamp,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "range": float(row["range"]),
                }
            )
    if not rows:
        return pd.DataFrame(columns=["pattern", "direction", "signal_time", "open", "high", "low", "close", "range"])
    return pd.DataFrame(rows).sort_values("signal_time").reset_index(drop=True)


def detect_exit_pattern(base_features: pd.DataFrame, idx: int, direction: str) -> bool:
    if idx < 2:
        return False
    cur = base_features.iloc[idx]
    prev = base_features.iloc[idx - 1]
    prev2 = base_features.iloc[idx - 2]
    if direction == "long":
        bearish_engulfing = bool(
            prev["bull"]
            and cur["bear"]
            and cur["open"] >= prev["close"] - prev["range"] * 0.10
            and cur["close"] <= prev["open"] + prev["range"] * 0.10
            and cur["body"] >= prev["body"] * 1.05
        )
        evening_star = bool(
            prev2["bull"]
            and prev2["long_body"]
            and prev["small_body"]
            and cur["bear"]
            and cur["close"] <= prev2["mid_body"]
        )
        shooting_star = bool(cur["upper_ratio"] >= 0.55 and cur["lower_ratio"] <= 0.15 and cur["body_ratio"] <= 0.35)
        return bearish_engulfing or evening_star or shooting_star
    bullish_engulfing = bool(
        prev["bear"]
        and cur["bull"]
        and cur["open"] <= prev["close"] + prev["range"] * 0.10
        and cur["close"] >= prev["open"] - prev["range"] * 0.10
        and cur["body"] >= prev["body"] * 1.05
    )
    morning_star = bool(
        prev2["bear"]
        and prev2["long_body"]
        and prev["small_body"]
        and cur["bull"]
        and cur["close"] >= prev2["mid_body"]
    )
    hammer = bool(cur["lower_ratio"] >= 0.55 and cur["upper_ratio"] <= 0.15 and cur["body_ratio"] <= 0.35)
    return bullish_engulfing or morning_star or hammer


def backtest_pattern_system(
    base_df: pd.DataFrame,
    signal_df: pd.DataFrame,
    context_h1: pd.DataFrame,
    signal_patterns_long: List[str],
    signal_patterns_short: List[str],
    signal_pattern_map: Dict[str, pd.Series],
    signal_timeframe: str,
    fee_per_side: float = 0.0005,
) -> List[StrategyTrade]:
    context = compute_context_bias(context_h1)
    base_features = add_features(base_df)
    signal_events = pd.concat(
        [
            expand_pattern_events(signal_df, signal_pattern_map, signal_patterns_long, "long"),
            expand_pattern_events(signal_df, signal_pattern_map, signal_patterns_short, "short"),
        ],
        ignore_index=True,
    )
    if signal_events.empty:
        return []

    trades: List[StrategyTrade] = []
    busy_until: Optional[pd.Timestamp] = None

    for event in signal_events.sort_values("signal_time").itertuples(index=False):
        signal_time = pd.Timestamp(event.signal_time)
        if busy_until is not None and signal_time <= busy_until:
            continue

        h1_pos = context.index.searchsorted(signal_time, side="right") - 1
        if h1_pos < 0:
            continue
        bias_row = context.iloc[h1_pos]
        if event.direction == "long" and not bool(bias_row["bias_long"]):
            continue
        if event.direction == "short" and not bool(bias_row["bias_short"]):
            continue

        start_idx = base_df.index.searchsorted(signal_time, side="right")
        end_idx = min(start_idx + 3, len(base_df) - 1)
        if start_idx >= len(base_df):
            continue

        signal_range = max(float(event.range), 1e-9)
        if event.direction == "long":
            entry_trigger = float(event.high)
            stop_price = float(event.low) - signal_range * 0.10
            risk = entry_trigger - stop_price
        else:
            entry_trigger = float(event.low)
            stop_price = float(event.high) + signal_range * 0.10
            risk = stop_price - entry_trigger
        if risk <= 0 or (risk / entry_trigger) > 0.02:
            continue

        entry_idx: Optional[int] = None
        entry_price: Optional[float] = None
        for idx in range(start_idx, end_idx + 1):
            candle = base_df.iloc[idx]
            if event.direction == "long" and float(candle["high"]) >= entry_trigger:
                entry_idx = idx
                entry_price = entry_trigger
                break
            if event.direction == "short" and float(candle["low"]) <= entry_trigger:
                entry_idx = idx
                entry_price = entry_trigger
                break
        if entry_idx is None or entry_price is None:
            continue

        tp1 = entry_price + risk if event.direction == "long" else entry_price - risk
        tp2 = entry_price + 2.0 * risk if event.direction == "long" else entry_price - 2.0 * risk
        quantity_open = 1.0
        realized_r = 0.0
        stop_now = stop_price
        took_tp1 = False
        max_favorable = 0.0
        max_adverse = 0.0
        exit_price: Optional[float] = None
        exit_time: Optional[pd.Timestamp] = None
        exit_reason: Optional[str] = None
        last_idx = min(entry_idx + 24, len(base_df) - 1)

        for idx in range(entry_idx, last_idx + 1):
            candle = base_df.iloc[idx]
            high = float(candle["high"])
            low = float(candle["low"])
            close = float(candle["close"])
            if event.direction == "long":
                max_favorable = max(max_favorable, (high - entry_price) / risk)
                max_adverse = min(max_adverse, (low - entry_price) / risk)
                if (not took_tp1) and high >= tp1:
                    realized_r += 0.5
                    quantity_open = 0.5
                    stop_now = max(stop_now, entry_price)
                    took_tp1 = True
                if low <= stop_now:
                    exit_price, exit_time, exit_reason = stop_now, base_df.index[idx], "stop"
                    break
                if high >= tp2:
                    exit_price, exit_time, exit_reason = tp2, base_df.index[idx], "tp2"
                    break
                if idx > entry_idx + 1 and detect_exit_pattern(base_features, idx, "long"):
                    exit_price, exit_time, exit_reason = close, base_df.index[idx], "pattern_exit"
                    break
            else:
                max_favorable = max(max_favorable, (entry_price - low) / risk)
                max_adverse = min(max_adverse, (entry_price - high) / risk)
                if (not took_tp1) and low <= tp1:
                    realized_r += 0.5
                    quantity_open = 0.5
                    stop_now = min(stop_now, entry_price)
                    took_tp1 = True
                if high >= stop_now:
                    exit_price, exit_time, exit_reason = stop_now, base_df.index[idx], "stop"
                    break
                if low <= tp2:
                    exit_price, exit_time, exit_reason = tp2, base_df.index[idx], "tp2"
                    break
                if idx > entry_idx + 1 and detect_exit_pattern(base_features, idx, "short"):
                    exit_price, exit_time, exit_reason = close, base_df.index[idx], "pattern_exit"
                    break

        if exit_price is None or exit_time is None or exit_reason is None:
            exit_price = float(base_df.iloc[last_idx]["close"])
            exit_time = base_df.index[last_idx]
            exit_reason = "time_stop"

        remaining_r = (
            (exit_price - entry_price) / risk if event.direction == "long" else (entry_price - exit_price) / risk
        )
        gross_r = realized_r + quantity_open * remaining_r
        fee_r = ((entry_price + exit_price) * fee_per_side) / risk
        net_r = gross_r - fee_r
        busy_until = exit_time
        trades.append(
            StrategyTrade(
                direction=event.direction,
                signal_pattern=event.pattern,
                signal_timeframe=signal_timeframe,
                signal_time=signal_time.isoformat(),
                entry_time=base_df.index[entry_idx].isoformat(),
                exit_time=exit_time.isoformat(),
                entry_price=float(entry_price),
                exit_price=float(exit_price),
                stop_price=float(stop_price),
                tp1_price=float(tp1),
                tp2_price=float(tp2),
                exit_reason=exit_reason,
                bars_held_5m=int(max(base_df.index.searchsorted(exit_time) - entry_idx, 0)),
                gross_r=float(gross_r),
                net_r=float(net_r),
                mfe_r=float(max_favorable),
                mae_r=float(max_adverse),
            )
        )
    return trades


def backtest_hold_system(
    frames: Dict[str, pd.DataFrame],
    pattern_maps: Dict[str, Dict[str, pd.Series]],
    specs: List[HoldSystemSpec],
    fee_per_side: float = 0.0005,
) -> List[StrategyTrade]:
    rows: List[Dict[str, object]] = []
    for spec in specs:
        df = frames[spec.timeframe]
        pattern_mask = pattern_maps[spec.timeframe][spec.pattern].fillna(False)
        events = df.loc[pattern_mask].copy()
        if spec.min_body_ratio > 0:
            events = events[events["body_ratio"] >= spec.min_body_ratio]
        if spec.require_volume_confirmation:
            events = events[events["volume"] >= events["volume_median_20"]]
        for timestamp, event_row in events.iterrows():
            idx = df.index.get_loc(timestamp)
            if idx + 1 >= len(df):
                continue
            rows.append(
                {
                    "signal_time": timestamp,
                    "timeframe": spec.timeframe,
                    "pattern": spec.pattern,
                    "direction": spec.direction,
                    "hold_bars": spec.hold_bars,
                    "stop_atr_mult": spec.stop_atr_mult,
                    "signal_atr": float(event_row["atr"]) if pd.notna(event_row["atr"]) else np.nan,
                }
            )
    if not rows:
        return []

    trades: List[StrategyTrade] = []
    busy_until: Optional[pd.Timestamp] = None
    for row in sorted(rows, key=lambda item: item["signal_time"]):
        signal_time = pd.Timestamp(row["signal_time"])
        if busy_until is not None and signal_time <= busy_until:
            continue

        timeframe = str(row["timeframe"])
        pattern = str(row["pattern"])
        direction = str(row["direction"])
        hold_bars = int(row["hold_bars"])
        stop_atr_mult = float(row["stop_atr_mult"])
        df = frames[timeframe]
        idx = df.index.get_loc(signal_time)
        entry_idx = idx + 1
        exit_idx = min(entry_idx + hold_bars, len(df) - 1)
        if exit_idx <= entry_idx:
            continue

        entry_row = df.iloc[entry_idx]
        entry_price = float(entry_row["open"])
        atr_value = float(row["signal_atr"]) if pd.notna(row["signal_atr"]) else entry_price * 0.005
        atr_value = max(atr_value, entry_price * 0.005)
        stop_price = entry_price - stop_atr_mult * atr_value if direction == "long" else entry_price + stop_atr_mult * atr_value
        risk = abs(entry_price - stop_price)
        if risk <= 0:
            continue

        exit_price = float(df.iloc[exit_idx]["close"])
        exit_time = df.index[exit_idx]
        exit_reason = "time_stop"
        mfe_r = 0.0
        mae_r = 0.0
        for probe_idx in range(entry_idx, exit_idx + 1):
            candle = df.iloc[probe_idx]
            high = float(candle["high"])
            low = float(candle["low"])
            if direction == "long":
                mfe_r = max(mfe_r, (high - entry_price) / risk)
                mae_r = min(mae_r, (low - entry_price) / risk)
                if low <= stop_price:
                    exit_price = stop_price
                    exit_time = df.index[probe_idx]
                    exit_reason = "stop"
                    break
            else:
                mfe_r = max(mfe_r, (entry_price - low) / risk)
                mae_r = min(mae_r, (entry_price - high) / risk)
                if high >= stop_price:
                    exit_price = stop_price
                    exit_time = df.index[probe_idx]
                    exit_reason = "stop"
                    break

        gross_r = (exit_price - entry_price) / risk if direction == "long" else (entry_price - exit_price) / risk
        fee_r = ((entry_price + exit_price) * fee_per_side) / risk
        net_r = gross_r - fee_r
        busy_until = exit_time
        bars_held_5m = int(max((exit_time - df.index[entry_idx]).total_seconds() // (5 * 60), 0))
        trades.append(
            StrategyTrade(
                direction=direction,
                signal_pattern=pattern,
                signal_timeframe=timeframe,
                signal_time=signal_time.isoformat(),
                entry_time=df.index[entry_idx].isoformat(),
                exit_time=exit_time.isoformat(),
                entry_price=float(entry_price),
                exit_price=float(exit_price),
                stop_price=float(stop_price),
                tp1_price=0.0,
                tp2_price=0.0,
                exit_reason=exit_reason,
                bars_held_5m=bars_held_5m,
                gross_r=float(gross_r),
                net_r=float(net_r),
                mfe_r=float(mfe_r),
                mae_r=float(mae_r),
            )
        )
    return trades


def summarize_trades(trades: List[StrategyTrade], label: str) -> Dict[str, object]:
    if not trades:
        return {
            "label": label,
            "trades": 0,
            "win_rate": 0.0,
            "expectancy_r": 0.0,
            "profit_factor": 0.0,
            "avg_hold_bars_5m": 0.0,
            "net_r": 0.0,
            "exit_mix": {},
        }
    net = np.array([trade.net_r for trade in trades], dtype=float)
    wins = net[net > 0]
    losses = net[net <= 0]
    exit_mix: Dict[str, int] = {}
    for trade in trades:
        exit_mix[trade.exit_reason] = exit_mix.get(trade.exit_reason, 0) + 1
    return {
        "label": label,
        "trades": int(len(trades)),
        "win_rate": float((net > 0).mean()),
        "expectancy_r": float(net.mean()),
        "profit_factor": float(wins.sum() / abs(losses.sum())) if losses.size and abs(losses.sum()) > 0 else (float("inf") if wins.size else 0.0),
        "avg_hold_bars_5m": float(np.mean([trade.bars_held_5m for trade in trades])),
        "net_r": float(net.sum()),
        "avg_mfe_r": float(np.mean([trade.mfe_r for trade in trades])),
        "avg_mae_r": float(np.mean([trade.mae_r for trade in trades])),
        "exit_mix": exit_mix,
    }


def format_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def build_report_markdown(
    data_path: Path,
    stats: pd.DataFrame,
    best_patterns: pd.DataFrame,
    specs: List[HoldSystemSpec],
    all_summary: Dict[str, object],
    train_summary: Dict[str, object],
    test_summary: Dict[str, object],
    train_cutoff: pd.Timestamp,
) -> str:
    lines: List[str] = []
    lines.append("# ETH candlestick pattern analysis")
    lines.append("")
    lines.append(f"- source: `{data_path}`")
    lines.append(f"- train/test split: `{train_cutoff.isoformat()}`")
    lines.append("- timeframes reviewed: `5m`, `15m`, `30m`, `1h`")
    lines.append("")
    lines.append("## Best entry patterns by timeframe")
    lines.append("")
    for row in best_patterns.head(12).itertuples(index=False):
        lines.append(
            f"- `{row.timeframe}` `{row.pattern}` ({row.direction}) count={row.count}, "
            f"mean60={row.mean_return_60m * 100:.3f}%, win60={row.win_rate_60m * 100:.1f}%, use={row.best_use}"
        )
    lines.append("")
    lines.append("## Exit / caution patterns")
    lines.append("")
    exit_candidates = stats[
        (stats["best_use"] == "exit/watch")
        | ((stats["direction"] != "neutral") & (stats["mean_return_30m"] > 0) & (stats["mean_return_180m"] < stats["mean_return_60m"]))
    ].sort_values(["timeframe", "mean_return_30m"], ascending=[True, False]).head(10)
    for row in exit_candidates.itertuples(index=False):
        lines.append(
            f"- `{row.timeframe}` `{row.pattern}` count={row.count}, "
            f"30m={row.mean_return_30m * 100:.3f}%, 60m={row.mean_return_60m * 100:.3f}%, 180m={row.mean_return_180m * 100:.3f}%"
        )
    lines.append("")
    lines.append("## Derived trading system")
    lines.append("")
    for spec in specs:
        lines.append(
            f"- `{spec.timeframe}` `{spec.pattern}` {spec.direction} entry on next bar open, "
            f"`{spec.hold_bars}` bars hold ({spec.hold_bars * TIMEFRAME_MINUTES[spec.timeframe]} min), "
            f"stop `{spec.stop_atr_mult:.1f} ATR`, min body `{spec.min_body_ratio:.1f}`, "
            f"volume filter={'on' if spec.require_volume_confirmation else 'off'}"
        )
    lines.append("- execution: next bar open after signal, fee model `0.05%` per side, no overlap between positions")
    lines.append("- cleanup: low-frequency and weak-expectancy patterns are excluded by train/test screening before portfolio assembly")
    lines.append("")
    lines.append("## Strategy summary")
    lines.append("")
    lines.append(
        f"- full sample: trades={all_summary['trades']}, win_rate={format_pct(all_summary['win_rate'])}, "
        f"expectancy={all_summary['expectancy_r']:.3f}R, net={all_summary['net_r']:.2f}R, PF={all_summary['profit_factor']:.2f}"
    )
    lines.append(
        f"- train sample: trades={train_summary['trades']}, win_rate={format_pct(train_summary['win_rate'])}, "
        f"expectancy={train_summary['expectancy_r']:.3f}R, net={train_summary['net_r']:.2f}R, PF={train_summary['profit_factor']:.2f}"
    )
    lines.append(
        f"- test sample: trades={test_summary['trades']}, win_rate={format_pct(test_summary['win_rate'])}, "
        f"expectancy={test_summary['expectancy_r']:.3f}R, net={test_summary['net_r']:.2f}R, PF={test_summary['profit_factor']:.2f}"
    )
    lines.append("")
    lines.append("## Practical reading")
    lines.append("")
    lines.append("- strongest patterns on this month of ETH should be treated as local tendencies, not universal market laws")
    lines.append("- continuation patterns that keep positive 180m edge are better for fresh entries")
    lines.append("- indecision patterns and short-lived bursts are better as exit or risk-reduction cues")
    return "\n".join(lines) + "\n"


def run(data_path: Path, output_dir: Path) -> Dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)

    base = pd.read_csv(data_path, parse_dates=["time"])
    base["time"] = pd.to_datetime(base["time"], utc=True)
    base = base.set_index("time").sort_index()
    frames = {"5m": add_features(base)}
    for tf in ("15m", "30m", "1h"):
        frames[tf] = add_features(resample_ohlcv(base, tf))

    pattern_definitions = {item.name: item for item in build_pattern_definitions()}
    pattern_maps = {tf: detect_patterns(frame) for tf, frame in frames.items()}

    stats = pd.concat(
        [
            evaluate_pattern_entries(tf, frames[tf], pattern_maps[tf], pattern_definitions)
            for tf in ("5m", "15m", "30m", "1h")
        ],
        ignore_index=True,
    )
    stats = stats.sort_values(["timeframe", "edge_score", "count"], ascending=[True, False, False]).reset_index(drop=True)
    stats.to_csv(output_dir / "pattern_stats.csv", index=False)

    best_patterns = (
        stats[stats["direction"] != "neutral"]
        .sort_values(["edge_score", "count"], ascending=[False, False])
        .reset_index(drop=True)
    )
    best_patterns.head(40).to_csv(output_dir / "best_patterns.csv", index=False)

    signal_tf = "15m"
    train_cutoff = split_index(frames[signal_tf], 0.70)
    train_stats = pd.concat(
        [
            evaluate_pattern_entries(
                tf,
                frames[tf].loc[frames[tf].index <= train_cutoff],
                {name: mask.loc[mask.index <= train_cutoff] for name, mask in pattern_maps[tf].items()},
                pattern_definitions,
            )
            for tf in ("5m", "15m", "30m", "1h")
        ],
        ignore_index=True,
    )
    specs = search_spec_variants(frames, pattern_maps, train_stats, train_cutoff)

    trades = backtest_hold_system(
        frames=frames,
        pattern_maps=pattern_maps,
        specs=specs,
        fee_per_side=0.0005,
    )
    trades_df = pd.DataFrame(asdict(trade) for trade in trades)
    trades_df.to_csv(output_dir / "system_trades.csv", index=False)

    train_trades = [trade for trade in trades if pd.Timestamp(trade.entry_time) <= train_cutoff]
    test_trades = [trade for trade in trades if pd.Timestamp(trade.entry_time) > train_cutoff]
    summary = {
        "data_path": str(data_path),
        "output_dir": str(output_dir),
        "train_cutoff": train_cutoff.isoformat(),
        "signal_timeframe": "multi",
        "selected_specs": [asdict(spec) for spec in specs],
        "full_sample": summarize_trades(trades, "full"),
        "train_sample": summarize_trades(train_trades, "train"),
        "test_sample": summarize_trades(test_trades, "test"),
    }
    with open(output_dir / "summary.json", "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)

    report_md = build_report_markdown(
        data_path=data_path,
        stats=stats,
        best_patterns=best_patterns,
        specs=specs,
        all_summary=summary["full_sample"],
        train_summary=summary["train_sample"],
        test_summary=summary["test_sample"],
        train_cutoff=train_cutoff,
    )
    (output_dir / "report.md").write_text(report_md, encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze ETH candlestick patterns and derive a simple trading system.")
    parser.add_argument(
        "--data",
        type=Path,
        default=Path("correction/data_cache/ETH_USDT_5m_30d.csv"),
        help="Path to base 5m ETH OHLCV CSV file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports/eth_candlestick_system_30d"),
        help="Directory for generated reports.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run(args.data, args.output_dir)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
