from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from correction import (
    active_variant_by_name,
    build_lab_config,
    default_routes,
    impulse_quality_score,
    lab_variant_by_name,
    module_confidence,
)
from correction_block import BASIC_CORRECTION_PROFILE
from correction_exchange import (
    BingXExchange,
    ExchangeConfig,
    OrderRequest,
    ProtectionRequest,
    parse_bool,
    require_live_confirmation,
)
from correction_hourly import ExhaustionConfig, HourlyExhaustionFibStrategy
from correction_lab import PredictiveReversalLab
from correction_liquidity_sweep import LiquiditySweepLab, SweepConfig, default_sweep_profile, default_variants as sweep_variants, variant_by_name
from correction_regime import BingXClient, DataCache, now_utc
from correction_trend import confidence_score, curated_rules, default_trend_profile, feature_snapshot, passes_rule, trend_profiles
from correction_trend_pullback import EthTrendPullbackLab, PullbackConfig, default_variants as trend_default_variants


BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "state"
REPORTS_DIR = BASE_DIR / "reports" / "live_scan"


@dataclass
class LivePlan:
    source: str
    strategy_name: str
    module_name: str
    symbol: str
    direction: str
    signal_time: str
    entry_price: float
    model_entry_price: float
    entry_zone_low: float
    entry_zone_high: float
    stop_price: float
    tp1_price: float
    tp2_price: float
    risk_points: float
    confidence: float
    priority: int
    note: str

    @property
    def side(self) -> str:
        return "BUY" if self.direction == "long" else "SELL"

    @property
    def signal_key(self) -> str:
        return f"{self.source}|{self.module_name}|{self.direction}|{self.signal_time}"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def recalculate_targets_from_model(direction: str, model_entry: float, model_stop: float, model_tp1: float, model_tp2: float, execution_entry: float) -> tuple[float, float]:
    model_risk = abs(model_entry - model_stop)
    execution_risk = abs(execution_entry - model_stop)
    if model_risk <= 0 or execution_risk <= 0:
        raise ValueError("risk must be positive")
    rr1 = abs(model_tp1 - model_entry) / model_risk
    rr2 = abs(model_tp2 - model_entry) / model_risk
    if direction == "long":
        return execution_entry + rr1 * execution_risk, execution_entry + rr2 * execution_risk
    return execution_entry - rr1 * execution_risk, execution_entry - rr2 * execution_risk


def recalculate_targets_from_rr(direction: str, entry_price: float, stop_price: float, tp1_r: float, tp2_r: float) -> tuple[float, float]:
    risk_points = abs(entry_price - stop_price)
    if risk_points <= 0:
        raise ValueError("risk must be positive")
    if direction == "long":
        return entry_price + tp1_r * risk_points, entry_price + tp2_r * risk_points
    return entry_price - tp1_r * risk_points, entry_price - tp2_r * risk_points


def compute_order_quantity(capital: float, risk_fraction: float, entry_price: float, stop_price: float, qty_precision: int) -> float:
    risk_points = abs(entry_price - stop_price)
    if capital <= 0 or risk_fraction <= 0 or risk_points <= 0:
        return 0.0
    quantity = (capital * risk_fraction) / risk_points
    return round(max(quantity, 0.0), qty_precision)


def signal_age_minutes(signal_time: str) -> float:
    signal_dt = pd.Timestamp(signal_time).to_pydatetime()
    return (utc_now() - signal_dt).total_seconds() / 60.0


def select_best_plan(plans: List[LivePlan], max_signal_age_minutes: int) -> Optional[LivePlan]:
    fresh = [plan for plan in plans if signal_age_minutes(plan.signal_time) <= max_signal_age_minutes]
    if not fresh:
        return None
    return sorted(
        fresh,
        key=lambda item: (
            pd.Timestamp(item.signal_time),
            item.priority,
            item.confidence,
        ),
        reverse=True,
    )[0]


def live_end_time(data_mode: str) -> Optional[datetime]:
    return now_utc() if data_mode == "live" else None


def build_correction_live_plans(
    symbol: str,
    days: int,
    initial_capital: float,
    risk_per_trade: float,
    cache: DataCache,
    client: BingXClient,
    lookback_bars: int,
    data_mode: str,
) -> List[LivePlan]:
    plans: List[LivePlan] = []
    end_time = live_end_time(data_mode)
    for route in default_routes(BASIC_CORRECTION_PROFILE):
        if route.kind == "active":
            config = ExhaustionConfig(days=days, end_time=end_time, initial_capital=initial_capital, risk_per_trade=risk_per_trade)
            variant = active_variant_by_name(route.name)
            strategy = HourlyExhaustionFibStrategy(config, variant, client, cache, REPORTS_DIR)
            frames = strategy.prepare_frames(symbol)
            latest_price = float(frames["5m"]["close"].iloc[-1])
            timestamps = list(frames[variant.confirm_interval].index[-lookback_bars:])
            for timestamp in reversed(timestamps):
                signal = strategy.build_signal(symbol, frames, timestamp)
                if signal is None:
                    continue
                if not (min(signal.entry_zone_low, signal.entry_zone_high) <= latest_price <= max(signal.entry_zone_low, signal.entry_zone_high)):
                    continue
                tp1_price, tp2_price = recalculate_targets_from_model(
                    signal.direction,
                    signal.planned_entry,
                    signal.stop_price,
                    signal.tp1_price,
                    signal.tp2_price,
                    latest_price,
                )
                plans.append(
                    LivePlan(
                        source="correction",
                        strategy_name="basic",
                        module_name=route.name,
                        symbol=symbol,
                        direction=signal.direction,
                        signal_time=signal.time.isoformat(),
                        entry_price=latest_price,
                        model_entry_price=float(signal.planned_entry),
                        entry_zone_low=float(signal.entry_zone_low),
                        entry_zone_high=float(signal.entry_zone_high),
                        stop_price=float(signal.stop_price),
                        tp1_price=float(tp1_price),
                        tp2_price=float(tp2_price),
                        risk_points=abs(latest_price - float(signal.stop_price)),
                        confidence=module_confidence(route),
                        priority=10 + route.priority,
                        note=signal.note,
                    )
                )
                break
        else:
            config = build_lab_config(days, route.config_preset, initial_capital, risk_per_trade, end_time)
            variant = lab_variant_by_name(route.name)
            strategy = PredictiveReversalLab(config, variant, client, cache, REPORTS_DIR)
            shallow_strategy: Optional[PredictiveReversalLab] = None
            if route.adaptive_shallow_threshold is not None and route.name == "adl15_noavwap":
                shallow_variant = variant.__class__(
                    name="adl15_shallow_noavwap",
                    confirm_interval="15m",
                    indicator_mode="adl",
                    require_avwap=False,
                    require_cmf_shift=False,
                    require_bb_reentry=False,
                    entry_fib=0.236,
                    tp1_r=0.62,
                    tp2_r=1.28,
                    min_rr=0.95,
                )
                shallow_strategy = PredictiveReversalLab(config, shallow_variant, client, cache, REPORTS_DIR)
            frames = strategy.prepare_frames(symbol)
            latest_price = float(frames["5m"]["close"].iloc[-1])
            timestamps = list(frames[variant.confirm_interval].index[-lookback_bars:])
            for timestamp in reversed(timestamps):
                signal = strategy.build_signal(symbol, frames, timestamp)
                if signal is None:
                    continue
                if shallow_strategy is not None:
                    quality_score = impulse_quality_score(signal, frames[variant.confirm_interval].loc[timestamp])
                    if quality_score >= route.adaptive_shallow_threshold:
                        shallow_signal = shallow_strategy.build_signal(symbol, frames, timestamp)
                        if shallow_signal is not None:
                            signal = shallow_signal
                if not (min(signal.entry_zone_low, signal.entry_zone_high) <= latest_price <= max(signal.entry_zone_low, signal.entry_zone_high)):
                    continue
                tp1_price, tp2_price = recalculate_targets_from_model(
                    signal.direction,
                    signal.planned_entry,
                    signal.stop_price,
                    signal.tp1_price,
                    signal.tp2_price,
                    latest_price,
                )
                plans.append(
                    LivePlan(
                        source="correction",
                        strategy_name="basic",
                        module_name=route.name,
                        symbol=symbol,
                        direction=signal.direction,
                        signal_time=signal.time.isoformat(),
                        entry_price=latest_price,
                        model_entry_price=float(signal.planned_entry),
                        entry_zone_low=float(signal.entry_zone_low),
                        entry_zone_high=float(signal.entry_zone_high),
                        stop_price=float(signal.stop_price),
                        tp1_price=float(tp1_price),
                        tp2_price=float(tp2_price),
                        risk_points=abs(latest_price - float(signal.stop_price)),
                        confidence=module_confidence(route),
                        priority=10 + route.priority,
                        note=signal.note,
                    )
                )
                break
    return plans


def build_trend_live_plans(
    profile: str,
    symbol: str,
    days: int,
    cache: DataCache,
    client: BingXClient,
    lookback_bars: int,
    entry_tolerance_r: float,
    data_mode: str,
) -> List[LivePlan]:
    plans: List[LivePlan] = []
    end_time = live_end_time(data_mode)
    rules_by_name = {rule.name: rule for rule in curated_rules()}
    variants_by_name = {variant.name: variant for variant in trend_default_variants()}
    labs_by_variant: Dict[str, tuple[EthTrendPullbackLab, Dict[str, pd.DataFrame]]] = {}

    for leg in trend_profiles()[profile]:
        rule = rules_by_name[leg.rule_name]
        if rule.base_variant_name not in labs_by_variant:
            config = PullbackConfig(days=days, end_time=end_time)
            variant = variants_by_name[rule.base_variant_name]
            lab = EthTrendPullbackLab(config, variant, client, cache, REPORTS_DIR)
            labs_by_variant[rule.base_variant_name] = (lab, lab.prepare_frames(symbol))
        lab, frames = labs_by_variant[rule.base_variant_name]
        variant = variants_by_name[rule.base_variant_name]
        latest_price = float(frames["5m"]["close"].iloc[-1])
        timestamps = list(frames["15m"].index[-lookback_bars:])
        for timestamp in reversed(timestamps):
            features = feature_snapshot(lab, frames, timestamp, variant)
            if features is None or not passes_rule(rule, features):
                continue
            signal = lab.build_signal(symbol, frames, timestamp)
            if signal is None:
                continue
            signal = replace(
                signal,
                tp1_r=leg.tp1_r if leg.tp1_r is not None else signal.tp1_r,
                tp2_r=leg.tp2_r if leg.tp2_r is not None else signal.tp2_r,
            )
            if leg.direction is not None and signal.direction != leg.direction:
                continue
            confidence = confidence_score(features)
            if confidence < leg.min_confidence:
                continue
            if leg.min_t1h is not None and float(features["t1h"]) < leg.min_t1h:
                continue
            if leg.max_stop_atr is not None and float(features["stop_atr"]) > leg.max_stop_atr:
                continue
            risk_points = abs(latest_price - float(signal.stop_price))
            if risk_points <= 0:
                continue
            if abs(latest_price - float(signal.planned_entry)) > entry_tolerance_r * risk_points:
                continue
            tp1_price, tp2_price = recalculate_targets_from_rr(signal.direction, latest_price, float(signal.stop_price), signal.tp1_r, signal.tp2_r)
            plans.append(
                LivePlan(
                    source="trend",
                    strategy_name=profile,
                    module_name=rule.name,
                    symbol=symbol,
                    direction=signal.direction,
                    signal_time=signal.time.isoformat(),
                    entry_price=latest_price,
                    model_entry_price=float(signal.planned_entry),
                    entry_zone_low=min(float(signal.planned_entry), latest_price),
                    entry_zone_high=max(float(signal.planned_entry), latest_price),
                    stop_price=float(signal.stop_price),
                    tp1_price=float(tp1_price),
                    tp2_price=float(tp2_price),
                    risk_points=risk_points,
                    confidence=confidence,
                    priority=rule.priority,
                    note=signal.note,
                )
            )
            break
    return plans


def build_sweep_live_plans(
    profile: str,
    symbol: str,
    days: int,
    cache: DataCache,
    client: BingXClient,
    lookback_bars: int,
    entry_tolerance_r: float,
    data_mode: str,
) -> List[LivePlan]:
    plans: List[LivePlan] = []
    end_time = live_end_time(data_mode)
    variant = variant_by_name(profile)
    config = SweepConfig(days=days, end_time=end_time)
    lab = LiquiditySweepLab(config, variant, client, cache, REPORTS_DIR)
    frames = lab.prepare_frames(symbol)
    signal_frame = frames[variant.signal_interval]
    latest_price = float(frames["5m"]["close"].iloc[-1])
    timestamps = list(signal_frame.index[-lookback_bars:])
    for timestamp in reversed(timestamps):
        signal = lab.build_signal(symbol, frames, timestamp)
        if signal is None:
            continue
        risk_points = abs(latest_price - float(signal.stop_price))
        if risk_points <= 0:
            continue
        if abs(latest_price - float(signal.planned_entry)) > entry_tolerance_r * risk_points:
            continue
        tp1_price, tp2_price = recalculate_targets_from_rr(
            signal.direction,
            latest_price,
            float(signal.stop_price),
            signal.tp1_r,
            signal.tp2_r,
        )
        plans.append(
            LivePlan(
                source="sweep",
                strategy_name=profile,
                module_name=signal.level_source,
                symbol=symbol,
                direction=signal.direction,
                signal_time=signal.time.isoformat(),
                entry_price=latest_price,
                model_entry_price=float(signal.planned_entry),
                entry_zone_low=min(float(signal.planned_entry), latest_price),
                entry_zone_high=max(float(signal.planned_entry), latest_price),
                stop_price=float(signal.stop_price),
                tp1_price=float(tp1_price),
                tp2_price=float(tp2_price),
                risk_points=risk_points,
                confidence=float(signal.score),
                priority=5,
                note=signal.note,
            )
        )
        break
    return plans


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live signal and execution runner for basic correction + locked trend.")
    parser.add_argument("--paper", choices=["true", "false"], default=None)
    parser.add_argument("--confirm-live", action="store_true")
    parser.add_argument("--data-mode", choices=["cache", "live"], default=os.getenv("DATA_MODE", "cache"))
    parser.add_argument("--days", type=int, default=int(os.getenv("LIVE_DAYS", "30")))
    parser.add_argument("--symbol", default=os.getenv("SYMBOL", "ETH-USDT"))
    parser.add_argument("--initial-capital", type=float, default=float(os.getenv("INITIAL_CAPITAL", "10000")))
    parser.add_argument("--risk-percent", type=float, default=float(os.getenv("RISK_PERCENT", "1")))
    parser.add_argument("--qty", type=float, default=None)
    parser.add_argument("--lookback-bars", type=int, default=int(os.getenv("LOOKBACK_BARS", "6")))
    parser.add_argument("--entry-tolerance-r", type=float, default=float(os.getenv("ENTRY_TOLERANCE_R", "0.25")))
    parser.add_argument("--max-signal-age-minutes", type=int, default=int(os.getenv("MAX_SIGNAL_AGE_MINUTES", "180")))
    parser.add_argument("--state-file", default=str(STATE_DIR / "live_state.json"))
    parser.add_argument("--output-file", default=str(REPORTS_DIR / "latest_signal.json"))
    parser.add_argument("--trend-profile", default=os.getenv("TREND_PROFILE", default_trend_profile()), choices=list(trend_profiles().keys()))
    parser.add_argument("--sweep-profile", default=os.getenv("SWEEP_PROFILE", "off"), choices=["off"] + [item.name for item in sweep_variants()])
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("scan")
    execute = subparsers.add_parser("execute")
    execute.add_argument("--force", action="store_true")
    return parser.parse_args()


def scan_plans(args: argparse.Namespace) -> Dict[str, Any]:
    cache = DataCache(BASE_DIR / "data_cache")
    client = BingXClient()
    risk_per_trade = args.risk_percent / 100.0
    correction_plans = build_correction_live_plans(
        args.symbol,
        args.days,
        args.initial_capital,
        risk_per_trade,
        cache,
        client,
        args.lookback_bars,
        args.data_mode,
    )
    trend_plans = build_trend_live_plans(
        args.trend_profile,
        args.symbol,
        args.days,
        cache,
        client,
        args.lookback_bars,
        args.entry_tolerance_r,
        args.data_mode,
    )
    sweep_plans: List[LivePlan] = []
    if args.sweep_profile != "off":
        sweep_plans = build_sweep_live_plans(
            args.sweep_profile,
            args.symbol,
            args.days,
            cache,
            client,
            args.lookback_bars,
            args.entry_tolerance_r,
            args.data_mode,
        )
    all_plans = correction_plans + trend_plans + sweep_plans
    selected = select_best_plan(all_plans, args.max_signal_age_minutes)
    payload = {
        "symbol": args.symbol,
        "data_mode": args.data_mode,
        "trend_profile": args.trend_profile,
        "sweep_profile": args.sweep_profile,
        "plans_found": len(all_plans),
        "selected_plan": asdict(selected) if selected is not None else None,
        "all_plans": [asdict(plan) for plan in sorted(all_plans, key=lambda item: (pd.Timestamp(item.signal_time), item.priority, item.confidence), reverse=True)],
    }
    save_json(Path(args.output_file), payload)
    return payload


def execute_plan(args: argparse.Namespace, payload: Dict[str, Any]) -> Dict[str, Any]:
    selected_payload = payload.get("selected_plan")
    if selected_payload is None:
        return {"ok": False, "msg": "No active plan found", "scan": payload}
    plan = LivePlan(**selected_payload)
    config = ExchangeConfig()
    if args.paper is not None:
        config.paper_trading = parse_bool(args.paper)
    exchange = BingXExchange(config)
    require_live_confirmation(exchange, args.confirm_live, "execute")

    # Do not interfere with any other live system already touching ETH-USDT.
    existing_positions = exchange.get_positions(plan.symbol)
    if existing_positions:
        return {
            "ok": False,
            "msg": "Existing live position detected on symbol, skipping to avoid strategy conflict",
            "symbol": plan.symbol,
            "positions": existing_positions,
        }
    existing_orders = exchange.get_open_orders(plan.symbol)
    if existing_orders:
        return {
            "ok": False,
            "msg": "Existing live open orders detected on symbol, skipping to avoid strategy conflict",
            "symbol": plan.symbol,
            "orders": existing_orders,
        }

    state_path = Path(args.state_file)
    state = load_json(state_path)
    if state.get("last_signal_key") == plan.signal_key and not args.force:
        return {
            "ok": False,
            "msg": "Signal already executed",
            "signal_key": plan.signal_key,
            "state_file": str(state_path.resolve()),
        }

    capital = args.initial_capital
    if not exchange.paper_trading:
        capital = max(exchange.get_balance(), args.initial_capital)
    quantity = args.qty
    if quantity is None:
        quantity = compute_order_quantity(capital, args.risk_percent / 100.0, plan.entry_price, plan.stop_price, exchange.config.qty_precision)
    if quantity is None or quantity <= 0:
        return {"ok": False, "msg": "Calculated quantity is zero", "plan": asdict(plan)}

    order_result = exchange.place_market_order(OrderRequest(symbol=plan.symbol, side=plan.side, quantity=quantity))
    protection_result = None
    if order_result.get("ok"):
        protection_result = exchange.set_protection_orders(
            ProtectionRequest(
                symbol=plan.symbol,
                direction=plan.direction,
                stop_price=plan.stop_price,
                take_profit_price=plan.tp1_price,
                quantity=quantity,
            )
        )
        save_json(
            state_path,
            {
                "last_signal_key": plan.signal_key,
                "last_executed_at": utc_now().isoformat(),
                "last_plan": asdict(plan),
                "last_order_result": order_result,
                "last_protection_result": protection_result,
            },
        )
    return {
        "ok": bool(order_result.get("ok")),
        "paper_trading": exchange.paper_trading,
        "quantity": quantity,
        "plan": asdict(plan),
        "order_result": order_result,
        "protection_result": protection_result,
        "state_file": str(state_path.resolve()),
    }


def main() -> None:
    args = parse_args()
    payload = scan_plans(args)
    if args.command == "scan":
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return
    execution_payload = execute_plan(args, payload)
    print(json.dumps(execution_payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
