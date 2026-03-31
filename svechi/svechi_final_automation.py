from __future__ import annotations

import json
import os
import sys
import time
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, List
from urllib import error, parse, request

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from svechi.eth_candlestick_pattern_system import (  # noqa: E402
    HoldSystemSpec,
    add_features,
    backtest_hold_system,
    detect_patterns,
    resample_ohlcv,
    summarize_trades,
)


SVECHI_DIR = Path(__file__).resolve().parent
FINAL_DIR = Path(os.getenv("SVECHI_RUNTIME_ROOT", str(SVECHI_DIR / "final_runtime"))).resolve()
REPORTS_DIR = FINAL_DIR / "reports"
LOGS_DIR = FINAL_DIR / "logs"
LOCK_PATH = FINAL_DIR / "svechi_final.lock"
STATE_PATH = FINAL_DIR / "state.json"
SVECHI_ENV_PATH = Path(os.getenv("SVECHI_ENV_PATH", str(SVECHI_DIR / "svechi.env"))).resolve()
ENV_CANDIDATES = [SVECHI_ENV_PATH]

FINAL_SPECS = [
    HoldSystemSpec(timeframe="15m", pattern="shooting_star", direction="short", hold_bars=12, stop_atr_mult=0.8, min_body_ratio=0.2, require_volume_confirmation=False),
    HoldSystemSpec(timeframe="20m", pattern="morning_star", direction="long", hold_bars=12, stop_atr_mult=0.8, min_body_ratio=0.0, require_volume_confirmation=False),
    HoldSystemSpec(timeframe="30m", pattern="bearish_engulfing", direction="short", hold_bars=8, stop_atr_mult=1.2, min_body_ratio=0.2, require_volume_confirmation=True),
    HoldSystemSpec(timeframe="10m", pattern="hanging_man", direction="short", hold_bars=12, stop_atr_mult=0.8, min_body_ratio=0.0, require_volume_confirmation=True),
]


def ensure_dirs() -> None:
    for path in (FINAL_DIR, REPORTS_DIR, LOGS_DIR):
        path.mkdir(parents=True, exist_ok=True)


@contextmanager
def acquire_lock() -> Iterator[None]:
    ensure_dirs()
    fd = None
    acquired = False
    try:
        try:
            fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            existing_pid = _read_lock_pid()
            if _pid_is_running(existing_pid):
                raise RuntimeError("Svechi final system is already running in another terminal.") from exc
            LOCK_PATH.unlink(missing_ok=True)
            fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        acquired = True
        yield
    finally:
        if fd is not None:
            os.close(fd)
        if acquired and LOCK_PATH.exists():
            LOCK_PATH.unlink(missing_ok=True)


def _read_lock_pid() -> int | None:
    if not LOCK_PATH.exists():
        return None
    try:
        return int(LOCK_PATH.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def _pid_is_running(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def load_env_candidates() -> None:
    for env_path in ENV_CANDIDATES:
        if not env_path.exists():
            continue
        for line in env_path.read_text(encoding="utf-8").splitlines():
            raw = line.strip()
            if not raw or raw.startswith("#") or "=" not in raw:
                continue
            key, value = raw.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def parse_bool(value: str, default: bool = False) -> bool:
    raw = str(value).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


if parse_bool(os.getenv("SVECHI_ALLOW_FALLBACK_ENV", "false"), default=False):
    ENV_CANDIDATES.extend(
        [
            ROOT / "correction" / ".env",
            ROOT / "telegram_content_mvp" / ".env",
        ]
    )


def runtime_config_snapshot() -> Dict[str, object]:
    load_env_candidates()
    return {
        "config_path": str(SVECHI_ENV_PATH),
        "bot_tag": os.getenv("BOT_TAG", "svechi"),
        "paper": parse_bool(os.getenv("PAPER", "true"), default=True),
        "symbol": os.getenv("SYMBOL", "ETH-USDT").strip() or "ETH-USDT",
        "initial_capital": float(os.getenv("INITIAL_CAPITAL", "10000") or 10000),
        "risk_percent": float(os.getenv("RISK_PERCENT", "1") or 1),
        "qty_precision": int(os.getenv("QTY_PRECISION", "6") or 6),
        "price_precision": int(os.getenv("PRICE_PRECISION", "2") or 2),
        "recv_window": int(os.getenv("RECV_WINDOW", "5000") or 5000),
        "send_telegram": parse_bool(os.getenv("SEND_TELEGRAM", "true"), default=True),
    }


def telegram_credentials() -> tuple[str, str]:
    load_env_candidates()
    return os.getenv("TELEGRAM_BOT_TOKEN", "").strip(), os.getenv("TELEGRAM_CHAT_ID", "").strip()


def send_telegram_message(text: str) -> Dict[str, object]:
    if not runtime_config_snapshot()["send_telegram"]:
        return {"ok": False, "detail": "Telegram sending is disabled in svechi.env."}
    token, chat_id = telegram_credentials()
    if not token or not chat_id:
        return {"ok": False, "detail": "Telegram credentials are missing."}
    payload = parse.urlencode({"chat_id": chat_id, "text": text, "disable_web_page_preview": "true"}).encode("utf-8")
    req = request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=payload, method="POST")
    try:
        with request.urlopen(req, timeout=20) as response:
            body = response.read().decode("utf-8", errors="replace")
        return {"ok": bool(json.loads(body).get("ok")), "detail": body}
    except error.URLError as exc:
        return {"ok": False, "detail": str(exc)}


def print_status(title: str, details: Dict[str, object]) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fields: List[str] = []
    for key, value in details.items():
        if value in (None, "", [], {}):
            continue
        fields.append(f"{key}={value}")
    suffix = " | " + ", ".join(fields) if fields else ""
    print(f"[{stamp}] {title}{suffix}", flush=True)


def save_state(payload: Dict[str, object]) -> None:
    ensure_dirs()
    STATE_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def current_datasets() -> Dict[str, Path]:
    jan_src = ROOT / "correction" / "data_cache_dec_jan_trend" / "ETH_USDT_5m_61d_20260131T235959Z.csv"
    jan_slice = SVECHI_DIR / "month_datasets" / "ETH_USDT_5m_2026-01_slice.csv"
    if not jan_slice.exists() and jan_src.exists():
        df = pd.read_csv(jan_src, parse_dates=["time"])
        df["time"] = pd.to_datetime(df["time"], utc=True)
        df = df[(df["time"] >= "2026-01-01T00:00:00Z") & (df["time"] < "2026-02-01T00:00:00Z")]
        jan_slice.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(jan_slice, index=False)

    latest_60d = max(
        (ROOT / "correction").rglob("ETH_USDT_5m_60d*.csv"),
        key=lambda path: path.stat().st_mtime,
    )
    return {
        "2025-12": SVECHI_DIR / "month_datasets" / "ETH_USDT_5m_2025-12.csv",
        "2026-01": jan_slice,
        "2026-02_30d": ROOT / "correction" / "data_cache" / "ETH_USDT_5m_30d.csv",
        "2026-03": SVECHI_DIR / "month_datasets" / "ETH_USDT_5m_2026-03.csv",
        "latest_60d": latest_60d,
    }


def load_frames(data_path: Path) -> Dict[str, pd.DataFrame]:
    base = pd.read_csv(data_path, parse_dates=["time"])
    base["time"] = pd.to_datetime(base["time"], utc=True)
    base = base.set_index("time").sort_index()
    frames = {"5m": add_features(base)}
    for timeframe in ("10m", "15m", "20m", "30m", "45m", "1h"):
        frames[timeframe] = add_features(resample_ohlcv(base, timeframe))
    return frames


def latest_signal_from_specs(data_path: Path, specs: List[HoldSystemSpec]) -> Dict[str, object]:
    frames = load_frames(data_path)
    pattern_maps = {tf: detect_patterns(df) for tf, df in frames.items()}
    signals: List[Dict[str, object]] = []
    for spec in specs:
        frame = frames[spec.timeframe]
        pattern_series = pattern_maps[spec.timeframe][spec.pattern].fillna(False)
        if len(frame) == 0 or not bool(pattern_series.iloc[-1]):
            continue
        latest = frame.iloc[-1]
        if spec.min_body_ratio > 0 and float(latest["body_ratio"]) < spec.min_body_ratio:
            continue
        if spec.require_volume_confirmation:
            median_volume = latest.get("volume_median_20")
            if pd.isna(median_volume) or float(latest["volume"]) < float(median_volume):
                continue
        price = float(latest["close"])
        atr_value = float(latest["atr"]) if pd.notna(latest["atr"]) else price * 0.005
        atr_value = max(atr_value, price * 0.005)
        stop = price - spec.stop_atr_mult * atr_value if spec.direction == "long" else price + spec.stop_atr_mult * atr_value
        signals.append(
            {
                "timeframe": spec.timeframe,
                "pattern": spec.pattern,
                "direction": spec.direction,
                "signal_time": frame.index[-1].isoformat(),
                "reference_price": price,
                "stop_price": float(stop),
                "hold_minutes": spec.hold_bars * {"10m": 10, "15m": 15, "20m": 20, "30m": 30, "45m": 45}[spec.timeframe],
            }
        )
    return {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "data_path": str(data_path),
        "signal_count": len(signals),
        "signals": signals,
    }


def run_dataset(label: str, data_path: Path) -> Dict[str, object]:
    frames = load_frames(data_path)
    pattern_maps = {tf: detect_patterns(df) for tf, df in frames.items()}
    trades = backtest_hold_system(frames, pattern_maps, FINAL_SPECS, fee_per_side=0.0005)
    dataset_dir = REPORTS_DIR / label
    dataset_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(asdict(trade) for trade in trades).to_csv(dataset_dir / "system_trades.csv", index=False)
    summary = {
        "label": label,
        "data_path": str(data_path),
        "selected_specs": [asdict(spec) for spec in FINAL_SPECS],
        "full_sample": summarize_trades(trades, label),
    }
    (dataset_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def build_telegram_text(results: Dict[str, object], latest_signal: Dict[str, object]) -> str:
    cfg = runtime_config_snapshot()
    lines = [
        f"Svechi final system [{cfg['bot_tag']}]",
        f"mode={'paper' if cfg['paper'] else 'live'} | symbol={cfg['symbol']} | risk={cfg['risk_percent']}%",
    ]
    for label, payload in results.items():
        sample = payload["full_sample"]
        lines.append(
            f"{label}: trades={sample['trades']}, WR={sample['win_rate'] * 100:.1f}%, Exp={sample['expectancy_r']:.3f}R, PF={sample['profit_factor']:.2f}, Net={sample['net_r']:.2f}R"
        )
    if latest_signal["signal_count"] > 0:
        lines.append("Active signals:")
        for item in latest_signal["signals"][:10]:
            lines.append(
                f"- {item['timeframe']} {item['pattern']} {item['direction']} | ref {item['reference_price']:.2f} | stop {item['stop_price']:.2f} | hold {item['hold_minutes']}m"
            )
    else:
        lines.append("Active signals: none")
    return "\n".join(lines)


def run_once(config: Dict[str, object]) -> Dict[str, object]:
    datasets = current_datasets()
    results: Dict[str, object] = {}
    for label, path in datasets.items():
        if path.exists():
            results[label] = run_dataset(label, path)
    latest_signal = latest_signal_from_specs(datasets["latest_60d"], FINAL_SPECS)
    (REPORTS_DIR / "latest_signal.json").write_text(json.dumps(latest_signal, indent=2), encoding="utf-8")
    telegram_text = build_telegram_text(results, latest_signal)
    consolidated = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "config": config,
        "selected_specs": [asdict(spec) for spec in FINAL_SPECS],
        "results": results,
        "latest_signal": latest_signal,
        "telegram": {"ok": False, "detail": "Heartbeat handles Telegram delivery."},
    }
    (REPORTS_DIR / "final_summary.json").write_text(json.dumps(consolidated, indent=2), encoding="utf-8")
    (REPORTS_DIR / "telegram_message.txt").write_text(telegram_text, encoding="utf-8")
    return consolidated


def main() -> None:
    with acquire_lock():
        config = runtime_config_snapshot()
        poll_seconds = int(os.getenv("POLL_SECONDS", "300") or 300)
        heartbeat_minutes = int(os.getenv("HEARTBEAT_MINUTES", "60") or 60)
        last_heartbeat = 0.0
        loop_index = 0

        print_status(
            "system started",
            {
                "symbol": config["symbol"],
                "paper": config["paper"],
                "capital": config["initial_capital"],
                "risk_percent": config["risk_percent"],
                "heartbeat_minutes": heartbeat_minutes,
                "poll_seconds": poll_seconds,
            },
        )
        send_telegram_message(
            f"[{config['bot_tag']}] system started\n"
            f"symbol: {config['symbol']}\n"
            f"paper: {config['paper']}\n"
            f"capital: {config['initial_capital']}\n"
            f"risk_percent: {config['risk_percent']}\n"
            f"heartbeat_minutes: {heartbeat_minutes}"
        )

        while True:
            loop_index += 1
            loop_started = time.time()
            try:
                consolidated = run_once(config)
                signal_count = int(consolidated["latest_signal"]["signal_count"])
                print_status("scan complete", {"loop": loop_index, "signal_count": signal_count, "status": "ok"})
                save_state(
                    {
                        "last_loop": loop_index,
                        "last_run_at": pd.Timestamp.now(tz="UTC").isoformat(),
                        "signal_count": signal_count,
                        "paper": config["paper"],
                        "capital": config["initial_capital"],
                        "risk_percent": config["risk_percent"],
                    }
                )
            except Exception as exc:
                print_status("error", {"loop": loop_index, "error": str(exc)})
                send_telegram_message(f"[{config['bot_tag']}] error\nloop: {loop_index}\nerror: {exc}")
                save_state(
                    {
                        "last_loop": loop_index,
                        "last_run_at": pd.Timestamp.now(tz="UTC").isoformat(),
                        "last_error": str(exc),
                        "paper": config["paper"],
                    }
                )

            now_ts = time.time()
            if now_ts - last_heartbeat >= heartbeat_minutes * 60:
                signal_count = 0
                latest_signal_path = REPORTS_DIR / "latest_signal.json"
                if latest_signal_path.exists():
                    latest_payload = json.loads(latest_signal_path.read_text(encoding="utf-8"))
                    signal_count = int(latest_payload.get("signal_count", 0))
                heartbeat_text = (
                    f"[{config['bot_tag']}] heartbeat\n"
                    f"status: running\n"
                    f"loop: {loop_index}\n"
                    f"symbol: {config['symbol']}\n"
                    f"paper: {config['paper']}\n"
                    f"capital: {config['initial_capital']}\n"
                    f"risk_percent: {config['risk_percent']}\n"
                    f"signal_count: {signal_count}"
                )
                print_status(
                    "heartbeat",
                    {
                        "loop": loop_index,
                        "status": "running",
                        "symbol": config["symbol"],
                        "paper": config["paper"],
                        "signal_count": signal_count,
                    },
                )
                send_telegram_message(heartbeat_text)
                last_heartbeat = now_ts

            elapsed = time.time() - loop_started
            time.sleep(max(poll_seconds - elapsed, 5))


if __name__ == "__main__":
    main()
