from __future__ import annotations

import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List, Optional
from urllib import error, parse, request

import pandas as pd

from eth_candlestick_pattern_system import (
    HoldSystemSpec,
    add_features,
    detect_patterns,
    resample_ohlcv,
    run,
)


ROOT = Path(__file__).resolve().parent.parent
SVECHI_DIR = Path(__file__).resolve().parent
CORRECTION_DIR = ROOT / "correction"
OUTPUT_DIR = SVECHI_DIR / "automation_outputs"
ENV_CANDIDATES = [
    ROOT / "correction" / ".env",
    ROOT / "telegram_content_mvp" / ".env",
]

DATASET_PATTERNS = {
    "30d": ["ETH_USDT_5m_30d.csv"],
    "60d": ["ETH_USDT_5m_60d*.csv", "ETH_USDT_5m_61d*.csv"],
}


def find_latest_dataset(patterns: List[str]) -> Optional[Path]:
    candidates: List[Path] = []
    for pattern in patterns:
        candidates.extend(CORRECTION_DIR.rglob(pattern))
    files = [path for path in candidates if path.is_file()]
    if not files:
        return None
    return max(files, key=lambda path: path.stat().st_mtime)


def load_env_candidates() -> None:
    for env_path in ENV_CANDIDATES:
        if not env_path.exists():
            continue
        for line in env_path.read_text(encoding="utf-8").splitlines():
            raw = line.strip()
            if not raw or raw.startswith("#") or "=" not in raw:
                continue
            key, value = raw.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def telegram_credentials() -> tuple[str, str]:
    load_env_candidates()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    return token, chat_id


def send_telegram_message(text: str) -> Dict[str, object]:
    token, chat_id = telegram_credentials()
    if not token or not chat_id:
        return {"ok": False, "detail": "Telegram credentials are missing."}
    payload = parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    req = request.Request(url, data=payload, method="POST")
    try:
        with request.urlopen(req, timeout=20) as response:
            body = response.read().decode("utf-8", errors="replace")
        parsed = json.loads(body)
        return {"ok": bool(parsed.get("ok")), "detail": body}
    except error.URLError as exc:
        return {"ok": False, "detail": str(exc)}


def format_run_summary(label: str, run_payload: Dict[str, object]) -> str:
    if run_payload.get("status") != "ok":
        return f"{label}: dataset missing"
    summary = run_payload["summary"]
    full_sample = summary["full_sample"]
    latest_signal = run_payload["latest_signal"]
    specs = summary.get("selected_specs", [])
    spec_text = ", ".join(f"{item['timeframe']} {item['pattern']} {item['direction']}" for item in specs) or "no specs"
    signal_count = latest_signal.get("signal_count", 0)
    return (
        f"{label}: trades={full_sample['trades']}, WR={full_sample['win_rate'] * 100:.1f}%, "
        f"Exp={full_sample['expectancy_r']:.3f}R, PF={full_sample['profit_factor']:.2f}, "
        f"signals={signal_count}, specs={spec_text}"
    )


def format_signal_lines(run_payload: Dict[str, object]) -> List[str]:
    if run_payload.get("status") != "ok":
        return []
    latest_signal = run_payload.get("latest_signal", {})
    signal_rows = latest_signal.get("signals", [])
    lines: List[str] = []
    for item in signal_rows:
        lines.append(
            f"- {item['timeframe']} {item['pattern']} {item['direction']} | ref {item['reference_price']:.2f} | stop {item['stop_price']:.2f} | hold {item['hold_minutes']}m"
        )
    return lines


def build_telegram_text(consolidated: Dict[str, object]) -> str:
    runs = consolidated.get("runs", {})
    lines = ["Svechi automation"]
    for label in ("30d", "60d"):
        if label in runs:
            lines.append(format_run_summary(label, runs[label]))
    signal_lines: List[str] = []
    for label in ("30d", "60d"):
        if label in runs:
            for line in format_signal_lines(runs[label]):
                signal_lines.append(f"{label} {line}")
    if signal_lines:
        lines.append("Active signals:")
        lines.extend(signal_lines[:10])
    else:
        lines.append("Active signals: none")
    return "\n".join(lines)


def load_selected_specs(summary_path: Path) -> List[HoldSystemSpec]:
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    specs: List[HoldSystemSpec] = []
    for item in payload.get("selected_specs", []):
        specs.append(HoldSystemSpec(**item))
    return specs


def load_frames(data_path: Path) -> Dict[str, pd.DataFrame]:
    base = pd.read_csv(data_path, parse_dates=["time"])
    base["time"] = pd.to_datetime(base["time"], utc=True)
    base = base.set_index("time").sort_index()
    frames = {"5m": add_features(base)}
    for timeframe in ("15m", "30m", "1h"):
        frames[timeframe] = add_features(resample_ohlcv(base, timeframe))
    return frames


def spec_matches_latest_bar(frame: pd.DataFrame, pattern_name: str, spec: HoldSystemSpec) -> bool:
    patterns = detect_patterns(frame)
    if pattern_name not in patterns or len(frame.index) < 2:
        return False
    latest_idx = frame.index[-1]
    if not bool(patterns[pattern_name].fillna(False).iloc[-1]):
        return False
    latest = frame.iloc[-1]
    if spec.min_body_ratio > 0 and float(latest["body_ratio"]) < spec.min_body_ratio:
        return False
    if spec.require_volume_confirmation:
        median_volume = latest.get("volume_median_20")
        if pd.isna(median_volume) or float(latest["volume"]) < float(median_volume):
            return False
    return True


def build_latest_signal(data_path: Path, specs: List[HoldSystemSpec]) -> Dict[str, object]:
    frames = load_frames(data_path)
    signals: List[Dict[str, object]] = []
    for spec in specs:
        frame = frames[spec.timeframe]
        if len(frame.index) < 2:
            continue
        if not spec_matches_latest_bar(frame, spec.pattern, spec):
            continue
        latest = frame.iloc[-1]
        close_price = float(latest["close"])
        atr_value = float(latest["atr"]) if pd.notna(latest["atr"]) else close_price * 0.005
        atr_value = max(atr_value, close_price * 0.005)
        stop_price = close_price - spec.stop_atr_mult * atr_value if spec.direction == "long" else close_price + spec.stop_atr_mult * atr_value
        hold_minutes = spec.hold_bars * {"5m": 5, "15m": 15, "30m": 30, "1h": 60}[spec.timeframe]
        signals.append(
            {
                "timeframe": spec.timeframe,
                "pattern": spec.pattern,
                "direction": spec.direction,
                "signal_time": frame.index[-1].isoformat(),
                "reference_price": close_price,
                "stop_price": float(stop_price),
                "hold_minutes": hold_minutes,
                "stop_atr_mult": spec.stop_atr_mult,
                "min_body_ratio": spec.min_body_ratio,
                "require_volume_confirmation": spec.require_volume_confirmation,
            }
        )

    return {
        "data_path": str(data_path),
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "signal_count": len(signals),
        "signals": signals,
    }


def run_full_automation() -> Dict[str, object]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_runs: Dict[str, Dict[str, object]] = {}

    for label, patterns in DATASET_PATTERNS.items():
        data_path = find_latest_dataset(patterns)
        if data_path is None:
            report_runs[label] = {"status": "missing_dataset", "patterns": patterns}
            continue
        output_dir = OUTPUT_DIR / f"eth_candlestick_system_{label}"
        summary = run(data_path, output_dir)
        specs = load_selected_specs(output_dir / "summary.json")
        latest_signal = build_latest_signal(data_path, specs)
        (output_dir / "latest_signal.json").write_text(json.dumps(latest_signal, indent=2), encoding="utf-8")
        report_runs[label] = {
            "status": "ok",
            "data_path": str(data_path),
            "output_dir": str(output_dir),
            "summary": summary,
            "latest_signal": latest_signal,
        }

    consolidated = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "root": str(SVECHI_DIR),
        "runs": report_runs,
    }
    telegram_text = build_telegram_text(consolidated)
    telegram_result = send_telegram_message(telegram_text)
    consolidated["telegram"] = telegram_result
    (OUTPUT_DIR / "automation_summary.json").write_text(json.dumps(consolidated, indent=2), encoding="utf-8")
    (OUTPUT_DIR / "telegram_message.txt").write_text(telegram_text, encoding="utf-8")
    return consolidated


def main() -> None:
    summary = run_full_automation()
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
