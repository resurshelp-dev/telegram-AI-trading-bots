# Trading Suite

Unified runtime package for the trading bots that live next to `telegram_content_mvp` but must not interfere with it.

Included bots:

- `correction`
- `3bar`
- `svechi`
- `kaktak`
- `fixed`

Recommended server layout:

- `/opt/trading-bots`
  The whole repository root with the bot source folders and this `trading_suite` folder.
- `/opt/resurs-ai`
  The Telegram MVP stays separate.

What this suite gives you:

- every bot runs in `paper` mode by default
- every bot writes logs, state and reports into `trading_suite/runtime/<bot>`
- one consolidated summary is written into `trading_suite/runtime/summary`
- ready shell launchers for manual runs
- ready `systemd` units for "start it and forget it"

## Python Environment

Create one dedicated venv for all trading bots:

```bash
cd /opt/trading-bots
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r trading_suite/requirements.txt
```

You can override the interpreter for every launcher with:

```bash
export TRADING_PYTHON=/opt/trading-bots/.venv/bin/python
```

## Runtime Paths

All runtime data goes here:

- `trading_suite/runtime/correction`
- `trading_suite/runtime/3bar`
- `trading_suite/runtime/svechi`
- `trading_suite/runtime/kaktak`
- `trading_suite/runtime/fixed`
- `trading_suite/runtime/summary`

Inside each bot runtime:

- `logs/`
- `state/` when supported by the bot
- `reports/` when supported by the bot

## Manual Launch

```bash
cd /opt/trading-bots
chmod +x trading_suite/bin/*.sh
trading_suite/bin/prepare_runtime.sh
trading_suite/bin/run_correction.sh
trading_suite/bin/run_3bar.sh
trading_suite/bin/run_svechi.sh
trading_suite/bin/run_kaktak.sh
trading_suite/bin/run_fixed.sh
trading_suite/bin/run_summary_loop.sh
```

Quick status:

```bash
cd /opt/trading-bots
${TRADING_PYTHON:-python3} trading_suite/bin/status.py
```

## systemd

Copy the units from `trading_suite/systemd/` to `/etc/systemd/system/` and then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now trading-correction.service
sudo systemctl enable --now trading-3bar.service
sudo systemctl enable --now trading-svechi.service
sudo systemctl enable --now trading-kaktak.service
sudo systemctl enable --now trading-fixed.service
sudo systemctl enable --now trading-summary.service
```

Main control files:

- `trading_suite/runtime/summary/latest_summary.json`
- `trading_suite/runtime/summary/latest_summary.md`

If you want the safest first rollout, start in this order:

1. `trading-summary.service`
2. `trading-correction.service`
3. `trading-3bar.service`
4. `trading-svechi.service`
5. `trading-kaktak.service`
6. `trading-fixed.service`
