# Fixed Autonomous Contrarian Bot

Everything needed for this bot lives in this folder.

Files:

- `contrarian_bot.py` - strategy, BingX API, live loop, smoke test, state recovery
- `.env.example` - runtime configuration template
- `requirements.txt` - Python dependencies
- `start_live.ps1` - continuous live/paper launcher with log redirection
- `start_smoke_test.ps1` - minimal live exchange plumbing test
- `logs/` - rotating text log and JSONL event log
- `state/` - runtime state for restart recovery
- `reports/` - backtest summary and trade log

Recommended first steps:

1. Copy `.env.example` to `.env`
2. Fill in `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`
3. Keep `PAPER=true` for the first live run
4. Start the bot with `start_live.ps1`

Backtest:

```powershell
C:\Users\User\PycharmProjects\neirosystems\maintest\.venv\Scripts\python.exe .\fixed\contrarian_bot.py --mode backtest --data-file .\data_cache\BTC_USDT_5m_60d.csv --days 30 --verbose false --show-progress false
```

Paper live:

```powershell
powershell -ExecutionPolicy Bypass -File .\fixed\start_live.ps1
```

Real live:

```powershell
powershell -ExecutionPolicy Bypass -File .\fixed\start_live.ps1 -Paper:$false
```

Minimal real exchange smoke test:

```powershell
powershell -ExecutionPolicy Bypass -File .\fixed\start_smoke_test.ps1 -Side BUY -Qty 0.0001
```

Notes:

- Entry signals are unchanged.
- The bot uses market entries, exchange SL/TP, local trailing updates, state recovery and stale-order cleanup.
- Any dangling non-reduce-only orders or protection orders without a matching live position are canceled automatically.
