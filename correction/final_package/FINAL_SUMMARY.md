# Final Correction Project

Эта папка — финальная автономная поставка проекта.

Что внутри:

- `code/` — итоговый код системы.
- `scripts/` — PowerShell-скрипты запуска.
- `reports/` — ключевые подтвержденные отчеты.
- `README.md` — базовая документация по запуску.

Текущая структура:

- `correction.py` — зафиксированная ETH correction-база.
- `correction_trend.py` — отдельный ETH trend-модуль.
- `eth_supervisor.py` — объединение `correction + trend` без пересечений по времени.

Что зафиксировано как default:

- correction-база не изменялась.
- default trend profile: `profit_body_hq_t1h`
- default supervisor использует correction-базу + trend `profit_body_hq_t1h`

Актуальные контрольные результаты:

- baseline `30d`:
  - `7` сделок
  - `100%` win rate
  - `+0.2237`

- trend out-of-sample до `2026-01-20`:
  - `3` сделки
  - `100%` win rate
  - `+0.1302`

- trend out-of-sample до `2026-01-31`:
  - `7` сделок
  - `100%` win rate
  - `+0.3656`

- supervisor `60d` до `2026-03-20`:
  - `16` сделок
  - `100%` win rate
  - `+0.7735`

Главный вывод:

- correction — основа проекта.
- trend подключен отдельно и доработан так, чтобы не ломать базу.
- собранный supervisor — финальный рабочий контур проекта.

Запуск:

```powershell
.\scripts\run_correction.ps1 --days 90 --symbol ETH-USDT --initial-capital 10 --risk-percent 1
```

```powershell
.\scripts\run_trend.ps1 --days 90 --symbol ETH-USDT
```

```powershell
.\scripts\run_supervisor.ps1 --days 60 --symbol ETH-USDT --initial-capital 10 --risk-percent 1
```
