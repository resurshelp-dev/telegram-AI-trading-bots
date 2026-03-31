# ETH Correction + Trend

Это автономная ETH-сборка из трех уровней:

- `correction.py` — базовый correction-контур.
- `correction_trend.py` — отдельный trend-контур.
- `eth_supervisor.py` — объединяет `correction + trend` без пересечений по времени.

Файлы:

- `correction_regime.py` — BingX client, кэш и общие утилиты.
- `correction_hourly.py` — active exhaustion/fib логика.
- `correction_lab.py` — reversal lab логика.
- `correction_trend_pullback.py` — базовая trend pullback логика.
- `correction_trend.py` — HQ trend rules и trend profile.

Запуск:

```powershell
.\correction\run_correction.ps1 --days 90 --symbol ETH-USDT --initial-capital 10 --risk-percent 1
```

```powershell
.\correction\run_trend.ps1 --days 90 --symbol ETH-USDT --profile profit_body_hq_t1h
```

```powershell
.\correction\run_supervisor.ps1 --days 90 --symbol ETH-USDT --initial-capital 10 --risk-percent 1
```

По умолчанию:

- кэш: `correction/data_cache`
- correction отчеты: `correction/reports/baseline_run`
- trend отчеты: `correction/reports/trend_run`
- supervisor отчеты: `correction/reports/supervisor_run`

Текущий рабочий trend profile:

- `profit_body_hq_t1h`

Если нужны ключи BingX через окружение:

- `BINGX_API_KEY`
- `BINGX_SECRET_KEY`

Для исторических свечей обычно достаточно публичного market data API.
