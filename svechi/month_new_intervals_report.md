# Month New Intervals Report

Источник:

- `correction/data_cache/ETH_USDT_5m_30d.csv`
- период: `2026-02-18 10:30 UTC` -> `2026-03-20 10:25 UTC`

## Новые интервалы, которые протестированы

- `10m`
- `20m`
- `45m`

## Сильные кандидаты по новым интервалам

### `10m`

- `inverted_hammer long`
  - лучший вариант: `hold=18`, `stop=1.0 ATR`, `min_body=0.2`
  - train: `19` сделок, WR `42.11%`, expectancy `0.059R`, PF `1.08`
  - test: `6` сделок, WR `83.33%`, expectancy `2.215R`, PF `12.42`
  - вывод: интересный, но пока слишком нестабилен по train и мал по выборке

- `hanging_man short`
  - лучший вариант: `hold=12`, `stop=0.8 ATR`, `min_body=0.0`
  - train: `25` сделок, WR `48.00%`, expectancy `0.794R`, PF `2.42`
  - test: `11` сделок, WR `45.45%`, expectancy `0.211R`, PF `1.36`
  - вывод: рабочий, но слабее базового `15m shooting_star`

### `20m`

- `morning_star long`
  - лучший вариант: `hold=12`, `stop=0.8 ATR`, `min_body=0.0`
  - train: `11` сделок, WR `45.45%`, expectancy `0.553R`, PF `1.83`
  - test: `4` сделки, WR `50.00%`, expectancy `0.005R`, PF `1.01`
  - вывод: аккуратный long-модуль, но скорее фильтрованный и редкий, чем сильный драйвер прибыли

### `45m`

- `bearish_engulfing short`
  - лучший устойчивый вариант: `hold=5`, `stop=0.8 ATR`, `min_body=0.2`, `volume_filter=on`
  - train: `9` сделок, WR `55.56%`, expectancy `1.741R`, PF `4.72`
  - test: `8` сделок, WR `37.50%`, expectancy `0.085R`, PF `1.12`
  - более сбалансированный вариант: `hold=3`, `stop=0.8 ATR`, `min_body=0.0`, `volume_filter=off`
  - train: `16` сделок, WR `62.50%`, expectancy `1.200R`, PF `4.72`
  - test: `10` сделок, WR `50.00%`, expectancy `0.203R`, PF `1.35`
  - вывод: лучший новый short-интервал из всех трёх

## Базовая система месяца

- `5m morning_star long`
  - `hold=36`
  - `stop=0.8 ATR`
- `15m shooting_star short`
  - `hold=12`
  - `stop=0.8 ATR`

Результат:

- all: `56` сделок, WR `44.64%`, expectancy `1.418R`, net `79.42R`, PF `3.18`
- train: `36` сделок, WR `41.67%`, expectancy `1.252R`, PF `2.87`
- test: `20` сделок, WR `50.00%`, expectancy `1.717R`, PF `3.78`

## Мягкие модификации

### 1. Более консервативная замена long-части

Заменить `5m morning_star` на `20m morning_star`.

Новая связка:

- `20m morning_star long`, `hold=12`, `stop=0.8 ATR`
- `15m shooting_star short`, `hold=12`, `stop=0.8 ATR`

Результат:

- all: `42` сделки, WR `47.62%`, expectancy `0.991R`, net `41.61R`, PF `2.58`
- train: `25` сделок, WR `44.00%`, expectancy `0.961R`, PF `2.45`
- test: `17` сделок, WR `52.94%`, expectancy `1.034R`, PF `2.81`

Вывод:

- винрейт выше базового
- сделок меньше
- система мягче и чище
- абсолютная прибыль ниже базовой

### 2. Более консервативная замена short-части

Заменить `15m shooting_star` на `45m bearish_engulfing`.

Новая связка:

- `5m morning_star long`, `hold=36`, `stop=0.8 ATR`
- `45m bearish_engulfing short`, `hold=5`, `stop=0.8 ATR`, `min_body=0.2`, `volume_filter=on`

Результат:

- all: `47` сделок, WR `44.68%`, expectancy `1.130R`, net `53.13R`, PF `2.77`
- train: `28` сделок, WR `42.86%`, expectancy `1.114R`, PF `2.74`
- test: `19` сделок, WR `47.37%`, expectancy `1.154R`, PF `2.82`

Вывод:

- частота ниже
- устойчивость хорошая
- прибыль ниже базовой, но система более размеренная

### 3. Мягкое расширение для роста абсолютной прибыли

Добавить к базовой системе `45m bearish_engulfing short`.

Новая связка:

- `5m morning_star long`
- `15m shooting_star short`
- `45m bearish_engulfing short`

Результат:

- all: `67` сделок, WR `44.78%`, expectancy `1.263R`, net `84.61R`, PF `2.98`
- train: `41` сделок, WR `43.90%`, expectancy `1.241R`, PF `2.97`
- test: `26` сделок, WR `46.15%`, expectancy `1.297R`, PF `2.99`

Вывод:

- net выше базовой
- винрейт почти не меняется
- expectancy немного ниже, но всё ещё сильная
- это лучший мягкий вариант для роста прибыли без сильной ломки системы

## Практический вывод

- Если цель номер один — сохранить высокий edge, базовую систему пока лучше не ломать.
- Если нужен более спокойный профиль и чуть выше win rate, лучше смотреть на замену `5m morning_star` -> `20m morning_star`.
- Если нужен мягкий рост абсолютной прибыли при похожем поведении системы, лучший кандидат — добавить `45m bearish_engulfing short`.
