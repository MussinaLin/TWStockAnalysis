# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Taiwan stock analysis tool that fetches daily OHLCV data, institutional investor net buy/sell figures, computes technical indicators (RSI, MACD, Bollinger Bands), and performs alpha stock picking analysis. Stocks are configured via `.env` file. Output is written to Excel files.

## Commands

```bash
# Install (requires Python 3.12+)
pip install -e .

# Run (default: today's date in Taipei timezone)
tw-stock-analysis

# Specify date
tw-stock-analysis --date 2025-10-15

# Initialize with 120-day backfill if Excel doesn't exist
tw-stock-analysis --init-backfill

# Backfill last N days
tw-stock-analysis --backfill-days 90

# Backfill date range
tw-stock-analysis --backfill-start 2025-08-01 --backfill-end 2025-10-15

# Replay mode (alpha analysis on existing data, no API calls)
tw-stock-analysis --replay --date 2025-10-15
tw-stock-analysis --replay-start 2025-10-01 --replay-end 2025-10-15

# Lint
ruff check src/
```

## Architecture

All source code is in `src/tw_stock_analysis/`. Entry point: `run.py:main()`.

### Core Modules

- **run.py** — CLI entry point and orchestration. Handles data fetching, indicator computation, and output generation.
- **config.py** — `AppConfig` frozen dataclass loaded from `.env`. Contains all configurable parameters (MACD, Bollinger Bands, alpha conditions).
- **sources.py** — Data fetching layer. Each `fetch_*` function hits a specific API and returns parsed DataFrames. Handles ROC/ISO date formats and encoding quirks. Raises `DataUnavailableError` on missing data.
- **prepare.py** — Data preparation and normalization. `_find_columns()` for fuzzy column matching, `prepare_*` functions to standardize DataFrames from different sources.
- **indicators.py** — Technical indicator calculations: `compute_rsi()`, `compute_macd()`, `compute_bollinger_bands()`.
- **alpha.py** — Alpha stock picking analysis. `build_alpha_sheet()` evaluates multiple conditions and writes results to Excel.
- **excel_utils.py** — Excel I/O utilities. `load_history()` reads historical data; `write_daily_sheet()` appends new worksheets.

### Data Source Fallback Chain

For each stock's daily price data, the system tries in order:
1. TWSE STOCK_DAY_ALL (OpenAPI, current day all stocks)
2. TWSE STOCK_DAY (per-stock monthly API, cached)
3. TWSE MI_INDEX (market index fallback)
4. TPEX daily quotes V2 (for OTC stocks)

### Alpha Picking Conditions

Selection logic:
1. **Required**: cond_insti AND (cond_vol_ma10 OR cond_vol_ma20)
2. **Optional** (at least 2 must be true): cond_rsi, cond_macd, cond_bb_narrow, cond_bb_near_upper

| Condition | Type | Description |
|-----------|------|-------------|
| cond_insti | Required | Institutional net buy: recent avg > long-term avg |
| cond_vol_ma10 | Required (either) | Volume > 10MA × ratio |
| cond_vol_ma20 | Required (either) | Volume > 20MA × ratio |
| cond_rsi | Optional | RSI in healthy range (default 40-70) |
| cond_macd | Optional | MACD histogram > threshold |
| cond_bb_narrow | Optional | Bollinger bandwidth narrowing |
| cond_bb_near_upper | Optional | %B > threshold (approaching upper band) |

## Output Files

- **tw_stock_daily.xlsx** — Daily trading data, one sheet per date
- **alpha_pick.xlsx** — Alpha analysis results
  - `alpha_YYYY-MM-DD` — Regular analysis
  - `replay_YYYY-MM-DD` — Replay mode analysis

## Configuration

Copy `.env.example` to `.env`. Key sections:

```bash
# Required: Stock list
STOCKS=2330,2317,2454

# MACD parameters
MACD_FAST=8
MACD_SLOW=17
MACD_SIGNAL=9

# Bollinger Bands
BB_PERIOD=20
BB_NARROW_SHORT_DAYS=5
BB_NARROW_LONG_DAYS=20
BB_PERCENT_B_MIN=0.75

# Alpha conditions
VOL_BREAKOUT_RATIO=1.5
ALPHA_RSI_MIN=40
ALPHA_RSI_MAX=70
ALPHA_MACD_HIST_MIN=0
ALPHA_INSTI_DAYS_SHORT=15
ALPHA_INSTI_DAYS_LONG=30
```

See `.env.example` for full documentation of all parameters.

## Code Style

- Line length: 100 (configured in `pyproject.toml`)
- Use `ruff check src/` for linting
