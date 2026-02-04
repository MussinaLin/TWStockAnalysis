# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Taiwan stock analysis tool for 00987A (台新優勢成長ETF) constituent stocks. Fetches daily OHLCV data, three institutional investor (外資/投信/自營商) net buy/sell figures, and computes technical indicators (RSI-14, MACD) from multiple Taiwan exchange APIs. Output is written to Excel with one worksheet per trading date.

## Commands

```bash
# Install (requires Python 3.13+)
pip install -e .

# Run (default: today's date in Taipei timezone)
tw-00987a-daily

# Specify date
tw-00987a-daily --date 2026-02-03

# Initialize with 120-day backfill if Excel doesn't exist
tw-00987a-daily --init-backfill

# Backfill last N days
tw-00987a-daily --backfill-days 90

# Backfill date range
tw-00987a-daily --backfill-start 2025-10-01 --backfill-end 2026-02-03

# Lint
ruff check src/
```

## Architecture

All source code is in `src/tw_stock_analysis/`. Entry point: `run.py:main()`.

- **config.py** — `AppConfig` frozen dataclass loaded from `.env` (TPEX URL templates, extra stock symbols via `STOCKS` env var)
- **sources.py** — Data fetching layer. Each `fetch_*` function hits a specific API and returns parsed DataFrames. Handles ROC/ISO date formats, encoding quirks, and CSV header detection. Raises `DataUnavailableError` on missing data.
- **indicators.py** — `compute_rsi()` and `compute_macd()` operating on `pd.Series` of closing prices
- **excel_utils.py** — `load_history()` reads historical closes from all dated sheets; `write_daily_sheet()` appends new date worksheets; tracks market closures in a `market_closed` sheet
- **run.py** — Orchestration. `_build_daily_rows()` is the core aggregation function that merges data from multiple sources with a multi-level fallback chain, then computes technical indicators incrementally using in-memory history.

### Data Source Fallback Chain

For each stock's daily price data, `_build_daily_rows()` tries in order:
1. TWSE STOCK_DAY_ALL (OpenAPI, current day all stocks)
2. TWSE STOCK_DAY (per-stock monthly API, cached)
3. TWSE MI_INDEX (market index fallback)
4. TPEX daily quotes (for OTC stocks)

This ensures robustness when individual APIs are unavailable.

### Column Matching

`run.py` uses `_normalize_col()` and `_find_column()` for fuzzy keyword matching on column names from external data sources, since column headers vary across APIs and may contain BOM characters or inconsistent whitespace.

## Configuration

Copy `.env.example` to `.env`. Key variables:
- `TPEX_DAILY_QUOTES_URL_TEMPLATE` / `TPEX_3INSTI_URL_TEMPLATE` — URL templates with `{roc}` or `{iso}` date placeholders for TPEX historical backfill
- `STOCKS` — Comma-separated extra stock symbols to include beyond 00987A constituents

## Ruff

Line length is 100 (`pyproject.toml`).
