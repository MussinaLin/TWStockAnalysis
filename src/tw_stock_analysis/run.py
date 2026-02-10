"""Main entry point for TW Stock Analysis."""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dotenv import load_dotenv

from .alpha import build_alpha_sheet, build_alpha_sheets_batch, build_summary_sheet
from .config import AppConfig
from .excel_utils import (
    get_sheet_names,
    load_history,
    remove_sheet,
    write_daily_sheet,
    write_market_closed_sheet,
)
from .indicators import compute_bollinger_bands, compute_macd, compute_rsi
from .prepare import (
    prepare_tpex_3insti,
    prepare_tpex_issued_shares,
    prepare_tpex_quotes,
    prepare_twse_3insti,
    prepare_twse_day_all,
    prepare_twse_issued_shares,
    prepare_twse_mi_index,
)
from .sources import (
    DataUnavailableError,
    fetch_tpex_3insti_v2,
    fetch_tpex_company_basic,
    fetch_tpex_daily_quotes_v2,
    fetch_twse_company_basic,
    fetch_twse_mi_index,
    fetch_twse_stock_day,
    fetch_twse_stock_day_all,
    fetch_twse_t86,
    find_twse_ohlcv,
)

OUTPUT_FILE = Path("tw_stock_daily.xlsx")
ALPHA_FILE = Path("alpha_pick.xlsx")
TAIPEI_TZ = ZoneInfo("Asia/Taipei")
DEFAULT_BACKFILL_DAYS = 120

# Cache for issued shares (doesn't change often)
_issued_shares_cache: dict[str, int] = {}


def _fetch_issued_shares(session: requests.Session) -> dict[str, int]:
    """Fetch issued shares for all TWSE and TPEX stocks.

    Returns dict mapping symbol to issued shares count.
    Uses cache to avoid repeated API calls.
    """
    global _issued_shares_cache
    if _issued_shares_cache:
        return _issued_shares_cache

    result: dict[str, int] = {}

    # Fetch TWSE listed companies
    try:
        twse_basic = fetch_twse_company_basic(session)
        twse_shares = prepare_twse_issued_shares(twse_basic)
        for _, row in twse_shares.iterrows():
            symbol = str(row["symbol"]).strip()
            issued = row["issued_shares"]
            if symbol and issued:
                result[symbol] = int(issued)
        print(f"已取得 {len(twse_shares)} 筆上市公司發行股數")
    except (DataUnavailableError, requests.RequestException) as exc:
        print(f"取得 TWSE 公司發行股數失敗：{exc}")

    # Fetch TPEX OTC companies
    try:
        tpex_basic = fetch_tpex_company_basic(session)
        tpex_shares = prepare_tpex_issued_shares(tpex_basic)
        for _, row in tpex_shares.iterrows():
            symbol = str(row["symbol"]).strip()
            issued = row["issued_shares"]
            if symbol and issued:
                result[symbol] = int(issued)
        print(f"已取得 {len(tpex_shares)} 筆上櫃公司發行股數")
    except (DataUnavailableError, requests.RequestException) as exc:
        print(f"取得 TPEX 公司發行股數失敗：{exc}")

    _issued_shares_cache = result
    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="台股每日分析")
    parser.add_argument(
        "--date",
        type=str,
        help="指定日期 (YYYY-MM-DD)，預設為台北當天日期",
    )
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=None,
        help="回補最近 N 天（含指定日期）",
    )
    parser.add_argument(
        "--backfill-start",
        type=str,
        default=None,
        help="回補起始日期 (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--backfill-end",
        type=str,
        default=None,
        help="回補結束日期 (YYYY-MM-DD)，預設為 --date 或當天",
    )
    parser.add_argument(
        "--init-backfill",
        action="store_true",
        help="若 Excel 不存在，初始化回補歷史資料",
    )
    parser.add_argument(
        "--replay",
        action="store_true",
        help="復盤模式：讀取現有 Excel 資料進行 alpha 分析，不呼叫 API",
    )
    parser.add_argument(
        "--replay-start",
        type=str,
        default=None,
        help="復盤起始日期 (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--replay-end",
        type=str,
        default=None,
        help="復盤結束日期 (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--update-summary",
        action="store_true",
        help="僅更新 alpha_pick.xlsx 的 summary sheet",
    )
    return parser.parse_args()


def _parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def _build_date_range(start: dt.date, end: dt.date) -> list[dt.date]:
    if start > end:
        start, end = end, start
    days = (end - start).days
    return [start + dt.timedelta(days=offset) for offset in range(days + 1)]


def _fetch_tpex_sources(
    session: requests.Session,
    date: dt.date,
) -> tuple[pd.DataFrame | None, dt.date | None, pd.DataFrame | None, dt.date | None]:
    """Fetch and prepare TPEX data sources."""
    tpex_quotes_raw, tpex_quotes_date = fetch_tpex_daily_quotes_v2(session, date)
    tpex_quotes = prepare_tpex_quotes(tpex_quotes_raw)

    tpex_3insti_raw, tpex_3insti_date = fetch_tpex_3insti_v2(session, date)
    tpex_3insti = prepare_tpex_3insti(tpex_3insti_raw)

    if tpex_quotes_date != date:
        tpex_quotes = None
    if tpex_3insti_date != date:
        tpex_3insti = None

    return tpex_quotes, tpex_quotes_date, tpex_3insti, tpex_3insti_date


def _fetch_twse_3insti(session: requests.Session, date: dt.date) -> pd.DataFrame:
    """Fetch and prepare TWSE institutional investors data."""
    twse_t86 = fetch_twse_t86(session, date)
    return prepare_twse_3insti(twse_t86)


def _build_daily_rows(
    session: requests.Session,
    date: dt.date,
    holdings: pd.DataFrame,
    history: dict[str, pd.Series],
    volume_history: dict[str, pd.Series],
    twse_3insti: pd.DataFrame,
    twse_day_all: pd.DataFrame | None,
    twse_mi_index: pd.DataFrame | None,
    tpex_quotes: pd.DataFrame,
    tpex_3insti: pd.DataFrame,
    twse_month_cache: dict[tuple[str, dt.date], pd.DataFrame],
    config: AppConfig,
    issued_shares: dict[str, int] | None = None,
) -> pd.DataFrame:
    """Build daily data rows for all holdings."""
    rows: list[dict] = []
    total = len(holdings)

    for idx, item in holdings.iterrows():
        symbol = str(item["symbol"]).strip()
        name = str(item["name"]).strip()
        if name.lower() == "nan":
            name = ""
        display_name = f" {name}" if name else ""
        print(f"{date.isoformat()} {idx + 1}/{total} {symbol}{display_name}")

        # Fetch OHLCV data with fallback chain
        ohlcv = _fetch_ohlcv_with_fallback(
            session, date, symbol, twse_day_all, twse_mi_index,
            tpex_quotes, twse_month_cache
        )
        open_price, close_price, high_price, low_price, volume = ohlcv

        # Get name from data sources if not available
        if not name:
            name = _get_name_from_sources(
                symbol, twse_day_all, twse_mi_index, tpex_quotes
            )

        # Get institutional investors data
        foreign_net, trust_net, dealer_net = _get_institutional_data(
            symbol, twse_3insti, tpex_3insti
        )

        # Update history and compute indicators
        series = _update_history(history, symbol, date, close_price)
        vol_series = _update_history(volume_history, symbol, date, volume)

        indicators = _compute_indicators(series, vol_series, config)

        # Convert to lots (張)
        volume_lots = volume // 1000 if volume is not None else None
        vol_ma5_lots = int(indicators["vol_ma5"] // 1000) if indicators["vol_ma5"] else None
        vol_ma10_lots = int(indicators["vol_ma10"] // 1000) if indicators["vol_ma10"] else None
        vol_ma20_lots = int(indicators["vol_ma20"] // 1000) if indicators["vol_ma20"] else None
        foreign_net_lots = foreign_net // 1000 if foreign_net is not None else None
        trust_net_lots = trust_net // 1000 if trust_net is not None else None
        dealer_net_lots = dealer_net // 1000 if dealer_net is not None else None
        insti_total_lots = (
            None
            if foreign_net_lots is None and trust_net_lots is None and dealer_net_lots is None
            else (foreign_net_lots or 0) + (trust_net_lots or 0) + (dealer_net_lots or 0)
        )

        # Calculate turnover rate
        turnover_rate = None
        if issued_shares and volume is not None:
            shares = issued_shares.get(symbol)
            if shares and shares > 0:
                turnover_rate = round(volume / shares * 100, 4)

        rows.append({
            "symbol": symbol,
            "name": name,
            "open": open_price,
            "close": close_price,
            "high": high_price,
            "low": low_price,
            "volume": volume_lots,
            "turnover_rate": turnover_rate,
            "vol_ma5": vol_ma5_lots,
            "vol_ma10": vol_ma10_lots,
            "vol_ma20": vol_ma20_lots,
            "foreign_net": foreign_net_lots,
            "trust_net": trust_net_lots,
            "dealer_net": dealer_net_lots,
            "institutional_investors_net": insti_total_lots,
            "rsi_14": indicators["rsi"],
            "macd": indicators["macd"],
            "macd_signal": indicators["macd_signal"],
            "macd_hist": indicators["macd_hist"],
            "bb_upper": indicators["bb_upper"],
            "bb_middle": indicators["bb_middle"],
            "bb_lower": indicators["bb_lower"],
            "bb_percent_b": indicators["bb_percent_b"],
            "bb_bandwidth": indicators["bb_bandwidth"],
        })

    return pd.DataFrame(rows)


def _fetch_ohlcv_with_fallback(
    session: requests.Session,
    date: dt.date,
    symbol: str,
    twse_day_all: pd.DataFrame | None,
    twse_mi_index: pd.DataFrame | None,
    tpex_quotes: pd.DataFrame,
    twse_month_cache: dict[tuple[str, dt.date], pd.DataFrame],
) -> tuple[float | None, float | None, float | None, float | None, int | None]:
    """Fetch OHLCV data with fallback chain: DAY_ALL -> STOCK_DAY -> MI_INDEX -> TPEX."""
    open_price = close_price = high_price = low_price = volume = None

    # Try TWSE STOCK_DAY_ALL
    if twse_day_all is not None:
        row = twse_day_all.loc[twse_day_all["symbol"] == symbol]
        if not row.empty:
            open_price = row.iloc[0]["open"]
            close_price = row.iloc[0]["close"]
            high_price = row.iloc[0].get("high")
            low_price = row.iloc[0].get("low")
            volume = row.iloc[0].get("volume")

    # Try TWSE STOCK_DAY (monthly)
    if any(v is None for v in [open_price, close_price, high_price, low_price, volume]):
        month_start = date.replace(day=1)
        cache_key = (symbol, month_start)
        twse_day = twse_month_cache.get(cache_key)

        if twse_day is None:
            try:
                twse_day = fetch_twse_stock_day(session, symbol, date)
                twse_month_cache[cache_key] = twse_day
            except DataUnavailableError:
                pass

        if twse_day is not None:
            ohlcv = find_twse_ohlcv(twse_day, date)
            if open_price is None:
                open_price = ohlcv[0]
            if high_price is None:
                high_price = ohlcv[1]
            if low_price is None:
                low_price = ohlcv[2]
            if close_price is None:
                close_price = ohlcv[3]
            if volume is None:
                volume = ohlcv[4]

    # Try TWSE MI_INDEX
    if any(v is None for v in [open_price, close_price, high_price, low_price, volume]):
        if twse_mi_index is not None:
            row = twse_mi_index.loc[twse_mi_index["symbol"] == symbol]
            if not row.empty:
                if open_price is None:
                    open_price = row.iloc[0]["open"]
                if close_price is None:
                    close_price = row.iloc[0]["close"]
                if high_price is None:
                    high_price = row.iloc[0].get("high")
                if low_price is None:
                    low_price = row.iloc[0].get("low")
                if volume is None:
                    volume = row.iloc[0].get("volume")

    # Try TPEX quotes
    if open_price is None and close_price is None:
        row = tpex_quotes.loc[tpex_quotes["symbol"] == symbol]
        if not row.empty:
            open_price = row.iloc[0]["open"]
            close_price = row.iloc[0]["close"]
            high_price = row.iloc[0].get("high")
            low_price = row.iloc[0].get("low")
            volume = row.iloc[0].get("volume")

    return open_price, close_price, high_price, low_price, volume


def _get_name_from_sources(
    symbol: str,
    twse_day_all: pd.DataFrame | None,
    twse_mi_index: pd.DataFrame | None,
    tpex_quotes: pd.DataFrame,
) -> str:
    """Try to get stock name from available data sources."""
    for df in [twse_day_all, twse_mi_index, tpex_quotes]:
        if df is None:
            continue
        if "name" not in df.columns:
            continue
        row = df.loc[df["symbol"] == symbol]
        if not row.empty:
            name = str(row.iloc[0].get("name", "")).strip()
            if name and name.lower() != "nan":
                return name
    return ""


def _get_institutional_data(
    symbol: str,
    twse_3insti: pd.DataFrame,
    tpex_3insti: pd.DataFrame,
) -> tuple[int | None, int | None, int | None]:
    """Get institutional investors net buy/sell data."""
    foreign_net = trust_net = dealer_net = None

    row = twse_3insti.loc[twse_3insti["symbol"] == symbol]
    if not row.empty:
        foreign_net = row.iloc[0]["foreign_net"]
        trust_net = row.iloc[0]["trust_net"]
        dealer_net = row.iloc[0]["dealer_net"]
    else:
        row = tpex_3insti.loc[tpex_3insti["symbol"] == symbol]
        if not row.empty:
            foreign_net = row.iloc[0]["foreign_net"]
            trust_net = row.iloc[0]["trust_net"]
            dealer_net = row.iloc[0]["dealer_net"]

    return foreign_net, trust_net, dealer_net


def _update_history(
    history: dict[str, pd.Series],
    symbol: str,
    date: dt.date,
    value: float | int | None,
) -> pd.Series:
    """Update history series with new value and return updated series."""
    series = history.get(symbol, pd.Series(dtype=float))
    if value is not None:
        series = pd.concat(
            [series, pd.Series([float(value)], index=pd.to_datetime([date]))]
        )
        series = series[~series.index.duplicated(keep="last")].sort_index()
        history[symbol] = series
    return series


def _compute_indicators(
    price_series: pd.Series,
    vol_series: pd.Series,
    config: AppConfig,
) -> dict:
    """Compute all technical indicators."""
    result = {
        "rsi": None,
        "macd": None,
        "macd_signal": None,
        "macd_hist": None,
        "vol_ma5": None,
        "vol_ma10": None,
        "vol_ma20": None,
        "bb_upper": None,
        "bb_middle": None,
        "bb_lower": None,
        "bb_percent_b": None,
        "bb_bandwidth": None,
    }

    # Volume MAs
    if len(vol_series) >= 5:
        result["vol_ma5"] = vol_series.tail(5).mean()
    if len(vol_series) >= 10:
        result["vol_ma10"] = vol_series.tail(10).mean()
    if len(vol_series) >= 20:
        result["vol_ma20"] = vol_series.tail(20).mean()

    # RSI
    if len(price_series) >= 14:
        rsi = compute_rsi(price_series)
        result["rsi"] = rsi.iloc[-1] if not rsi.empty else None

    # MACD
    if len(price_series) >= 2:
        macd, macd_signal, macd_hist = compute_macd(
            price_series,
            fast=config.macd_fast,
            slow=config.macd_slow,
            signal=config.macd_signal,
        )
        result["macd"] = macd.iloc[-1] if not macd.empty else None
        result["macd_signal"] = macd_signal.iloc[-1] if not macd_signal.empty else None
        result["macd_hist"] = macd_hist.iloc[-1] if not macd_hist.empty else None

    # Bollinger Bands
    if len(price_series) >= config.bb_period:
        bb_upper, bb_middle, bb_lower, bb_pct_b, bb_bw = compute_bollinger_bands(
            price_series, period=config.bb_period
        )
        result["bb_upper"] = bb_upper.iloc[-1] if not bb_upper.empty else None
        result["bb_middle"] = bb_middle.iloc[-1] if not bb_middle.empty else None
        result["bb_lower"] = bb_lower.iloc[-1] if not bb_lower.empty else None
        result["bb_percent_b"] = bb_pct_b.iloc[-1] if not bb_pct_b.empty else None
        result["bb_bandwidth"] = bb_bw.iloc[-1] if not bb_bw.empty else None

    return result


def _run_for_date(
    session: requests.Session,
    date: dt.date,
    holdings: pd.DataFrame,
    history: dict[str, pd.Series],
    volume_history: dict[str, pd.Series],
    sheet_names: set[str],
    twse_month_cache: dict[tuple[str, dt.date], pd.DataFrame],
    config: AppConfig,
    today: dt.date,
    skip_existing: bool = False,
    issued_shares: dict[str, int] | None = None,
) -> bool:
    """Process data for a single date."""
    sheet_name = date.isoformat()
    print(f"開始處理日期 {sheet_name}")

    # Skip weekends
    if date.weekday() >= 5:
        print(f"{sheet_name} 週末休市，略過寫入")
        write_market_closed_sheet(OUTPUT_FILE, date, "weekend", f"weekday={date.weekday()}")
        remove_sheet(OUTPUT_FILE, sheet_name)
        return False

    # Skip existing sheets in backfill mode
    if skip_existing and sheet_name in sheet_names:
        print(f"已存在 {sheet_name}，略過回補。")
        return False

    # Fetch TWSE 3-institutional data
    try:
        twse_3insti = _fetch_twse_3insti(session, date)
    except DataUnavailableError as exc:
        print(f"{sheet_name} TWSE 資料尚未公告或取得失敗：{exc}")
        twse_3insti = pd.DataFrame(columns=["symbol", "foreign_net", "trust_net", "dealer_net"])
    except requests.RequestException as exc:
        print(f"{sheet_name} TWSE 網路連線失敗：{exc}")
        return False

    # Fetch TWSE STOCK_DAY_ALL (today only)
    twse_day_all = None
    twse_day_all_date = None
    if date == today:
        try:
            twse_day_all_raw, twse_day_all_date = fetch_twse_stock_day_all(session)
            if twse_day_all_date is None:
                print(f"{sheet_name} TWSE STOCK_DAY_ALL 無法解析日期，略過使用")
            elif twse_day_all_date != date:
                print(f"{sheet_name} TWSE STOCK_DAY_ALL 日期不匹配：{twse_day_all_date} != {date}")
            else:
                twse_day_all = prepare_twse_day_all(twse_day_all_raw)
        except (DataUnavailableError, requests.RequestException) as exc:
            print(f"{sheet_name} TWSE STOCK_DAY_ALL 取得失敗：{exc}")

    # Fetch TWSE MI_INDEX
    twse_mi_index = None
    twse_mi_index_date = None
    try:
        twse_mi_index_raw, twse_mi_index_date = fetch_twse_mi_index(session, date)
        if twse_mi_index_date is None and not twse_mi_index_raw.empty and date == today:
            twse_mi_index_date = date
        if twse_mi_index_date == date:
            twse_mi_index = prepare_twse_mi_index(twse_mi_index_raw)
        elif twse_mi_index_date is not None:
            print(f"{sheet_name} TWSE MI_INDEX 日期不匹配：{twse_mi_index_date} != {date}")
    except (DataUnavailableError, requests.RequestException) as exc:
        print(f"{sheet_name} TWSE MI_INDEX 取得失敗：{exc}")

    # Check if TWSE data is available
    twse_confirmed = (
        (twse_day_all_date == date)
        or (twse_mi_index_date == date)
        or (not twse_3insti.empty)
    )
    if not twse_confirmed:
        print(f"{sheet_name} TWSE 資料不足，視為休市，略過寫入")
        write_market_closed_sheet(OUTPUT_FILE, date, "twse_unavailable", "")
        remove_sheet(OUTPUT_FILE, sheet_name)
        return False

    # Fetch TPEX data
    try:
        tpex_quotes, tpex_quotes_date, tpex_3insti, tpex_3insti_date = _fetch_tpex_sources(
            session, date
        )
        if tpex_quotes_date and tpex_quotes_date != date:
            print(f"{sheet_name} TPEX 日行情日期不匹配：{tpex_quotes_date} != {date}")
        if tpex_3insti_date and tpex_3insti_date != date:
            print(f"{sheet_name} TPEX 三大法人日期不匹配：{tpex_3insti_date} != {date}")
    except (DataUnavailableError, requests.RequestException) as exc:
        print(f"{sheet_name} TPEX 資料取得失敗：{exc}")
        tpex_quotes = None
        tpex_3insti = None

    if tpex_quotes is None:
        tpex_quotes = pd.DataFrame(columns=["symbol", "name", "open", "close", "high", "low", "volume"])
    if tpex_3insti is None:
        tpex_3insti = pd.DataFrame(columns=["symbol", "name", "foreign_net", "trust_net", "dealer_net"])

    # Build daily data
    output_df = _build_daily_rows(
        session=session,
        date=date,
        holdings=holdings,
        history=history,
        volume_history=volume_history,
        twse_3insti=twse_3insti,
        twse_day_all=twse_day_all,
        twse_mi_index=twse_mi_index,
        tpex_quotes=tpex_quotes,
        tpex_3insti=tpex_3insti,
        twse_month_cache=twse_month_cache,
        config=config,
        issued_shares=issued_shares,
    )

    if output_df.empty:
        print(f"{sheet_name} 找不到任何成份股資料。")
        return False

    if output_df["close"].isna().all():
        print(f"{sheet_name} 當天價格資料尚未公告，未寫入 Excel。")
        return False

    write_daily_sheet(OUTPUT_FILE, date, output_df)
    sheet_names.add(sheet_name)
    print(f"已寫入 {OUTPUT_FILE} ({sheet_name})")
    return True


def main() -> None:
    """Main entry point."""
    load_dotenv()
    config = AppConfig.from_env()
    args = _parse_args()
    today = dt.datetime.now(TAIPEI_TZ).date()
    target_date = _parse_date(args.date) if args.date else today

    # Update summary only mode
    if args.update_summary:
        if not ALPHA_FILE.exists():
            print(f"錯誤：{ALPHA_FILE} 不存在")
            return
        print(f"更新 {ALPHA_FILE} 的 summary sheet...")
        build_summary_sheet(ALPHA_FILE)
        print("summary sheet 更新完成")
        return

    # Replay mode: only run alpha analysis on existing data
    if args.replay or args.replay_start or args.replay_end:
        if not OUTPUT_FILE.exists():
            print(f"復盤模式錯誤：{OUTPUT_FILE} 不存在")
            return
        sheet_names = get_sheet_names(OUTPUT_FILE)

        # Determine replay date range
        if args.replay_start or args.replay_end:
            # Batch mode for date range - optimized
            replay_start = _parse_date(args.replay_start) if args.replay_start else target_date
            replay_end = _parse_date(args.replay_end) if args.replay_end else target_date
            replay_dates = _build_date_range(replay_start, replay_end)
            print(f"復盤模式（批次）：分析 {replay_dates[0]} ~ {replay_dates[-1]} 共 {len(replay_dates)} 天")
            build_alpha_sheets_batch(
                config, replay_dates, OUTPUT_FILE, ALPHA_FILE,
                sheet_prefix="replay"
            )
        else:
            # Single date mode
            target_sheet = target_date.isoformat()
            if target_sheet not in sheet_names:
                print(f"復盤模式錯誤：{OUTPUT_FILE} 中不存在 {target_sheet} sheet")
                return
            print(f"復盤模式：分析 {target_date} 及之前的資料")
            build_alpha_sheet(
                config, target_date, OUTPUT_FILE, ALPHA_FILE,
                max_date=target_date, sheet_prefix="replay"
            )
        return

    session = requests.Session()
    session.headers.update({"User-Agent": "tw-stock-daily/0.1"})

    if not config.extra_stocks:
        print("請在 .env 設定 STOCKS（逗號分隔的股票代號）")
        return

    holdings = pd.DataFrame([{"symbol": s, "name": ""} for s in config.extra_stocks])

    # Fetch issued shares for turnover rate calculation
    issued_shares = _fetch_issued_shares(session)

    history, volume_history = load_history(OUTPUT_FILE)
    sheet_names = get_sheet_names(OUTPUT_FILE)
    twse_month_cache: dict[tuple[str, dt.date], pd.DataFrame] = {}

    ran_backfill = False

    # Init backfill
    if args.init_backfill and not OUTPUT_FILE.exists():
        backfill_days = args.backfill_days or DEFAULT_BACKFILL_DAYS
        start_date = target_date - dt.timedelta(days=backfill_days - 1)
        backfill_dates = _build_date_range(start_date, target_date)
        print(f"初始化回補 {len(backfill_dates)} 天（含 {target_date.isoformat()}）")
        for date in backfill_dates:
            _run_for_date(
                session, date, holdings, history, volume_history,
                sheet_names, twse_month_cache, config, today,
                skip_existing=True,
                issued_shares=issued_shares,
            )
        ran_backfill = True

    # Backfill mode
    if args.backfill_start or args.backfill_end or args.backfill_days:
        if args.backfill_start:
            start_date = _parse_date(args.backfill_start)
        elif args.backfill_days:
            start_date = target_date - dt.timedelta(days=args.backfill_days - 1)
        else:
            start_date = target_date

        end_date = _parse_date(args.backfill_end) if args.backfill_end else target_date
        backfill_dates = _build_date_range(start_date, end_date)
        print(f"回補 {len(backfill_dates)} 天：{backfill_dates[0]} ~ {backfill_dates[-1]}")
        for date in backfill_dates:
            _run_for_date(
                session, date, holdings, history, volume_history,
                sheet_names, twse_month_cache, config, today,
                skip_existing=True,
                issued_shares=issued_shares,
            )
        ran_backfill = True

    # Single date mode
    if not ran_backfill:
        _run_for_date(
            session, target_date, holdings, history, volume_history,
            sheet_names, twse_month_cache, config, today,
            skip_existing=False,
            issued_shares=issued_shares,
        )

    # Generate alpha analysis
    build_alpha_sheet(config, target_date, OUTPUT_FILE, ALPHA_FILE)


if __name__ == "__main__":
    main()
