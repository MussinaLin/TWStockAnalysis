from __future__ import annotations

import argparse
import datetime as dt
import re
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dotenv import load_dotenv

from .config import AppConfig
from .excel_utils import get_sheet_names, load_history, write_daily_sheet
from .indicators import compute_macd, compute_rsi
from .sources import (
    DataUnavailableError,
    _clean_int,
    _clean_number,
    fetch_00987a_holdings,
    fetch_tpex_3insti,
    fetch_tpex_daily_quotes,
    fetch_twse_stock_day_all,
    fetch_twse_stock_day,
    fetch_twse_t86,
    find_twse_open_close,
)

OUTPUT_FILE = Path("tw_00987A_daily.xlsx")
TAIPEI_TZ = ZoneInfo("Asia/Taipei")
DEFAULT_BACKFILL_DAYS = 120


def _normalize_col(text: str) -> str:
    cleaned = text.replace("\ufeff", "")
    cleaned = re.sub(r"\s+", "", cleaned)
    return cleaned.lower()


def _find_column(df: pd.DataFrame, keywords: list[str]) -> str | None:
    normalized_keywords = [_normalize_col(keyword) for keyword in keywords]
    for col in df.columns:
        text = _normalize_col(str(col))
        if all(keyword in text for keyword in normalized_keywords):
            return col
    return None


def _prepare_tpex_quotes(df: pd.DataFrame) -> pd.DataFrame:
    symbol_col = _find_column(df, ["證券代號"]) or _find_column(df, ["代號"])
    name_col = _find_column(df, ["名稱"])
    open_col = _find_column(df, ["開盤"]) or _find_column(df, ["開盤價"])
    close_col = _find_column(df, ["收盤"]) or _find_column(df, ["收盤價"])
    if not symbol_col or not open_col or not close_col:
        columns = ", ".join([str(col) for col in df.columns[:10]])
        raise DataUnavailableError(f"TPEX 行情欄位解析失敗，欄位={columns}")

    use_cols = [symbol_col, open_col, close_col]
    if name_col:
        use_cols.insert(1, name_col)

    temp = df[use_cols].copy()
    if name_col:
        temp.columns = ["symbol", "name", "open", "close"]
        temp["name"] = temp["name"].astype(str).str.strip().replace({"nan": ""})
    else:
        temp.columns = ["symbol", "open", "close"]
        temp["name"] = ""
    temp["symbol"] = temp["symbol"].astype(str).str.strip()
    temp["open"] = temp["open"].map(_clean_number)
    temp["close"] = temp["close"].map(_clean_number)
    return temp


def _prepare_tpex_3insti(df: pd.DataFrame) -> pd.DataFrame:
    symbol_col = _find_column(df, ["證券代號"]) or _find_column(df, ["代號"])
    name_col = _find_column(df, ["名稱"])
    foreign_col = _find_column(df, ["外資", "買賣超"])
    trust_col = _find_column(df, ["投信", "買賣超"])
    dealer_col = _find_column(df, ["自營商", "買賣超"])
    if not symbol_col or not foreign_col or not trust_col or not dealer_col:
        raise DataUnavailableError("TPEX 三大法人欄位解析失敗")

    use_cols = [symbol_col, foreign_col, trust_col, dealer_col]
    if name_col:
        use_cols.insert(1, name_col)

    temp = df[use_cols].copy()
    if name_col:
        temp.columns = ["symbol", "name", "foreign_net", "trust_net", "dealer_net"]
        temp["name"] = temp["name"].astype(str).str.strip().replace({"nan": ""})
    else:
        temp.columns = ["symbol", "foreign_net", "trust_net", "dealer_net"]
        temp["name"] = ""
    temp["symbol"] = temp["symbol"].astype(str).str.strip()
    temp["foreign_net"] = temp["foreign_net"].map(_clean_int)
    temp["trust_net"] = temp["trust_net"].map(_clean_int)
    temp["dealer_net"] = temp["dealer_net"].map(_clean_int)
    return temp


def _prepare_twse_3insti(df: pd.DataFrame) -> pd.DataFrame:
    symbol_col = _find_column(df, ["證券代號"]) or _find_column(df, ["代號"])
    name_col = _find_column(df, ["名稱"])
    foreign_col = _find_column(df, ["外資", "買賣超"])
    trust_col = _find_column(df, ["投信", "買賣超"])
    dealer_col = _find_column(df, ["自營商", "買賣超"])
    if not symbol_col or not foreign_col or not trust_col or not dealer_col:
        raise DataUnavailableError("TWSE 三大法人欄位解析失敗")

    use_cols = [symbol_col, foreign_col, trust_col, dealer_col]
    if name_col:
        use_cols.insert(1, name_col)

    temp = df[use_cols].copy()
    if name_col:
        temp.columns = ["symbol", "name", "foreign_net", "trust_net", "dealer_net"]
        temp["name"] = temp["name"].astype(str).str.strip().replace({"nan": ""})
    else:
        temp.columns = ["symbol", "foreign_net", "trust_net", "dealer_net"]
        temp["name"] = ""
    temp["symbol"] = temp["symbol"].astype(str).str.strip()
    temp["foreign_net"] = temp["foreign_net"].map(_clean_int)
    temp["trust_net"] = temp["trust_net"].map(_clean_int)
    temp["dealer_net"] = temp["dealer_net"].map(_clean_int)
    return temp


def _prepare_twse_day_all(df: pd.DataFrame) -> pd.DataFrame:
    symbol_col = _find_column(df, ["code"]) or _find_column(df, ["證券代號"]) or _find_column(df, ["代號"])
    name_col = _find_column(df, ["name"]) or _find_column(df, ["證券名稱"]) or _find_column(df, ["名稱"])
    open_col = (
        _find_column(df, ["openingprice"])
        or _find_column(df, ["open"])
        or _find_column(df, ["開盤價"])
        or _find_column(df, ["開盤"])
    )
    close_col = (
        _find_column(df, ["closingprice"])
        or _find_column(df, ["close"])
        or _find_column(df, ["收盤價"])
        or _find_column(df, ["收盤"])
    )

    if not symbol_col or not open_col or not close_col:
        raise DataUnavailableError("TWSE STOCK_DAY_ALL 欄位解析失敗")

    use_cols = [symbol_col, open_col, close_col]
    if name_col:
        use_cols.insert(1, name_col)

    temp = df[use_cols].copy()
    if name_col:
        temp.columns = ["symbol", "name", "open", "close"]
        temp["name"] = temp["name"].astype(str).str.strip().replace({"nan": ""})
    else:
        temp.columns = ["symbol", "open", "close"]
        temp["name"] = ""

    temp["symbol"] = temp["symbol"].astype(str).str.strip()
    temp["open"] = temp["open"].map(_clean_number)
    temp["close"] = temp["close"].map(_clean_number)
    return temp


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="00987A 成份股每日分析")
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
    return parser.parse_args()


def _parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def _build_date_range(start: dt.date, end: dt.date) -> list[dt.date]:
    if start > end:
        start, end = end, start
    days = (end - start).days
    return [start + dt.timedelta(days=offset) for offset in range(days + 1)]


def _prepare_tpex_sources(
    session: requests.Session,
    date: dt.date,
    config: AppConfig,
    today: dt.date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if date == today and not config.tpex_daily_quotes_url_template:
        tpex_quotes_raw = fetch_tpex_daily_quotes(session, None, template=None)
    else:
        tpex_quotes_raw = fetch_tpex_daily_quotes(
            session, date, template=config.tpex_daily_quotes_url_template
        )

    if date == today and not config.tpex_3insti_url_template:
        tpex_3insti_raw = fetch_tpex_3insti(session, None, template=None)
    else:
        tpex_3insti_raw = fetch_tpex_3insti(
            session, date, template=config.tpex_3insti_url_template
        )
    tpex_quotes = _prepare_tpex_quotes(tpex_quotes_raw)
    tpex_3insti = _prepare_tpex_3insti(tpex_3insti_raw)
    return tpex_quotes, tpex_3insti


def _prepare_twse_sources(session: requests.Session, date: dt.date) -> pd.DataFrame:
    twse_t86 = fetch_twse_t86(session, date)
    return _prepare_twse_3insti(twse_t86)


def _build_daily_rows(
    session: requests.Session,
    date: dt.date,
    holdings: pd.DataFrame,
    history: dict[str, pd.Series],
    twse_3insti: pd.DataFrame,
    twse_day_all: pd.DataFrame | None,
    tpex_quotes: pd.DataFrame,
    tpex_3insti: pd.DataFrame,
    twse_month_cache: dict[tuple[str, dt.date], pd.DataFrame],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for _, item in holdings.iterrows():
        symbol = str(item["symbol"]).strip()
        name = str(item["name"]).strip()
        if name.lower() == "nan":
            name = ""

        open_price = None
        close_price = None
        foreign_net = None
        trust_net = None
        dealer_net = None

        if twse_day_all is not None:
            row_all = twse_day_all.loc[twse_day_all["symbol"] == symbol]
            if not row_all.empty:
                open_price = row_all.iloc[0]["open"]
                close_price = row_all.iloc[0]["close"]
                if not name and "name" in row_all.columns:
                    name = str(row_all.iloc[0].get("name", "")).strip()

        if open_price is None and close_price is None:
            month_start = date.replace(day=1)
            cache_key = (symbol, month_start)
            twse_day = None
            if cache_key in twse_month_cache:
                twse_day = twse_month_cache[cache_key]
            else:
                try:
                    twse_day = fetch_twse_stock_day(session, symbol, date)
                except DataUnavailableError:
                    twse_day = None
                if twse_day is not None:
                    twse_month_cache[cache_key] = twse_day

            if twse_day is not None:
                open_price, close_price = find_twse_open_close(twse_day, date)

        if open_price is None and close_price is None:
            row = tpex_quotes.loc[tpex_quotes["symbol"] == symbol]
            if not row.empty:
                open_price = row.iloc[0]["open"]
                close_price = row.iloc[0]["close"]
                if not name and "name" in row.columns:
                    name = str(row.iloc[0].get("name", "")).strip()

        row_twse = twse_3insti.loc[twse_3insti["symbol"] == symbol]
        if not row_twse.empty:
            foreign_net = row_twse.iloc[0]["foreign_net"]
            trust_net = row_twse.iloc[0]["trust_net"]
            dealer_net = row_twse.iloc[0]["dealer_net"]
            if not name and "name" in row_twse.columns:
                name = str(row_twse.iloc[0].get("name", "")).strip()
        else:
            row_tpex = tpex_3insti.loc[tpex_3insti["symbol"] == symbol]
            if not row_tpex.empty:
                foreign_net = row_tpex.iloc[0]["foreign_net"]
                trust_net = row_tpex.iloc[0]["trust_net"]
                dealer_net = row_tpex.iloc[0]["dealer_net"]
                if not name and "name" in row_tpex.columns:
                    name = str(row_tpex.iloc[0].get("name", "")).strip()

        series = history.get(symbol, pd.Series(dtype=float))
        if close_price is not None:
            series = pd.concat(
                [series, pd.Series([float(close_price)], index=pd.to_datetime([date]))]
            )
            series = series[~series.index.duplicated(keep="last")].sort_index()
            history[symbol] = series

        rsi = compute_rsi(series).iloc[-1] if len(series) >= 14 else None
        macd, macd_signal, macd_hist = compute_macd(series) if len(series) >= 2 else (
            pd.Series(dtype=float),
            pd.Series(dtype=float),
            pd.Series(dtype=float),
        )
        macd_value = macd.iloc[-1] if not macd.empty else None
        macd_signal_value = macd_signal.iloc[-1] if not macd_signal.empty else None
        macd_hist_value = macd_hist.iloc[-1] if not macd_hist.empty else None

        rows.append(
            {
                "symbol": symbol,
                "name": name,
                "open": open_price,
                "close": close_price,
                "foreign_net": foreign_net,
                "trust_net": trust_net,
                "dealer_net": dealer_net,
                "institutional_investors_net": (
                    None
                    if foreign_net is None and trust_net is None and dealer_net is None
                    else (foreign_net or 0) + (trust_net or 0) + (dealer_net or 0)
                ),
                "rsi_14": rsi,
                "macd": macd_value,
                "macd_signal": macd_signal_value,
                "macd_hist": macd_hist_value,
            }
        )

    return pd.DataFrame(rows)


def _run_for_date(
    session: requests.Session,
    date: dt.date,
    holdings: pd.DataFrame,
    history: dict[str, pd.Series],
    sheet_names: set[str],
    twse_month_cache: dict[tuple[str, dt.date], pd.DataFrame],
    config: AppConfig,
    today: dt.date,
    skip_existing: bool = False,
) -> bool:
    sheet_name = date.isoformat()
    if skip_existing and sheet_name in sheet_names:
        print(f"已存在 {sheet_name}，略過回補。")
        return False

    try:
        twse_3insti = _prepare_twse_sources(session, date)
    except DataUnavailableError as exc:
        print(f"{sheet_name} TWSE 資料尚未公告或取得失敗：{exc}")
        twse_3insti = pd.DataFrame(
            columns=["symbol", "foreign_net", "trust_net", "dealer_net"]
        )
    except requests.RequestException as exc:
        print(f"{sheet_name} TWSE 網路連線失敗：{exc}")
        return False

    twse_day_all = None
    if date == today:
        try:
            twse_day_all_raw = fetch_twse_stock_day_all(session)
            twse_day_all = _prepare_twse_day_all(twse_day_all_raw)
        except DataUnavailableError as exc:
            print(f"{sheet_name} TWSE STOCK_DAY_ALL 取得失敗：{exc}")
            twse_day_all = None
        except requests.RequestException as exc:
            print(f"{sheet_name} TWSE STOCK_DAY_ALL 網路連線失敗：{exc}")
            twse_day_all = None

    try:
        tpex_quotes, tpex_3insti = _prepare_tpex_sources(session, date, config, today)
    except DataUnavailableError as exc:
        print(f"{sheet_name} TPEX 資料尚未公告或取得失敗：{exc}")
        tpex_quotes = pd.DataFrame(columns=["symbol", "open", "close"])
        tpex_3insti = pd.DataFrame(
            columns=["symbol", "foreign_net", "trust_net", "dealer_net"]
        )
    except requests.RequestException as exc:
        print(f"{sheet_name} TPEX 網路連線失敗：{exc}")
        return False

    output_df = _build_daily_rows(
        session=session,
        date=date,
        holdings=holdings,
        history=history,
        twse_3insti=twse_3insti,
        twse_day_all=twse_day_all,
        tpex_quotes=tpex_quotes,
        tpex_3insti=tpex_3insti,
        twse_month_cache=twse_month_cache,
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
    load_dotenv()
    config = AppConfig.from_env()
    args = _parse_args()
    today = dt.datetime.now(TAIPEI_TZ).date()
    target_date = _parse_date(args.date) if args.date else today

    session = requests.Session()
    session.headers.update({"User-Agent": "tw-00987a-daily/0.1"})

    try:
        holdings = fetch_00987a_holdings(session)
    except DataUnavailableError as exc:
        print(f"成份股資料取得失敗：{exc}")
        return
    except requests.RequestException as exc:
        print(f"成份股網路連線失敗：{exc}")
        return

    if config.extra_stocks:
        extra_df = pd.DataFrame(
            [{"symbol": symbol, "name": ""} for symbol in config.extra_stocks]
        )
        holdings = pd.concat([holdings, extra_df], ignore_index=True)
        holdings = holdings.drop_duplicates(subset=["symbol"]).reset_index(drop=True)

    history = load_history(OUTPUT_FILE)
    sheet_names = get_sheet_names(OUTPUT_FILE)
    twse_month_cache: dict[tuple[str, dt.date], pd.DataFrame] = {}

    ran_backfill = False

    if args.init_backfill and not OUTPUT_FILE.exists():
        backfill_days = args.backfill_days or DEFAULT_BACKFILL_DAYS
        start_date = target_date - dt.timedelta(days=backfill_days - 1)
        backfill_dates = _build_date_range(start_date, target_date)
        print(f"初始化回補 {len(backfill_dates)} 天（含 {target_date.isoformat()}）")
        for date in backfill_dates:
            _run_for_date(
                session=session,
                date=date,
                holdings=holdings,
                history=history,
                sheet_names=sheet_names,
                twse_month_cache=twse_month_cache,
                config=config,
                today=today,
                skip_existing=True,
            )
        ran_backfill = True

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
                session=session,
                date=date,
                holdings=holdings,
                history=history,
                sheet_names=sheet_names,
                twse_month_cache=twse_month_cache,
                config=config,
                today=today,
                skip_existing=True,
            )
        ran_backfill = True

    if not ran_backfill:
        _run_for_date(
            session=session,
            date=target_date,
            holdings=holdings,
            history=history,
            sheet_names=sheet_names,
            twse_month_cache=twse_month_cache,
            config=config,
            today=today,
            skip_existing=False,
        )


if __name__ == "__main__":
    main()
