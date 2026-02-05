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
from .excel_utils import (
    get_sheet_names,
    load_history,
    remove_sheet,
    write_daily_sheet,
    write_market_closed_sheet,
)
from .indicators import compute_macd, compute_rsi
from .sources import (
    DataUnavailableError,
    _clean_int,
    _clean_number,
    fetch_tpex_3insti_v2,
    fetch_tpex_daily_quotes_v2,
    fetch_twse_stock_day_all,
    fetch_twse_stock_day,
    fetch_twse_mi_index,
    fetch_twse_t86,
    find_twse_ohlcv,
)

OUTPUT_FILE = Path("tw_stock_daily.xlsx")
ALPHA_FILE = Path("alpha_pick.xlsx")
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
    high_col = _find_column(df, ["最高"]) or _find_column(df, ["最高價"])
    low_col = _find_column(df, ["最低"]) or _find_column(df, ["最低價"])
    volume_col = _find_column(df, ["成交股數"]) or _find_column(df, ["成交量"])
    if not symbol_col or not open_col or not close_col:
        columns = ", ".join([str(col) for col in df.columns[:10]])
        raise DataUnavailableError(f"TPEX 行情欄位解析失敗，欄位={columns}")

    use_cols = [symbol_col, open_col, close_col]
    if name_col:
        use_cols.insert(1, name_col)
    if high_col:
        use_cols.append(high_col)
    if low_col:
        use_cols.append(low_col)
    if volume_col:
        use_cols.append(volume_col)

    temp = df[use_cols].copy()
    columns = ["symbol", "open", "close"]
    if name_col:
        columns.insert(1, "name")
    if high_col:
        columns.append("high")
    if low_col:
        columns.append("low")
    if volume_col:
        columns.append("volume")

    temp.columns = columns
    if name_col:
        temp["name"] = temp["name"].astype(str).str.strip().replace({"nan": ""})
    else:
        temp["name"] = ""
    temp["symbol"] = temp["symbol"].astype(str).str.strip()
    temp["open"] = temp["open"].map(_clean_number)
    temp["close"] = temp["close"].map(_clean_number)
    if "high" in temp.columns:
        temp["high"] = temp["high"].map(_clean_number)
    else:
        temp["high"] = None
    if "low" in temp.columns:
        temp["low"] = temp["low"].map(_clean_number)
    else:
        temp["low"] = None
    if "volume" in temp.columns:
        temp["volume"] = temp["volume"].map(_clean_int)
    else:
        temp["volume"] = None
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
    high_col = (
        _find_column(df, ["highestprice"])
        or _find_column(df, ["high"])
        or _find_column(df, ["最高價"])
        or _find_column(df, ["最高"])
    )
    low_col = (
        _find_column(df, ["lowestprice"])
        or _find_column(df, ["low"])
        or _find_column(df, ["最低價"])
        or _find_column(df, ["最低"])
    )
    volume_col = (
        _find_column(df, ["tradevolume"])
        or _find_column(df, ["成交股數"])
        or _find_column(df, ["成交量"])
    )

    if not symbol_col or not open_col or not close_col:
        raise DataUnavailableError("TWSE STOCK_DAY_ALL 欄位解析失敗")

    use_cols = [symbol_col, open_col, close_col]
    if name_col:
        use_cols.insert(1, name_col)
    if high_col:
        use_cols.append(high_col)
    if low_col:
        use_cols.append(low_col)
    if volume_col:
        use_cols.append(volume_col)

    temp = df[use_cols].copy()
    columns = ["symbol", "open", "close"]
    if name_col:
        columns.insert(1, "name")
    if high_col:
        columns.append("high")
    if low_col:
        columns.append("low")
    if volume_col:
        columns.append("volume")

    temp.columns = columns
    if name_col:
        temp["name"] = temp["name"].astype(str).str.strip().replace({"nan": ""})
    else:
        temp["name"] = ""

    temp["symbol"] = temp["symbol"].astype(str).str.strip()
    temp["open"] = temp["open"].map(_clean_number)
    temp["close"] = temp["close"].map(_clean_number)
    if "high" in temp.columns:
        temp["high"] = temp["high"].map(_clean_number)
    else:
        temp["high"] = None
    if "low" in temp.columns:
        temp["low"] = temp["low"].map(_clean_number)
    else:
        temp["low"] = None
    if "volume" in temp.columns:
        temp["volume"] = temp["volume"].map(_clean_int)
    else:
        temp["volume"] = None
    return temp


def _prepare_twse_mi_index(df: pd.DataFrame) -> pd.DataFrame:
    symbol_col = _find_column(df, ["證券代號"]) or _find_column(df, ["代號"])
    name_col = _find_column(df, ["證券名稱"]) or _find_column(df, ["名稱"])
    open_col = _find_column(df, ["開盤價"]) or _find_column(df, ["開盤"])
    close_col = _find_column(df, ["收盤價"]) or _find_column(df, ["收盤"])
    high_col = _find_column(df, ["最高價"]) or _find_column(df, ["最高"])
    low_col = _find_column(df, ["最低價"]) or _find_column(df, ["最低"])
    volume_col = _find_column(df, ["成交股數"]) or _find_column(df, ["成交量"])

    if not symbol_col or not open_col or not close_col:
        raise DataUnavailableError("TWSE MI_INDEX 欄位解析失敗")

    use_cols = [symbol_col, open_col, close_col]
    if name_col:
        use_cols.insert(1, name_col)
    if high_col:
        use_cols.append(high_col)
    if low_col:
        use_cols.append(low_col)
    if volume_col:
        use_cols.append(volume_col)

    temp = df[use_cols].copy()
    columns = ["symbol", "open", "close"]
    if name_col:
        columns.insert(1, "name")
    if high_col:
        columns.append("high")
    if low_col:
        columns.append("low")
    if volume_col:
        columns.append("volume")

    temp.columns = columns
    if name_col:
        temp["name"] = temp["name"].astype(str).str.strip().replace({"nan": ""})
    else:
        temp["name"] = ""

    temp["symbol"] = temp["symbol"].astype(str).str.strip()
    temp["open"] = temp["open"].map(_clean_number)
    temp["close"] = temp["close"].map(_clean_number)
    if "high" in temp.columns:
        temp["high"] = temp["high"].map(_clean_number)
    else:
        temp["high"] = None
    if "low" in temp.columns:
        temp["low"] = temp["low"].map(_clean_number)
    else:
        temp["low"] = None
    if "volume" in temp.columns:
        temp["volume"] = temp["volume"].map(_clean_int)
    else:
        temp["volume"] = None
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
) -> tuple[pd.DataFrame | None, dt.date | None, pd.DataFrame | None, dt.date | None]:
    # Use V2 JSON API (supports historical queries natively)
    tpex_quotes_raw, tpex_quotes_date = fetch_tpex_daily_quotes_v2(session, date)
    tpex_quotes = _prepare_tpex_quotes(tpex_quotes_raw)

    tpex_3insti_raw, tpex_3insti_date = fetch_tpex_3insti_v2(session, date)
    tpex_3insti = _prepare_tpex_3insti(tpex_3insti_raw)

    if tpex_quotes_date != date:
        tpex_quotes = None
    if tpex_3insti_date != date:
        tpex_3insti = None

    return tpex_quotes, tpex_quotes_date, tpex_3insti, tpex_3insti_date


def _prepare_twse_sources(session: requests.Session, date: dt.date) -> pd.DataFrame:
    twse_t86 = fetch_twse_t86(session, date)
    return _prepare_twse_3insti(twse_t86)


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
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    total = len(holdings)
    for idx, item in holdings.iterrows():
        symbol = str(item["symbol"]).strip()
        name = str(item["name"]).strip()
        if name.lower() == "nan":
            name = ""
        display_name = f" {name}" if name else ""
        print(f"{date.isoformat()} {idx + 1}/{total} {symbol}{display_name}")

        open_price = None
        close_price = None
        high_price = None
        low_price = None
        volume = None
        foreign_net = None
        trust_net = None
        dealer_net = None

        if twse_day_all is not None:
            row_all = twse_day_all.loc[twse_day_all["symbol"] == symbol]
            if not row_all.empty:
                open_price = row_all.iloc[0]["open"]
                close_price = row_all.iloc[0]["close"]
                high_price = row_all.iloc[0].get("high")
                low_price = row_all.iloc[0].get("low")
                volume = row_all.iloc[0].get("volume")
                if not name and "name" in row_all.columns:
                    name = str(row_all.iloc[0].get("name", "")).strip()

        if (
            open_price is None
            or close_price is None
            or high_price is None
            or low_price is None
            or volume is None
        ):
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
                (
                    open_value,
                    high_value,
                    low_value,
                    close_value,
                    volume_value,
                ) = find_twse_ohlcv(twse_day, date)
                if open_price is None:
                    open_price = open_value
                if close_price is None:
                    close_price = close_value
                if high_price is None:
                    high_price = high_value
                if low_price is None:
                    low_price = low_value
                if volume is None:
                    volume = volume_value

        if (
            (open_price is None or close_price is None or high_price is None or low_price is None or volume is None)
            and twse_mi_index is not None
        ):
            row_mi = twse_mi_index.loc[twse_mi_index["symbol"] == symbol]
            if not row_mi.empty:
                if open_price is None:
                    open_price = row_mi.iloc[0]["open"]
                if close_price is None:
                    close_price = row_mi.iloc[0]["close"]
                if high_price is None:
                    high_price = row_mi.iloc[0].get("high")
                if low_price is None:
                    low_price = row_mi.iloc[0].get("low")
                if volume is None:
                    volume = row_mi.iloc[0].get("volume")
                if not name and "name" in row_mi.columns:
                    name = str(row_mi.iloc[0].get("name", "")).strip()

        if open_price is None and close_price is None:
            row = tpex_quotes.loc[tpex_quotes["symbol"] == symbol]
            if not row.empty:
                open_price = row.iloc[0]["open"]
                close_price = row.iloc[0]["close"]
                high_price = row.iloc[0].get("high")
                low_price = row.iloc[0].get("low")
                volume = row.iloc[0].get("volume")
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

        # Update volume history and compute moving averages
        vol_series = volume_history.get(symbol, pd.Series(dtype=float))
        if volume is not None:
            vol_series = pd.concat(
                [vol_series, pd.Series([float(volume)], index=pd.to_datetime([date]))]
            )
            vol_series = vol_series[~vol_series.index.duplicated(keep="last")].sort_index()
            volume_history[symbol] = vol_series

        vol_ma5 = vol_series.tail(5).mean() if len(vol_series) >= 5 else None
        vol_ma10 = vol_series.tail(10).mean() if len(vol_series) >= 10 else None

        rsi = compute_rsi(series).iloc[-1] if len(series) >= 14 else None
        macd, macd_signal, macd_hist = (
            compute_macd(series, fast=config.macd_fast, slow=config.macd_slow, signal=config.macd_signal)
            if len(series) >= 2
            else (pd.Series(dtype=float), pd.Series(dtype=float), pd.Series(dtype=float))
        )
        macd_value = macd.iloc[-1] if not macd.empty else None
        macd_signal_value = macd_signal.iloc[-1] if not macd_signal.empty else None
        macd_hist_value = macd_hist.iloc[-1] if not macd_hist.empty else None

        # Convert from shares to lots (張, 1 lot = 1000 shares)
        foreign_net_lots = foreign_net // 1000 if foreign_net is not None else None
        trust_net_lots = trust_net // 1000 if trust_net is not None else None
        dealer_net_lots = dealer_net // 1000 if dealer_net is not None else None
        insti_total_lots = (
            None
            if foreign_net_lots is None and trust_net_lots is None and dealer_net_lots is None
            else (foreign_net_lots or 0) + (trust_net_lots or 0) + (dealer_net_lots or 0)
        )

        rows.append(
            {
                "symbol": symbol,
                "name": name,
                "open": open_price,
                "close": close_price,
                "high": high_price,
                "low": low_price,
                "volume": volume,
                "vol_ma5": int(vol_ma5) if vol_ma5 is not None else None,
                "vol_ma10": int(vol_ma10) if vol_ma10 is not None else None,
                "foreign_net": foreign_net_lots,
                "trust_net": trust_net_lots,
                "dealer_net": dealer_net_lots,
                "institutional_investors_net": insti_total_lots,
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
    volume_history: dict[str, pd.Series],
    sheet_names: set[str],
    twse_month_cache: dict[tuple[str, dt.date], pd.DataFrame],
    config: AppConfig,
    today: dt.date,
    skip_existing: bool = False,
) -> bool:
    sheet_name = date.isoformat()
    print(f"開始處理日期 {sheet_name}")
    if date.weekday() >= 5:
        details = f"weekday={date.weekday()}"
        print(f"{sheet_name} 週末休市，略過寫入")
        write_market_closed_sheet(OUTPUT_FILE, date, "weekend", details)
        remove_sheet(OUTPUT_FILE, sheet_name)
        return False
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
    twse_day_all_date = None
    if date == today:
        try:
            twse_day_all_raw, twse_day_all_date = fetch_twse_stock_day_all(session)
            if twse_day_all_date is None:
                print(f"{sheet_name} TWSE STOCK_DAY_ALL 無法解析日期，略過使用")
            elif twse_day_all_date != date:
                print(
                    f"{sheet_name} TWSE STOCK_DAY_ALL 日期不匹配："
                    f"{twse_day_all_date} != {date}，略過使用"
                )
            else:
                twse_day_all = _prepare_twse_day_all(twse_day_all_raw)
        except DataUnavailableError as exc:
            print(f"{sheet_name} TWSE STOCK_DAY_ALL 取得失敗：{exc}")
            twse_day_all = None
            twse_day_all_date = None
        except requests.RequestException as exc:
            print(f"{sheet_name} TWSE STOCK_DAY_ALL 網路連線失敗：{exc}")
            twse_day_all = None
            twse_day_all_date = None

    twse_mi_index = None
    twse_mi_index_date = None
    try:
        twse_mi_index_raw, twse_mi_index_date = fetch_twse_mi_index(session, date)
        if twse_mi_index_date is None and not twse_mi_index_raw.empty and date == today:
            print(f"{sheet_name} TWSE MI_INDEX 無法解析日期，假設為 {date}")
            twse_mi_index_date = date
        if twse_mi_index_date is None:
            print(f"{sheet_name} TWSE MI_INDEX 無法解析日期，略過使用")
        elif twse_mi_index_date != date:
            print(
                f"{sheet_name} TWSE MI_INDEX 日期不匹配："
                f"{twse_mi_index_date} != {date}，略過使用"
            )
        else:
            twse_mi_index = _prepare_twse_mi_index(twse_mi_index_raw)
    except DataUnavailableError as exc:
        print(f"{sheet_name} TWSE MI_INDEX 取得失敗：{exc}")
        twse_mi_index = None
        twse_mi_index_date = None
    except requests.RequestException as exc:
        print(f"{sheet_name} TWSE MI_INDEX 網路連線失敗：{exc}")
        twse_mi_index = None
        twse_mi_index_date = None

    twse_confirmed = (
        (twse_day_all_date == date)
        or (twse_mi_index_date == date)
        or (not twse_3insti.empty)
    )
    if not twse_confirmed:
        details = (
            f"twse_day_all={twse_day_all_date}, "
            f"twse_mi_index={twse_mi_index_date}, "
            f"twse_t86_rows={len(twse_3insti)}"
        )
        print(f"{sheet_name} TWSE 資料不足，視為休市，略過寫入")
        write_market_closed_sheet(OUTPUT_FILE, date, "twse_unavailable", details)
        remove_sheet(OUTPUT_FILE, sheet_name)
        return False

    try:
        (
            tpex_quotes,
            tpex_quotes_date,
            tpex_3insti,
            tpex_3insti_date,
        ) = _prepare_tpex_sources(session, date)
        if tpex_quotes_date is None:
            print(f"{sheet_name} TPEX 日行情無法解析日期，略過使用")
        elif tpex_quotes_date != date:
            print(
                f"{sheet_name} TPEX 日行情日期不匹配："
                f"{tpex_quotes_date} != {date}，略過使用"
            )
        if tpex_3insti_date is None:
            print(f"{sheet_name} TPEX 三大法人無法解析日期，略過使用")
        elif tpex_3insti_date != date:
            print(
                f"{sheet_name} TPEX 三大法人日期不匹配："
                f"{tpex_3insti_date} != {date}，略過使用"
            )
    except DataUnavailableError as exc:
        print(f"{sheet_name} TPEX 資料尚未公告或取得失敗：{exc}")
        tpex_quotes = None
        tpex_quotes_date = None
        tpex_3insti = None
        tpex_3insti_date = None
    except requests.RequestException as exc:
        print(f"{sheet_name} TPEX 網路連線失敗：{exc}")
        return False

    if tpex_quotes is None:
        tpex_quotes = pd.DataFrame(
            columns=["symbol", "name", "open", "close", "high", "low", "volume"]
        )
    if tpex_3insti is None:
        tpex_3insti = pd.DataFrame(
            columns=["symbol", "name", "foreign_net", "trust_net", "dealer_net"]
        )

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


def _build_alpha_sheet(config: AppConfig, target_date: dt.date) -> None:
    """Analyse recent trading data and write alpha picks to alpha_pick.xlsx."""
    if not OUTPUT_FILE.exists():
        print("尚無每日資料，無法產生 alpha 分析。")
        return

    xls = pd.ExcelFile(OUTPUT_FILE)
    date_sheets = sorted([s for s in xls.sheet_names if s != "market_closed"], reverse=True)
    if not date_sheets:
        print("尚無每日交易資料，無法產生 alpha 分析。")
        return

    long_n = config.alpha_insti_days_long
    short_n = config.alpha_insti_days_short
    needed_sheets = date_sheets[:long_n]

    # Load recent sheets into a dict: {date_str: DataFrame}
    recent: dict[str, pd.DataFrame] = {}
    for s in needed_sheets:
        recent[s] = xls.parse(s)

    latest_sheet = date_sheets[0]
    latest_df = recent[latest_sheet]
    if "symbol" not in latest_df.columns:
        print("最新 sheet 缺少 symbol 欄位，無法分析。")
        return

    symbols = latest_df["symbol"].astype(str).str.strip().tolist()
    short_sheets = date_sheets[:short_n]
    long_sheets = date_sheets[:long_n]

    rows: list[dict] = []
    for sym in symbols:
        row_latest = latest_df[latest_df["symbol"].astype(str).str.strip() == sym]
        if row_latest.empty:
            continue
        r = row_latest.iloc[0]
        name = str(r.get("name", "")).strip()
        close = r.get("close")
        rsi = r.get("rsi_14")
        macd = r.get("macd")
        macd_signal = r.get("macd_signal")
        macd_hist = r.get("macd_hist")

        # Collect institutional net across recent sheets
        insti_short: list[float] = []
        for s in short_sheets:
            df = recent.get(s)
            if df is None:
                continue
            row = df[df["symbol"].astype(str).str.strip() == sym]
            if row.empty:
                continue
            val = row.iloc[0].get("institutional_investors_net")
            if pd.notna(val):
                insti_short.append(float(val))

        insti_long: list[float] = []
        for s in long_sheets:
            df = recent.get(s)
            if df is None:
                continue
            row = df[df["symbol"].astype(str).str.strip() == sym]
            if row.empty:
                continue
            val = row.iloc[0].get("institutional_investors_net")
            if pd.notna(val):
                insti_long.append(float(val))

        insti_short_sum = sum(insti_short) if insti_short else None
        insti_short_avg = (insti_short_sum / len(insti_short)) if insti_short else None
        insti_long_avg = (sum(insti_long) / len(insti_long)) if insti_long else None

        # --- Condition 1: institutional momentum ---
        cond_insti = (
            insti_short_sum is not None
            and insti_long_avg is not None
            and insti_short_avg is not None
            and insti_short_sum > 0
            and insti_short_avg > insti_long_avg
        )

        # --- Condition 2: RSI in healthy range ---
        cond_rsi = (
            rsi is not None
            and not pd.isna(rsi)
            and config.alpha_rsi_min <= float(rsi) <= config.alpha_rsi_max
        )

        # --- Condition 3: MACD histogram bullish ---
        cond_macd = (
            macd_hist is not None
            and not pd.isna(macd_hist)
            and float(macd_hist) > config.alpha_macd_hist_min
        )

        # --- Condition 4: Volume breakout (5-day MA) ---
        volume = r.get("volume")
        vol_ma5 = r.get("vol_ma5")
        vol_ma10 = r.get("vol_ma10")
        cond_vol_ma5 = (
            volume is not None
            and vol_ma5 is not None
            and not pd.isna(volume)
            and not pd.isna(vol_ma5)
            and float(volume) > float(vol_ma5)
        )

        # --- Condition 5: Volume breakout (10-day MA) ---
        cond_vol_ma10 = (
            volume is not None
            and vol_ma10 is not None
            and not pd.isna(volume)
            and not pd.isna(vol_ma10)
            and float(volume) > float(vol_ma10)
        )

        if not (cond_insti or cond_rsi or cond_macd or cond_vol_ma5 or cond_vol_ma10):
            continue

        reasons: list[str] = []
        if cond_insti:
            reasons.append(
                f"法人加碼：近{len(insti_short)}日淨買超合計"
                f"{insti_short_sum:+,.0f}，"
                f"日均{insti_short_avg:+,.0f} > "
                f"近{len(insti_long)}日均{insti_long_avg:+,.0f}"
            )
        if cond_rsi:
            reasons.append(f"RSI 健康：{float(rsi):.1f}（區間 {config.alpha_rsi_min}-{config.alpha_rsi_max}）")
        if cond_macd:
            reasons.append(f"MACD 多方：histogram {float(macd_hist):+.2f}")
        if cond_vol_ma5:
            reasons.append(f"量突破5MA：{int(volume):,} > {int(vol_ma5):,}")
        if cond_vol_ma10:
            reasons.append(f"量突破10MA：{int(volume):,} > {int(vol_ma10):,}")

        rows.append({
            "symbol": sym,
            "name": name,
            "close": close,
            "volume": volume,
            "vol_ma5": vol_ma5,
            "vol_ma10": vol_ma10,
            "rsi_14": round(float(rsi), 2) if pd.notna(rsi) else None,
            "macd": round(float(macd), 2) if pd.notna(macd) else None,
            "macd_signal": round(float(macd_signal), 2) if pd.notna(macd_signal) else None,
            "macd_hist": round(float(macd_hist), 2) if pd.notna(macd_hist) else None,
            f"insti_net_{short_n}d_sum": insti_short_sum,
            f"insti_net_{short_n}d_avg": round(insti_short_avg, 0) if insti_short_avg is not None else None,
            f"insti_net_{long_n}d_avg": round(insti_long_avg, 0) if insti_long_avg is not None else None,
            "cond_insti": cond_insti,
            "cond_rsi": cond_rsi,
            "cond_macd": cond_macd,
            "cond_vol_ma5": cond_vol_ma5,
            "cond_vol_ma10": cond_vol_ma10,
            "reasons": "；".join(reasons),
        })

    if not rows:
        print("未找到符合 alpha 條件的股票。")
        return

    alpha_df = pd.DataFrame(rows)
    sheet_name = f"alpha_{target_date.isoformat()}"

    if ALPHA_FILE.exists():
        with pd.ExcelWriter(
            ALPHA_FILE, engine="openpyxl", mode="a", if_sheet_exists="replace"
        ) as writer:
            alpha_df.to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        with pd.ExcelWriter(ALPHA_FILE, engine="openpyxl", mode="w") as writer:
            alpha_df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Alpha 分析已寫入 {ALPHA_FILE} ({sheet_name})，共 {len(rows)} 檔")


def main() -> None:
    load_dotenv()
    config = AppConfig.from_env()
    args = _parse_args()
    today = dt.datetime.now(TAIPEI_TZ).date()
    target_date = _parse_date(args.date) if args.date else today

    session = requests.Session()
    session.headers.update({"User-Agent": "tw-stock-daily/0.1"})

    if not config.extra_stocks:
        print("請在 .env 設定 STOCKS（逗號分隔的股票代號）")
        return

    holdings = pd.DataFrame([{"symbol": s, "name": ""} for s in config.extra_stocks])

    history, volume_history = load_history(OUTPUT_FILE)
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
                volume_history=volume_history,
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
                volume_history=volume_history,
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
            volume_history=volume_history,
            sheet_names=sheet_names,
            twse_month_cache=twse_month_cache,
            config=config,
            today=today,
            skip_existing=False,
        )

    _build_alpha_sheet(config, target_date)


if __name__ == "__main__":
    main()
