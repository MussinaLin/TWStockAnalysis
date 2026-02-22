"""Data preparation and normalization functions for stock data."""

from __future__ import annotations

import re

import pandas as pd

import datetime as dt

from .sources import _clean_int, _clean_number, _parse_roc_date, DataUnavailableError


def _normalize_col(text: str) -> str:
    """Normalize column name by removing BOM, whitespace, and lowercasing."""
    cleaned = text.replace("\ufeff", "")
    cleaned = re.sub(r"\s+", "", cleaned)
    return cleaned.lower()


def _find_column(df: pd.DataFrame, keywords: list[str]) -> str | None:
    """Find column that contains all keywords (normalized)."""
    normalized_keywords = [_normalize_col(keyword) for keyword in keywords]
    for col in df.columns:
        text = _normalize_col(str(col))
        if all(keyword in text for keyword in normalized_keywords):
            return col
    return None


def _find_columns(df: pd.DataFrame, col_specs: dict[str, list[list[str]]]) -> dict[str, str | None]:
    """Find multiple columns based on spec dict.

    Args:
        df: DataFrame to search
        col_specs: Dict mapping output name to list of keyword alternatives
                   e.g. {"symbol": [["證券代號"], ["代號"]], "open": [["開盤"], ["開盤價"]]}

    Returns:
        Dict mapping output name to found column name (or None)
    """
    result = {}
    for name, alternatives in col_specs.items():
        found = None
        for keywords in alternatives:
            found = _find_column(df, keywords)
            if found:
                break
        result[name] = found
    return result


def _extract_standard_columns(
    df: pd.DataFrame,
    cols: dict[str, str | None],
    required: list[str],
    error_msg: str,
) -> pd.DataFrame:
    """Extract and rename columns to standard names.

    Args:
        df: Source DataFrame
        cols: Mapping from standard name to source column name
        required: List of required standard names
        error_msg: Error message if required columns missing

    Returns:
        DataFrame with standardized column names
    """
    # Check required columns
    missing = [r for r in required if not cols.get(r)]
    if missing:
        available = ", ".join([str(c) for c in df.columns[:10]])
        raise DataUnavailableError(f"{error_msg}，缺少 {missing}，可用欄位={available}")

    # Build column mapping (only non-None)
    use_cols = []
    rename_map = {}
    for std_name, src_col in cols.items():
        if src_col:
            use_cols.append(src_col)
            rename_map[src_col] = std_name

    temp = df[use_cols].copy()
    temp = temp.rename(columns=rename_map)
    return temp


def prepare_tpex_quotes(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare TPEX daily quotes into standard format."""
    cols = _find_columns(df, {
        "symbol": [["證券代號"], ["代號"]],
        "name": [["名稱"]],
        "open": [["開盤"], ["開盤價"]],
        "close": [["收盤"], ["收盤價"]],
        "high": [["最高"], ["最高價"]],
        "low": [["最低"], ["最低價"]],
        "volume": [["成交股數"], ["成交量"]],
    })

    temp = _extract_standard_columns(
        df, cols, required=["symbol", "open", "close"],
        error_msg="TPEX 行情欄位解析失敗"
    )

    # Clean and convert
    if "name" in temp.columns:
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


def prepare_tpex_3insti(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare TPEX institutional investors data into standard format."""
    cols = _find_columns(df, {
        "symbol": [["證券代號"], ["代號"]],
        "name": [["名稱"]],
        "foreign_net": [["外資", "買賣超"], ["外資合計買賣超"]],
        "trust_net": [["投信", "買賣超"]],
        "dealer_net": [["自營商", "買賣超"], ["自營商合計買賣超"]],
    })

    temp = _extract_standard_columns(
        df, cols, required=["symbol", "foreign_net", "trust_net", "dealer_net"],
        error_msg="TPEX 三大法人欄位解析失敗"
    )

    if "name" in temp.columns:
        temp["name"] = temp["name"].astype(str).str.strip().replace({"nan": ""})
    else:
        temp["name"] = ""
    temp["symbol"] = temp["symbol"].astype(str).str.strip()
    temp["foreign_net"] = temp["foreign_net"].map(_clean_int)
    temp["trust_net"] = temp["trust_net"].map(_clean_int)
    temp["dealer_net"] = temp["dealer_net"].map(_clean_int)

    return temp


def prepare_twse_3insti(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare TWSE institutional investors data into standard format."""
    cols = _find_columns(df, {
        "symbol": [["證券代號"], ["代號"]],
        "name": [["名稱"]],
        "foreign_net": [["外陸資", "買賣超"], ["外資", "買賣超"]],
        "trust_net": [["投信", "買賣超"]],
        "dealer_net": [["自營商買賣超"], ["自營商", "買賣超"]],
    })

    temp = _extract_standard_columns(
        df, cols, required=["symbol", "foreign_net", "trust_net", "dealer_net"],
        error_msg="TWSE 三大法人欄位解析失敗"
    )

    if "name" in temp.columns:
        temp["name"] = temp["name"].astype(str).str.strip().replace({"nan": ""})
    else:
        temp["name"] = ""
    temp["symbol"] = temp["symbol"].astype(str).str.strip()
    temp["foreign_net"] = temp["foreign_net"].map(_clean_int)
    temp["trust_net"] = temp["trust_net"].map(_clean_int)
    temp["dealer_net"] = temp["dealer_net"].map(_clean_int)

    return temp


def prepare_twse_day_all(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare TWSE STOCK_DAY_ALL data into standard format."""
    cols = _find_columns(df, {
        "symbol": [["code"], ["證券代號"], ["代號"]],
        "name": [["name"], ["證券名稱"], ["名稱"]],
        "open": [["openingprice"], ["open"], ["開盤價"], ["開盤"]],
        "close": [["closingprice"], ["close"], ["收盤價"], ["收盤"]],
        "high": [["highestprice"], ["high"], ["最高價"], ["最高"]],
        "low": [["lowestprice"], ["low"], ["最低價"], ["最低"]],
        "volume": [["tradevolume"], ["成交股數"], ["成交量"]],
    })

    temp = _extract_standard_columns(
        df, cols, required=["symbol", "open", "close"],
        error_msg="TWSE STOCK_DAY_ALL 欄位解析失敗"
    )

    if "name" in temp.columns:
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


def prepare_twse_mi_index(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare TWSE MI_INDEX data into standard format."""
    cols = _find_columns(df, {
        "symbol": [["證券代號"], ["代號"]],
        "name": [["證券名稱"], ["名稱"]],
        "open": [["開盤價"], ["開盤"]],
        "close": [["收盤價"], ["收盤"]],
        "high": [["最高價"], ["最高"]],
        "low": [["最低價"], ["最低"]],
        "volume": [["成交股數"], ["成交量"]],
    })

    temp = _extract_standard_columns(
        df, cols, required=["symbol", "open", "close"],
        error_msg="TWSE MI_INDEX 欄位解析失敗"
    )

    if "name" in temp.columns:
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


def prepare_twse_issued_shares(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare TWSE company basic data to extract issued shares.

    Returns DataFrame with columns: symbol, name, issued_shares
    """
    cols = _find_columns(df, {
        "symbol": [["公司代號"], ["代號"]],
        "name": [["公司簡稱"], ["公司名稱"], ["名稱"]],
        "issued_shares": [["已發行普通股數"], ["發行股數"]],
        "paid_in_capital": [["實收資本額"]],
        "par_value": [["普通股每股面額"], ["每股面額"]],
    })

    # Try to get issued shares directly, or calculate from capital/par value
    symbol_col = cols.get("symbol")
    name_col = cols.get("name")
    issued_col = cols.get("issued_shares")
    capital_col = cols.get("paid_in_capital")
    par_col = cols.get("par_value")

    if not symbol_col:
        raise DataUnavailableError("TWSE 公司基本資料缺少代號欄位")

    result = pd.DataFrame()
    result["symbol"] = df[symbol_col].astype(str).str.strip()

    if name_col:
        result["name"] = df[name_col].astype(str).str.strip()
    else:
        result["name"] = ""

    if issued_col:
        result["issued_shares"] = df[issued_col].map(_clean_int)
    elif capital_col and par_col:
        # Calculate: issued_shares = paid_in_capital / par_value
        def _extract_par_value(val):
            if pd.isna(val):
                return None
            text = str(val)
            # Extract number from "新台幣 10.0000元"
            match = re.search(r"([\d.]+)", text)
            if match:
                return float(match.group(1))
            return _clean_number(text)

        capital = df[capital_col].map(_clean_int)
        par = df[par_col].map(_extract_par_value)
        result["issued_shares"] = (capital / par).map(
            lambda x: int(x) if pd.notna(x) else None
        )
    else:
        raise DataUnavailableError("TWSE 公司基本資料缺少發行股數或資本額/面額欄位")

    return result.dropna(subset=["issued_shares"])


def prepare_tpex_issued_shares(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare TPEX company basic data to extract issued shares.

    Returns DataFrame with columns: symbol, name, issued_shares
    """
    # TPEX JSON API uses English field names
    symbol_col = None
    name_col = None
    issued_col = None

    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ("securitiescompanycode", "companycode", "code"):
            symbol_col = col
        elif col_lower in ("companyabbreviation", "companyname"):
            name_col = col
        elif col_lower == "issueshares":
            issued_col = col

    if not symbol_col:
        # Fallback to Chinese column names
        cols = _find_columns(df, {
            "symbol": [["公司代號"], ["代號"]],
            "name": [["公司簡稱"], ["公司名稱"], ["名稱"]],
            "issued_shares": [["已發行普通股數"], ["發行股數"]],
        })
        symbol_col = cols.get("symbol")
        name_col = cols.get("name")
        issued_col = cols.get("issued_shares")

    if not symbol_col:
        raise DataUnavailableError("TPEX 公司基本資料缺少代號欄位")
    if not issued_col:
        raise DataUnavailableError("TPEX 公司基本資料缺少發行股數欄位")

    result = pd.DataFrame()
    result["symbol"] = df[symbol_col].astype(str).str.strip()
    if name_col:
        result["name"] = df[name_col].astype(str).str.strip()
    else:
        result["name"] = ""
    result["issued_shares"] = df[issued_col].map(_clean_int)

    return result.dropna(subset=["issued_shares"])


def prepare_twse_margin(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare TWSE margin trading data into standard format.

    Columns: symbol, margin_buy, margin_sell, margin_balance, margin_change,
             short_sell, short_buy, short_balance, short_change
    Units: Input is in shares (股), output is in lots (張, ÷1000)
    """
    cols = _find_columns(df, {
        "symbol": [["股票代號"], ["代號"]],
        "margin_buy": [["融資買進"]],
        "margin_sell": [["融資賣出"]],
        "margin_cash_repay": [["融資現金償還"]],
        "margin_balance": [["融資今日餘額"], ["融資餘額"]],
        "short_sell": [["融券賣出"]],
        "short_buy": [["融券買進"]],
        "short_stock_repay": [["融券現券償還"]],
        "short_balance": [["融券今日餘額"], ["融券餘額"]],
    })

    symbol_col = cols.get("symbol")
    if not symbol_col:
        raise DataUnavailableError("TWSE 融資融券欄位解析失敗，缺少 symbol")

    result = pd.DataFrame()
    result["symbol"] = df[symbol_col].astype(str).str.strip()

    # Helper to convert shares to lots (張)
    def _shares_to_lots(col_name: str) -> pd.Series:
        src_col = cols.get(col_name)
        if src_col:
            values = df[src_col].map(_clean_int)
            return values.map(lambda x: x // 1000 if x is not None else None)
        return pd.Series([None] * len(df))

    result["margin_buy"] = _shares_to_lots("margin_buy")
    result["margin_sell"] = _shares_to_lots("margin_sell")
    result["margin_balance"] = _shares_to_lots("margin_balance")
    result["short_sell"] = _shares_to_lots("short_sell")
    result["short_buy"] = _shares_to_lots("short_buy")
    result["short_balance"] = _shares_to_lots("short_balance")

    # Calculate margin_change: buy - sell - cash_repay
    margin_buy = df[cols["margin_buy"]].map(_clean_int) if cols.get("margin_buy") else None
    margin_sell = df[cols["margin_sell"]].map(_clean_int) if cols.get("margin_sell") else None
    margin_cash = df[cols["margin_cash_repay"]].map(_clean_int) if cols.get("margin_cash_repay") else None

    if margin_buy is not None and margin_sell is not None:
        margin_change = margin_buy - margin_sell
        if margin_cash is not None:
            margin_change = margin_change - margin_cash.fillna(0)
        result["margin_change"] = margin_change.map(lambda x: int(x // 1000) if pd.notna(x) else None)
    else:
        result["margin_change"] = None

    # Calculate short_change: sell - buy - stock_repay
    short_sell_raw = df[cols["short_sell"]].map(_clean_int) if cols.get("short_sell") else None
    short_buy_raw = df[cols["short_buy"]].map(_clean_int) if cols.get("short_buy") else None
    short_stock = df[cols["short_stock_repay"]].map(_clean_int) if cols.get("short_stock_repay") else None

    if short_sell_raw is not None and short_buy_raw is not None:
        short_change = short_sell_raw - short_buy_raw
        if short_stock is not None:
            short_change = short_change - short_stock.fillna(0)
        result["short_change"] = short_change.map(lambda x: int(x // 1000) if pd.notna(x) else None)
    else:
        result["short_change"] = None

    return result


def prepare_tpex_margin(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare TPEX margin trading data into standard format.

    Columns: symbol, margin_buy, margin_sell, margin_balance, margin_change,
             short_sell, short_buy, short_balance, short_change
    Units: Input is in shares (股), output is in lots (張, ÷1000)
    """
    # TPEX uses English column names
    col_mapping = {
        "symbol": "SecuritiesCompanyCode",
        "margin_buy": "MarginPurchase",
        "margin_sell": "MarginSales",
        "margin_cash_repay": "CashRedemption",
        "margin_balance": "MarginPurchaseBalance",
        "short_sell": "ShortSale",
        "short_buy": "ShortCovering",
        "short_stock_repay": "StockRedemption",
        "short_balance": "ShortSaleBalance",
    }

    # Find actual column names (case insensitive)
    df_cols_lower = {c.lower(): c for c in df.columns}
    cols = {}
    for std_name, tpex_name in col_mapping.items():
        actual_col = df_cols_lower.get(tpex_name.lower())
        cols[std_name] = actual_col

    symbol_col = cols.get("symbol")
    if not symbol_col:
        raise DataUnavailableError("TPEX 融資融券欄位解析失敗，缺少 symbol")

    result = pd.DataFrame()
    result["symbol"] = df[symbol_col].astype(str).str.strip()

    # Helper to convert shares to lots (張)
    def _shares_to_lots(col_name: str) -> pd.Series:
        src_col = cols.get(col_name)
        if src_col and src_col in df.columns:
            values = df[src_col].map(_clean_int)
            return values.map(lambda x: x // 1000 if x is not None else None)
        return pd.Series([None] * len(df))

    result["margin_buy"] = _shares_to_lots("margin_buy")
    result["margin_sell"] = _shares_to_lots("margin_sell")
    result["margin_balance"] = _shares_to_lots("margin_balance")
    result["short_sell"] = _shares_to_lots("short_sell")
    result["short_buy"] = _shares_to_lots("short_buy")
    result["short_balance"] = _shares_to_lots("short_balance")

    # Calculate margin_change: buy - sell - cash_repay
    margin_buy_col = cols.get("margin_buy")
    margin_sell_col = cols.get("margin_sell")
    margin_cash_col = cols.get("margin_cash_repay")

    if margin_buy_col and margin_sell_col:
        margin_buy = df[margin_buy_col].map(_clean_int)
        margin_sell = df[margin_sell_col].map(_clean_int)
        margin_change = margin_buy - margin_sell
        if margin_cash_col and margin_cash_col in df.columns:
            margin_cash = df[margin_cash_col].map(_clean_int)
            margin_change = margin_change - margin_cash.fillna(0)
        result["margin_change"] = margin_change.map(lambda x: int(x // 1000) if pd.notna(x) else None)
    else:
        result["margin_change"] = None

    # Calculate short_change: sell - buy - stock_repay
    short_sell_col = cols.get("short_sell")
    short_buy_col = cols.get("short_buy")
    short_stock_col = cols.get("short_stock_repay")

    if short_sell_col and short_buy_col:
        short_sell_raw = df[short_sell_col].map(_clean_int)
        short_buy_raw = df[short_buy_col].map(_clean_int)
        short_change = short_sell_raw - short_buy_raw
        if short_stock_col and short_stock_col in df.columns:
            short_stock = df[short_stock_col].map(_clean_int)
            short_change = short_change - short_stock.fillna(0)
        result["short_change"] = short_change.map(lambda x: int(x // 1000) if pd.notna(x) else None)
    else:
        result["short_change"] = None

    return result


def prepare_moneydj_margin(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare MoneyDJ margin trading data into standard format.

    Input DataFrame from fetch_moneydj_margin with columns:
    date, margin_buy, margin_sell, margin_balance, margin_change,
    short_sell, short_buy, short_balance, short_change

    Returns DataFrame with same columns but with parsed dates and cleaned integers.
    Units: lots/張 (already in lots from MoneyDJ)
    """
    if "date" not in df.columns:
        raise DataUnavailableError("MoneyDJ 融資融券欄位解析失敗，缺少 date")

    result = pd.DataFrame()

    # Parse ROC dates (民國, e.g., 115/02/11) to gregorian
    def _parse_moneydj_date(val) -> dt.date | None:
        if pd.isna(val):
            return None
        text = str(val).strip()
        if not text:
            return None
        return _parse_roc_date(text)

    result["date"] = df["date"].map(_parse_moneydj_date)

    # MoneyDJ values are already in lots (張), no conversion needed
    for col_name in ["margin_buy", "margin_sell", "margin_balance", "margin_change",
                     "short_sell", "short_buy", "short_balance", "short_change"]:
        if col_name in df.columns:
            result[col_name] = df[col_name].map(_clean_int)
        else:
            result[col_name] = None

    # 券資比 comes as percentage string like "1.25%", convert to float
    if "short_margin_ratio" in df.columns:
        def _parse_percent(val):
            if pd.isna(val):
                return None
            text = str(val).strip().replace("%", "")
            try:
                return float(text)
            except ValueError:
                return None
        result["short_margin_ratio"] = df["short_margin_ratio"].map(_parse_percent)
    else:
        result["short_margin_ratio"] = None

    # Drop rows with invalid dates
    result = result.dropna(subset=["date"])

    return result


def prepare_moneydj_holding_pct(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare MoneyDJ institutional holding percentage data.

    Input DataFrame from fetch_moneydj_holding_pct with columns:
    date, foreign_holding_pct, insti_holding_pct

    Returns DataFrame with parsed dates and percentages as decimals (e.g., 0.3503).
    """
    if "date" not in df.columns:
        raise DataUnavailableError("MoneyDJ 法人持股欄位解析失敗，缺少 date")

    result = pd.DataFrame()

    def _parse_moneydj_date(val) -> dt.date | None:
        if pd.isna(val):
            return None
        text = str(val).strip()
        if not text:
            return None
        return _parse_roc_date(text)

    result["date"] = df["date"].map(_parse_moneydj_date)

    # Parse percentage strings like "35.03%" to decimal 0.3503
    def _parse_pct_to_decimal(val):
        if pd.isna(val):
            return None
        text = str(val).strip().replace("%", "")
        try:
            return round(float(text) / 100, 6)
        except ValueError:
            return None

    for col in ["foreign_holding_pct", "insti_holding_pct"]:
        if col in df.columns:
            result[col] = df[col].map(_parse_pct_to_decimal)
        else:
            result[col] = None

    result = result.dropna(subset=["date"])

    return result
