"""Data preparation and normalization functions for stock data."""

from __future__ import annotations

import re

import pandas as pd

from .sources import _clean_int, _clean_number, DataUnavailableError


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
