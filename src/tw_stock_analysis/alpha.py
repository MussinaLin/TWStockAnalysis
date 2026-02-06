"""Alpha stock picking analysis module."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd

from .config import AppConfig


def build_alpha_sheet(
    config: AppConfig,
    target_date: dt.date,
    daily_file: Path,
    alpha_file: Path,
) -> None:
    """Analyse recent trading data and write alpha picks to Excel.

    Args:
        config: Application configuration
        target_date: Target date for analysis
        daily_file: Path to daily stock data Excel file
        alpha_file: Path to output alpha picks Excel file
    """
    if not daily_file.exists():
        print("尚無每日資料，無法產生 alpha 分析。")
        return

    xls = pd.ExcelFile(daily_file)
    date_sheets = sorted(
        [s for s in xls.sheet_names if s != "market_closed"],
        reverse=True
    )
    if not date_sheets:
        print("尚無每日交易資料，無法產生 alpha 分析。")
        return

    long_n = config.alpha_insti_days_long
    short_n = config.alpha_insti_days_short
    bb_long_n = config.bb_narrow_long_days
    # Need enough sheets for both insti and BB narrow analysis
    max_needed = max(long_n, bb_long_n)
    needed_sheets = date_sheets[:max_needed]

    # Load recent sheets
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
    bb_short_sheets = date_sheets[:config.bb_narrow_short_days]
    bb_long_sheets = date_sheets[:config.bb_narrow_long_days]

    rows: list[dict] = []
    for sym in symbols:
        row_data = _analyze_symbol(
            sym, latest_df, recent, short_sheets, long_sheets,
            bb_short_sheets, bb_long_sheets, config
        )
        if row_data:
            rows.append(row_data)

    if not rows:
        print("未找到符合 alpha 條件的股票。")
        return

    alpha_df = pd.DataFrame(rows)
    sheet_name = f"alpha_{target_date.isoformat()}"

    if alpha_file.exists():
        with pd.ExcelWriter(
            alpha_file, engine="openpyxl", mode="a", if_sheet_exists="replace"
        ) as writer:
            alpha_df.to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        with pd.ExcelWriter(alpha_file, engine="openpyxl", mode="w") as writer:
            alpha_df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Alpha 分析已寫入 {alpha_file} ({sheet_name})，共 {len(rows)} 檔")


def _analyze_symbol(
    sym: str,
    latest_df: pd.DataFrame,
    recent: dict[str, pd.DataFrame],
    short_sheets: list[str],
    long_sheets: list[str],
    bb_short_sheets: list[str],
    bb_long_sheets: list[str],
    config: AppConfig,
) -> dict | None:
    """Analyze a single symbol for alpha conditions.

    Returns:
        Dict with analysis data if any condition met, None otherwise.
    """
    row_latest = latest_df[latest_df["symbol"].astype(str).str.strip() == sym]
    if row_latest.empty:
        return None

    r = row_latest.iloc[0]
    name = str(r.get("name", "")).strip()
    close = r.get("close")
    rsi = r.get("rsi_14")
    macd = r.get("macd")
    macd_signal = r.get("macd_signal")
    macd_hist = r.get("macd_hist")
    volume = r.get("volume")
    vol_ma5 = r.get("vol_ma5")
    vol_ma10 = r.get("vol_ma10")
    bb_upper = r.get("bb_upper")
    bb_bandwidth = r.get("bb_bandwidth")
    bb_percent_b = r.get("bb_percent_b")

    # Collect institutional net across recent sheets
    insti_short = _collect_values(sym, recent, short_sheets, "institutional_investors_net")
    insti_long = _collect_values(sym, recent, long_sheets, "institutional_investors_net")

    insti_short_sum = sum(insti_short) if insti_short else None
    insti_short_avg = (insti_short_sum / len(insti_short)) if insti_short else None
    insti_long_avg = (sum(insti_long) / len(insti_long)) if insti_long else None

    # Collect BB bandwidth across recent sheets
    bb_bw_short = _collect_values(sym, recent, bb_short_sheets, "bb_bandwidth")
    bb_bw_long = _collect_values(sym, recent, bb_long_sheets, "bb_bandwidth")

    bb_bw_short_avg = (sum(bb_bw_short) / len(bb_bw_short)) if bb_bw_short else None
    bb_bw_long_avg = (sum(bb_bw_long) / len(bb_bw_long)) if bb_bw_long else None

    short_n = config.alpha_insti_days_short
    long_n = config.alpha_insti_days_long
    bb_short_n = config.bb_narrow_short_days
    bb_long_n = config.bb_narrow_long_days

    # Evaluate conditions
    cond_insti = (
        insti_short_sum is not None
        and insti_long_avg is not None
        and insti_short_avg is not None
        and insti_short_sum > 0
        and insti_short_avg > insti_long_avg
    )

    cond_rsi = (
        rsi is not None
        and not pd.isna(rsi)
        and config.alpha_rsi_min <= float(rsi) <= config.alpha_rsi_max
    )

    cond_macd = (
        macd_hist is not None
        and not pd.isna(macd_hist)
        and float(macd_hist) > config.alpha_macd_hist_min
    )

    vol_ratio = config.vol_breakout_ratio

    cond_vol_ma5 = (
        volume is not None
        and vol_ma5 is not None
        and not pd.isna(volume)
        and not pd.isna(vol_ma5)
        and float(volume) > float(vol_ma5) * vol_ratio
    )

    cond_vol_ma10 = (
        volume is not None
        and vol_ma10 is not None
        and not pd.isna(volume)
        and not pd.isna(vol_ma10)
        and float(volume) > float(vol_ma10) * vol_ratio
    )

    cond_bb_narrow = (
        bb_bw_short_avg is not None
        and bb_bw_long_avg is not None
        and bb_bw_short_avg < bb_bw_long_avg
    )

    cond_bb_near_upper = (
        bb_percent_b is not None
        and not pd.isna(bb_percent_b)
        and float(bb_percent_b) > config.bb_percent_b_min
    )

    if not (cond_insti or cond_rsi or cond_macd or cond_vol_ma5 or cond_vol_ma10
            or cond_bb_narrow or cond_bb_near_upper):
        return None

    # Build reasons
    reasons: list[str] = []
    if cond_insti:
        reasons.append(
            f"法人加碼：近{len(insti_short)}日淨買超合計"
            f"{insti_short_sum:+,.0f}，"
            f"日均{insti_short_avg:+,.0f} > "
            f"近{len(insti_long)}日均{insti_long_avg:+,.0f}"
        )
    if cond_rsi:
        reasons.append(
            f"RSI 健康：{float(rsi):.1f}（區間 {config.alpha_rsi_min}-{config.alpha_rsi_max}）"
        )
    if cond_macd:
        reasons.append(f"MACD 多方：histogram {float(macd_hist):+.2f}")
    if cond_vol_ma5:
        reasons.append(f"量突破5MA：{int(volume):,} > {int(vol_ma5):,}×{vol_ratio}")
    if cond_vol_ma10:
        reasons.append(f"量突破10MA：{int(volume):,} > {int(vol_ma10):,}×{vol_ratio}")
    if cond_bb_narrow:
        reasons.append(
            f"布林收窄：近{bb_short_n}日BW均{bb_bw_short_avg:.4f} < "
            f"近{bb_long_n}日BW均{bb_bw_long_avg:.4f}"
        )
    if cond_bb_near_upper:
        reasons.append(
            f"接近布林上軌：%B={float(bb_percent_b):.2f} > {config.bb_percent_b_min}"
        )

    return {
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
        f"insti_net_{short_n}d_avg": round(insti_short_avg, 0) if insti_short_avg else None,
        f"insti_net_{long_n}d_avg": round(insti_long_avg, 0) if insti_long_avg else None,
        "bb_upper": round(float(bb_upper), 2) if pd.notna(bb_upper) else None,
        "bb_bandwidth": round(float(bb_bandwidth), 4) if pd.notna(bb_bandwidth) else None,
        "bb_percent_b": round(float(bb_percent_b), 4) if pd.notna(bb_percent_b) else None,
        f"bb_bw_{bb_short_n}d_avg": round(bb_bw_short_avg, 4) if bb_bw_short_avg else None,
        f"bb_bw_{bb_long_n}d_avg": round(bb_bw_long_avg, 4) if bb_bw_long_avg else None,
        "cond_insti": cond_insti,
        "cond_rsi": cond_rsi,
        "cond_macd": cond_macd,
        "cond_vol_ma5": cond_vol_ma5,
        "cond_vol_ma10": cond_vol_ma10,
        "cond_bb_narrow": cond_bb_narrow,
        "cond_bb_near_upper": cond_bb_near_upper,
        "reasons": "；".join(reasons),
    }


def _collect_values(
    sym: str,
    recent: dict[str, pd.DataFrame],
    sheets: list[str],
    column: str,
) -> list[float]:
    """Collect values for a symbol across multiple sheets."""
    values: list[float] = []
    for s in sheets:
        df = recent.get(s)
        if df is None:
            continue
        row = df[df["symbol"].astype(str).str.strip() == sym]
        if row.empty:
            continue
        val = row.iloc[0].get(column)
        if pd.notna(val):
            values.append(float(val))
    return values
