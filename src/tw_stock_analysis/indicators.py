from __future__ import annotations

import pandas as pd


def compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Compute RSI series using Wilder's Smoothed Moving Average (SMMA).

    Wilder's SMMA is equivalent to EMA with alpha = 1/period.
    This is the standard method used by most trading platforms (TradingView, MT4, etc.).
    """
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    # Wilder's SMMA: SMMA_t = (SMMA_{t-1} * (period-1) + value_t) / period
    # Equivalent to EMA with alpha = 1/period
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def compute_macd(
    close: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Compute MACD, signal, histogram series."""
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(span=signal, adjust=False).mean()
    macd_hist = macd - macd_signal
    return macd, macd_signal, macd_hist


def compute_bollinger_bands(
    close: pd.Series,
    period: int = 20,
    num_std: float = 2.0,
) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series, pd.Series]:
    """Compute Bollinger Bands.

    Returns:
        Tuple of (upper, middle, lower, percent_b, bandwidth)
        - upper: Upper band (middle + num_std * std)
        - middle: Middle band (SMA)
        - lower: Lower band (middle - num_std * std)
        - percent_b: %B = (close - lower) / (upper - lower)
        - bandwidth: Bandwidth = (upper - lower) / middle
    """
    middle = close.rolling(window=period, min_periods=period).mean()
    std = close.rolling(window=period, min_periods=period).std()
    upper = middle + num_std * std
    lower = middle - num_std * std
    percent_b = (close - lower) / (upper - lower)
    bandwidth = (upper - lower) / middle
    return upper, middle, lower, percent_b, bandwidth
