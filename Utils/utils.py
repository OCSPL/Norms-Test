import pandas as pd
import numpy as np

def _to_float(x):
    """Coerce to float; returns NaN if not possible."""
    if isinstance(x, str):
        x = x.strip().replace(",", "")
    return pd.to_numeric(x, errors="coerce")

def percentage_to_decimal(perc):
    """
    Convert percentage to a ratio with the percent rounded to 2 decimals.

    Examples:
      "40%" -> 0.40
      "40.126%" -> 0.4013  (40.13% -> 0.4013)
      40 -> 0.40
      "40" -> 0.40
      0.4 -> 0.4   (already a ratio)
      "0.4" -> 0.4
    """
    if perc is None or (isinstance(perc, float) and pd.isna(perc)):
        return np.nan

    orig = perc
    is_pct_string = isinstance(orig, str) and "%" in orig

    # Strip percent sign if present and coerce
    val = _to_float(str(orig).replace("%", "")) if isinstance(orig, str) else _to_float(orig)
    if pd.isna(val):
        return np.nan

    # If it's a percent (has % or > 1), treat as percent and round to 2 decimals before dividing
    if is_pct_string or val > 1:
        val = round(val, 2)      # keep percent to 2 decimals
        return val / 100.0

    # Otherwise assume it's already a ratio (0..1)
    return float(val)

def multiply_with_percentage(value, perc, result_decimals=2):
    """
    Multiply a value with a percentage (rounded to 2 decimals) and
    return a number rounded to `result_decimals` (default 2).
    """
    v = _to_float(value)
    if pd.isna(v):
        v = 0.0

    ratio = percentage_to_decimal(perc)
    if pd.isna(ratio):
        return 0.0

    return round(float(v) * ratio, result_decimals)
