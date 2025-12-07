import pandas as pd

from src.config import (
    SENSOR_READINGS_FILE,
    TORQUE_TIMESERIES_FILE,
    PERF_METRICS_FILE,
    SENSOR_READINGS_CLEAN,
    TORQUE_TIMESERIES_CLEAN,
    PERF_METRICS_CLEAN,
)


def _normalize_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure there's a 'timestamp' column in UTC, using best-effort detection.
    """
    if df.empty:
        return df

    candidates = [c for c in df.columns if c.lower() in ("timestamp", "time", "datetime")]
    if candidates:
        col = candidates[0]
    else:
        # Fall back to the first column
        col = df.columns[0]

    df["timestamp"] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Drop any original time-like columns except the new 'timestamp'
    drop_cols = [
        c
        for c in df.columns
        if c != "timestamp" and c.lower() in ("timestamp", "time", "datetime")
    ]
    if drop_cols:
        df.drop(columns=drop_cols, inplace=True)

    df.sort_values("timestamp", inplace=True, na_position="last")
    return df


def _clean_time_series(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """
    Apply 'good data hygiene' to time series:
    - Normalize timestamps
    - Interpolate numeric values using time
    - Add status + notes to label imputed vs missing
    """
    if df.empty:
        return df

    df = _normalize_timestamp(df)

    # Initialize status/notes
    df["status"] = "valid"
    df["notes"] = ""

    # Use timestamp as index for time-based interpolation
    df = df.set_index("timestamp")

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if not numeric_cols:
        df = df.reset_index()
        return df

    # Track which cells were NaN before interpolation
    nan_before = df[numeric_cols].isna()

    # Time interpolation, both directions to fill middle gaps
    df[numeric_cols] = df[numeric_cols].interpolate(
        method="time",
        limit_direction="both",
    )

    # Cells that were NaN and are now filled = estimated
    nan_after = df[numeric_cols].isna()
    imputed_mask = nan_before & ~nan_after

    # Rows where any value was imputed
    rows_imputed = imputed_mask.any(axis=1)
    df.loc[rows_imputed, "status"] = "estimated"

    # Simple note for all imputed rows
    df.loc[rows_imputed, "notes"] = df.loc[rows_imputed, "notes"].where(
        df.loc[rows_imputed, "notes"] != "",
        other="Imputed via time interpolation",
    )

    # Rows that still have NaN after interpolation
    remaining_nan = df[numeric_cols].isna()
    rows_partial = remaining_nan.any(axis=1)
    df.loc[rows_partial, "status"] = "partial_missing"

    df.loc[rows_partial, "notes"] = df.loc[rows_partial, "notes"].where(
        df.loc[rows_partial, "notes"] != "",
        other="Missing values remain after interpolation",
    )

    df = df.reset_index()
    return df


def parse_sensor_streams():
    """
    Parse and clean sensor_readings, torque_timeseries, and performance_metrics
    with proper data hygiene (timestamps normalized, interpolation + labeling).
    """
    # sensor_readings
    try:
        sr = pd.read_csv(SENSOR_READINGS_FILE)
        sr = _clean_time_series(sr, "sensor_readings")
        sr.to_csv(SENSOR_READINGS_CLEAN, index=False)
    except FileNotFoundError:
        sr = pd.DataFrame()

    # torque_timeseries
    try:
        tt = pd.read_csv(TORQUE_TIMESERIES_FILE)
        tt = _clean_time_series(tt, "torque_timeseries")
        tt.to_csv(TORQUE_TIMESERIES_CLEAN, index=False)
    except FileNotFoundError:
        tt = pd.DataFrame()

    # performance_metrics
    try:
        pm = pd.read_csv(PERF_METRICS_FILE)
        pm = _clean_time_series(pm, "performance_metrics")
        pm.to_csv(PERF_METRICS_CLEAN, index=False)
    except FileNotFoundError:
        pm = pd.DataFrame()

    return sr, tt, pm


if __name__ == "__main__":
    sr, tt, pm = parse_sensor_streams()
    print(f"sensor_readings rows: {len(sr)}")
    print(f"torque_timeseries rows: {len(tt)}")
    print(f"performance_metrics rows: {len(pm)}")