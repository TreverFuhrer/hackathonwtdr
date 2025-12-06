import pandas as pd

from src.config import (
    SENSOR_READINGS_FILE,
    TORQUE_TIMESERIES_FILE,
    PERF_METRICS_FILE,
    SENSOR_READINGS_CLEAN,
    TORQUE_TIMESERIES_CLEAN,
    PERF_METRICS_CLEAN,
)


def _clean_timestamp(df: pd.DataFrame, col_name: str = "Timestamp") -> pd.DataFrame:
    # Try to find the timestamp column automatically if name differs
    candidates = [c for c in df.columns if c.lower() in ("timestamp", "time", "datetime")]
    if candidates:
        col = candidates[0]
    else:
        col = col_name if col_name in df.columns else df.columns[0]

    df["timestamp"] = pd.to_datetime(df[col], errors="coerce", utc=True)
    df.drop(columns=[col], inplace=True, errors="ignore")
    return df


def parse_sensor_streams():
    # sensor_readings
    try:
        sr = pd.read_csv(SENSOR_READINGS_FILE)
        sr = _clean_timestamp(sr)
        sr.to_csv(SENSOR_READINGS_CLEAN, index=False)
    except FileNotFoundError:
        sr = pd.DataFrame()

    # torque timeseries
    try:
        tt = pd.read_csv(TORQUE_TIMESERIES_FILE)
        tt = _clean_timestamp(tt)
        tt.to_csv(TORQUE_TIMESERIES_CLEAN, index=False)
    except FileNotFoundError:
        tt = pd.DataFrame()

    # performance metrics
    try:
        pm = pd.read_csv(PERF_METRICS_FILE)
        pm = _clean_timestamp(pm)
        pm.to_csv(PERF_METRICS_CLEAN, index=False)
    except FileNotFoundError:
        pm = pd.DataFrame()

    return sr, tt, pm


if __name__ == "__main__":
    sr, tt, pm = parse_sensor_streams()
    print(f"sensor_readings rows: {len(sr)}")
    print(f"torque_timeseries rows: {len(tt)}")
    print(f"performance_metrics rows: {len(pm)}")
