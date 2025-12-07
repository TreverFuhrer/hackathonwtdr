import pandas as pd

from src.config import TORQUE_CYCLES_FILE, TORQUE_CYCLES_CLEAN


def parse_torque_cycles() -> pd.DataFrame:
    """
    Parse raw torque cycle CSV into a clean, typed table with:
      - Normalized column names
      - Numeric peak_torque_pct (percent of rated torque)
      - Data hygiene flags (status, notes)
    """
    df = pd.read_csv(TORQUE_CYCLES_FILE)

    # 1) Normalize column names to what the rest of the pipeline expects
    rename_map: dict[str, str] = {}
    for col in df.columns:
        lc = col.lower()
        if "cycle" in lc and "id" in lc:
            rename_map[col] = "cycle_id"
        elif "start" in lc:
            rename_map[col] = "cycle_start"
        elif "end" in lc:
            rename_map[col] = "cycle_end"
        elif "axis" in lc:
            rename_map[col] = "axis"
        elif "peak" in lc and ("pct" in lc or "percent" in lc or "%" in lc):
            # e.g. "Peak_Torque_pct_of_rated"
            rename_map[col] = "peak_torque_pct"
        elif "error" in lc and "code" in lc:
            rename_map[col] = "related_error_code"

    if rename_map:
        df = df.rename(columns=rename_map)

    # 2) Ensure required columns exist
    required_cols = [
        "cycle_id",
        "cycle_start",
        "cycle_end",
        "axis",
        "peak_torque_pct",
        "related_error_code",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # 3) Normalize types
    df["cycle_start"] = pd.to_datetime(df["cycle_start"], errors="coerce", utc=True)
    df["cycle_end"] = pd.to_datetime(df["cycle_end"], errors="coerce", utc=True)
    df["axis"] = pd.to_numeric(df["axis"], errors="coerce").astype("Int64")
    df["peak_torque_pct"] = pd.to_numeric(df["peak_torque_pct"], errors="coerce")

    # 4) Data hygiene flags
    df["status"] = "valid"
    df["notes"] = ""

    missing_ts = df["cycle_start"].isna() | df["cycle_end"].isna()
    missing_torque = df["peak_torque_pct"].isna()

    both = missing_ts & missing_torque
    df.loc[both, "status"] = "partial_missing"
    df.loc[both, "notes"] = "Missing cycle_start/end and peak_torque_pct"

    only_ts = missing_ts & ~missing_torque
    df.loc[only_ts, "status"] = "partial_missing"
    df.loc[only_ts, "notes"] = "Missing cycle_start and/or cycle_end timestamp"

    only_torque = missing_torque & ~missing_ts
    df.loc[only_torque, "status"] = "partial_missing"
    df.loc[only_torque, "notes"] = "Missing peak_torque_pct"

    # 5) Save cleaned cycles
    df.to_csv(TORQUE_CYCLES_CLEAN, index=False)
    return df


if __name__ == "__main__":
    df = parse_torque_cycles()
    print(f"Parsed {len(df)} torque cycles -> {TORQUE_CYCLES_CLEAN}")