import pandas as pd

from src.config import TORQUE_CYCLES_FILE, TORQUE_CYCLES_CLEAN


def parse_torque_cycles() -> pd.DataFrame:
    df = pd.read_csv(TORQUE_CYCLES_FILE)

    # Try to standardize expected columns
    # Adjust these mappings if your column names differ
    rename_map = {}
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
        elif "peak" in lc and "%" in lc:
            rename_map[col] = "peak_torque_pct"
        elif "error" in lc or "code" in lc:
            rename_map[col] = "related_error_code"

    df = df.rename(columns=rename_map)

    # Basic required columns
    for required in ["cycle_id", "cycle_start", "cycle_end", "axis", "peak_torque_pct"]:
        if required not in df.columns:
            df[required] = None

    df["cycle_start"] = pd.to_datetime(df["cycle_start"], errors="coerce", utc=True)
    df["cycle_end"] = pd.to_datetime(df["cycle_end"], errors="coerce", utc=True)
    df["axis"] = pd.to_numeric(df["axis"], errors="coerce").astype("Int64")
    df["peak_torque_pct"] = pd.to_numeric(df["peak_torque_pct"], errors="coerce")

    df.to_csv(TORQUE_CYCLES_CLEAN, index=False)
    return df


if __name__ == "__main__":
    df = parse_torque_cycles()
    print(f"Parsed {len(df)} torque cycles -> {TORQUE_CYCLES_CLEAN}")
