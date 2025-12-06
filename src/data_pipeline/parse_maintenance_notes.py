import re
from dateutil import parser as dateparser

import pandas as pd

from src.config import MAINT_NOTES_FILE, MAINT_NOTES_PARSED


AXIS_PATTERN = re.compile(r"(axis|joint)\s*(\d+)", re.IGNORECASE)


def parse_maintenance_notes() -> pd.DataFrame:
    rows = []
    with MAINT_NOTES_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue

            # Example: "2025-11-19 - Replaced motor on axis 3."
            # Split on first "-" or "–"
            parts = re.split(r"\s[-–]\s", raw, maxsplit=1)
            if len(parts) == 2:
                date_str, rest = parts
            else:
                date_str, rest = None, raw

            dt = None
            if date_str:
                try:
                    dt = dateparser.parse(date_str).date()
                except Exception:
                    dt = None

            axis = None
            m = AXIS_PATTERN.search(rest)
            if m:
                try:
                    axis = int(m.group(2))
                except ValueError:
                    axis = None

            task_type = None
            lower = rest.lower()
            if "replace" in lower and "motor" in lower:
                task_type = "replace_motor"
            elif "lubric" in lower:
                task_type = "lubricate_axis"
            elif "belt" in lower:
                task_type = "check_belts"
            elif "sensor" in lower:
                task_type = "clean_sensors"
            elif "wire" in lower:
                task_type = "inspect_wiring"
            elif "calib" in lower:
                task_type = "calibrate_joints"

            rows.append(
                {
                    "date": dt,
                    "axis": axis,
                    "task_type": task_type,
                    "note_raw": rest,
                }
            )

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df.to_csv(MAINT_NOTES_PARSED, index=False)
    return df


if __name__ == "__main__":
    df = parse_maintenance_notes()
    print(f"Parsed {len(df)} maintenance notes -> {MAINT_NOTES_PARSED}")
