import re
from dateutil import parser as dateparser

import pandas as pd

from src.config import MAINT_NOTES_FILE, MAINT_NOTES_PARSED


AXIS_PATTERN = re.compile(r"(axis|joint)\s*(\d+)", re.IGNORECASE)


def parse_maintenance_notes() -> pd.DataFrame:
    rows: list[dict] = []
    with MAINT_NOTES_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue

            status = "valid"
            notes: list[str] = []

            # Expect something like "2025-11-19 - Replaced motor on axis 3"
            if " - " in raw:
                date_str, rest = raw.split(" - ", 1)
            else:
                date_str, rest = raw, ""
                status = "partial_missing"
                notes.append("Missing ' - ' separator; note body may be incomplete")

            try:
                dt = dateparser.parse(date_str).date()
            except Exception:
                dt = None
                status = "partial_missing"
                notes.append("Could not parse date in maintenance note")

            axis = None
            m_axis = AXIS_PATTERN.search(rest)
            if m_axis:
                axis = int(m_axis.group(2))
            else:
                notes.append("Axis/joint not identified in note")

            text_lower = rest.lower()
            if "replace" in text_lower and "motor" in text_lower:
                task_type = "replace_motor"
            elif "lubricat" in text_lower:
                task_type = "lubricate_axis"
            elif "belt" in text_lower:
                task_type = "check_belts"
            elif "sensor" in text_lower and "clean" in text_lower:
                task_type = "clean_sensors"
            elif "wiring" in text_lower or "cable" in text_lower:
                task_type = "inspect_wiring"
            elif "calibrat" in text_lower or "zero" in text_lower:
                task_type = "calibrate_joints"
            else:
                task_type = "other"

            rows.append(
                {
                    "date": dt,
                    "axis": axis,
                    "task_type": task_type,
                    "note_raw": rest,
                    "status": status,
                    "notes": "; ".join(notes) if notes else "",
                }
            )

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df.to_csv(MAINT_NOTES_PARSED, index=False)
    return df


if __name__ == "__main__":
    df = parse_maintenance_notes()
    print(f"Parsed {len(df)} maintenance notes -> {MAINT_NOTES_PARSED}")