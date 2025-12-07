import re
from datetime import datetime, date
from dateutil import parser as dateparser

import pandas as pd

from src.config import (
    ERROR_LOGS_FILE,
    ERROR_LOGS_PARSED,
    DEFAULT_LOG_DATE,
)

TIMESTAMP_PATTERNS = [
    # [HH:MM:SS] SRVO-160: Torque limit reached
    r"^\[(?P<time>\d{2}:\d{2}:\d{2})\]\s+(?P<rest>.+)$",
    # 2025-11-17 09:14:38 - SRVO-160: Torque limit reached
    r"^(?P<date>\d{4}[-/]\d{2}[-/]\d{2})\s+(?P<time>\d{2}:\d{2}:\d{2})\s*[-]?\s*(?P<rest>.+)$",
]

ERROR_PATTERN = re.compile(
    r"(?P<code>[A-Z]{3,4}-\d{3})[:\s-]+(?P<msg>.+)"
)

JUST_CODE_PATTERN = re.compile(
    r"^(?P<code>[A-Z]{3,4}-\d{3})[:\s-]+(?P<msg>.+)$"
)


def _parse_timestamp(raw: str, default_date: date) -> tuple[datetime | None, str, str]:
    """
    Returns (timestamp, timestamp_source, status_note)
    timestamp_source: full_datetime | time_only_default_date | missing
    """
    for pattern in TIMESTAMP_PATTERNS:
        m = re.search(pattern, raw)
        if not m:
            continue

        gd = m.groupdict()
        rest = gd.get("rest", "").strip()
        time_str = gd.get("time")
        date_str = gd.get("date")

        if date_str:
            # Full date + time in the line
            try:
                ts = dateparser.parse(f"{date_str} {time_str}")
                return ts, "full_datetime", ""
            except Exception:
                return None, "missing", "Failed to parse full datetime"

        # time only, we will attach DEFAULT_LOG_DATE
        try:
            ts = dateparser.parse(f"{default_date.isoformat()} {time_str}")
            return ts, "time_only_default_date", "Date inferred from DEFAULT_LOG_DATE"
        except Exception:
            return None, "missing", "Failed to parse time-only timestamp"

    # No timestamp pattern matched
    return None, "missing", "No timestamp present in line"


def parse_error_logs(default_date: date | None = None) -> pd.DataFrame:
    """
    Parse error_logs.txt into a normalized CSV with explicit data hygiene metadata.
    """
    if default_date is None:
        default_date = DEFAULT_LOG_DATE

    rows: list[dict] = []

    with ERROR_LOGS_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue

            # 1) Timestamp detection + source
            ts, ts_source, ts_note = _parse_timestamp(raw, default_date)

            # Extract the "rest" (non-timestamp) portion if we matched
            rest = raw
            for pattern in TIMESTAMP_PATTERNS:
                m = re.search(pattern, raw)
                if m:
                    rest = m.groupdict().get("rest", "").strip()
                    break

            error_code = None
            message = None

            m_err = ERROR_PATTERN.search(rest)
            if m_err:
                error_code = m_err.group("code").strip()
                message = m_err.group("msg").strip()
            else:
                m_simple = JUST_CODE_PATTERN.search(rest)
                if m_simple:
                    error_code = m_simple.group("code").strip()
                    message = m_simple.group("msg").strip()

            # Group from prefix (SRVO, MOTN, etc.)
            if error_code:
                group = error_code.split("-")[0]
            else:
                group = None

            # Data hygiene status + notes
            status = "valid"
            notes: list[str] = []

            if ts_source == "time_only_default_date":
                status = "estimated"
                if ts_note:
                    notes.append(ts_note)
            elif ts_source == "missing":
                status = "missing_timestamp"
                if ts_note:
                    notes.append(ts_note)

            if error_code is None and message is None:
                status = "parse_error"
                notes.append("Could not extract error_code or message from line")

            rows.append(
                {
                    "timestamp": ts,
                    "timestamp_source": ts_source,
                    "error_code": error_code,
                    "error_group": group,
                    "message_raw": message if message else rest,
                    "status": status,
                    "notes": "; ".join(notes) if notes else "",
                }
            )

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df.sort_values("timestamp", inplace=True, na_position="last")
    df.to_csv(ERROR_LOGS_PARSED, index=False)
    return df


if __name__ == "__main__":
    df = parse_error_logs()
    print(f"Parsed {len(df)} error log rows -> {ERROR_LOGS_PARSED}")