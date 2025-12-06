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
    r"^\[(?P<time>\d{2}:\d{2}:\d{2})\]\s+(?P<rest>.+)$",
    r"^(?P<date>\d{4}[-/]\d{2}[-/]\d{2})\s+(?P<time>\d{2}:\d{2}:\d{2})\s*[-]?\s*(?P<rest>.+)$",
]

ERROR_PATTERN = re.compile(
    r"(?P<code>[A-Z]{3,4}-\d{3})[:\s-]+(?P<msg>.+)"
)

# If a line has only "MOTN-019 - Fence open" (no timestamp)
JUST_CODE_PATTERN = re.compile(
    r"^(?P<code>[A-Z]{3,4}-\d{3})[:\s-]+(?P<msg>.+)$"
)


def parse_error_logs(default_date: date | None = None) -> pd.DataFrame:
    """
    Parse error_logs.txt into a normalized CSV.
    """
    if default_date is None:
        default_date = DEFAULT_LOG_DATE

    rows = []
    with ERROR_LOGS_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue

            timestamp = None
            rest = raw

            # Try timestamp patterns
            for pat in TIMESTAMP_PATTERNS:
                m = re.match(pat, raw)
                if m:
                    time_str = m.groupdict().get("time")
                    date_str = m.groupdict().get("date")
                    if date_str:
                        dt_str = f"{date_str} {time_str}"
                    else:
                        # attach default_date for time-only lines like [09:18:37]
                        dt_str = f"{default_date.isoformat()} {time_str}"
                    try:
                        timestamp = dateparser.parse(dt_str)
                    except Exception:
                        timestamp = None
                    rest = m.groupdict().get("rest", "")
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

            if not error_code and not message:
                # Store as raw if we can't parse
                rows.append(
                    {
                        "timestamp": timestamp,
                        "error_code": None,
                        "error_group": None,
                        "message_raw": raw,
                        "axis": None,
                    }
                )
                continue

            group = None
            if error_code:
                group = error_code.split("-")[0]

            rows.append(
                {
                    "timestamp": timestamp,
                    "error_code": error_code,
                    "error_group": group,
                    "message_raw": message,
                }
            )

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df.sort_values("timestamp", inplace=True)
    df.to_csv(ERROR_LOGS_PARSED, index=False)
    return df


if __name__ == "__main__":
    df = parse_error_logs()
    print(f"Parsed {len(df)} error log rows -> {ERROR_LOGS_PARSED}")