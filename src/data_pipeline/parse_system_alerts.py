from datetime import date
from dateutil import parser as dateparser

import pandas as pd

from src.config import (
    SYSTEM_ALERTS_FILE,
    SYSTEM_ALERTS_PARSED,
    DEFAULT_LOG_DATE,
)


def parse_system_alerts(default_date: date | None = None) -> pd.DataFrame:
    if default_date is None:
        default_date = DEFAULT_LOG_DATE

    rows = []
    with SYSTEM_ALERTS_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue

            # Expect something like: "10:03:00 NOTICE: Vibration spike"
            try:
                time_part, rest = raw.split(" ", 1)
            except ValueError:
                # fallback
                rows.append(
                    {
                        "timestamp": None,
                        "alert_level": None,
                        "alert_message": raw,
                        "alert_type": None,
                    }
                )
                continue

            dt_str = f"{default_date.isoformat()} {time_part}"
            timestamp = None
            try:
                timestamp = dateparser.parse(dt_str)
            except Exception:
                pass

            level = None
            msg = None
            if ":" in rest:
                # "NOTICE: Vibration spike"
                level_part, msg = rest.split(":", 1)
                level = level_part.strip().upper()
                msg = msg.strip()
            else:
                msg = rest.strip()

            # derive alert_type (temperature, vibration, network, servo, battery)
            alert_type = None
            if msg:
                m = msg.lower()
                if "temperature" in m or "temp" in m:
                    alert_type = "temperature"
                elif "vibration" in m:
                    alert_type = "vibration"
                elif "network" in m:
                    alert_type = "network"
                elif "servo" in m:
                    alert_type = "servo"
                elif "battery" in m:
                    alert_type = "battery"

            rows.append(
                {
                    "timestamp": timestamp,
                    "alert_level": level,
                    "alert_message": msg,
                    "alert_type": alert_type,
                }
            )

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df.sort_values("timestamp", inplace=True)
    df.to_csv(SYSTEM_ALERTS_PARSED, index=False)
    return df


if __name__ == "__main__":
    df = parse_system_alerts()
    print(f"Parsed {len(df)} system alert rows -> {SYSTEM_ALERTS_PARSED}")
