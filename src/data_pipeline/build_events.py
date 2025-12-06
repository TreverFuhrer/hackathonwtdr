from datetime import timedelta

import pandas as pd

from src.config import (
    ERROR_LOGS_PARSED,
    SYSTEM_ALERTS_PARSED,
    MAINT_NOTES_PARSED,
    TORQUE_CYCLES_CLEAN,
    EVENTS_FILE,
    TORQUE_MEDIUM_THRESHOLD,
    TORQUE_CRITICAL_THRESHOLD,
    REPEAT_WINDOW_HOURS,
)


def _load_stage(path) -> pd.DataFrame:
    """Helper to load a staged CSV with a timestamp column, or return empty."""
    try:
        return pd.read_csv(path, parse_dates=["timestamp"])
    except FileNotFoundError:
        return pd.DataFrame()


def build_events() -> pd.DataFrame:
    # 1) Load staged inputs
    errors = _load_stage(ERROR_LOGS_PARSED)
    alerts = _load_stage(SYSTEM_ALERTS_PARSED)

    try:
        torque_cycles = pd.read_csv(
            TORQUE_CYCLES_CLEAN,
            parse_dates=["cycle_start", "cycle_end"],
        )
    except FileNotFoundError:
        torque_cycles = pd.DataFrame()

    try:
        maint = pd.read_csv(MAINT_NOTES_PARSED, parse_dates=["date"])
    except FileNotFoundError:
        maint = pd.DataFrame(columns=["date", "axis", "task_type", "note_raw"])

    # ----- DEBUG: show torque_cycles structure -----
    if torque_cycles.empty:
        print("DEBUG – torque_cycles is EMPTY (no torque mapping available).")
    else:
        print("DEBUG – torque_cycles columns:", torque_cycles.columns.tolist())
        print("DEBUG – torque_cycles dtypes:")
        print(torque_cycles.dtypes)
        print("DEBUG – first 5 rows of torque_cycles:")
        print(torque_cycles.head())

        # Normalize axis type early
        if "axis" not in torque_cycles.columns:
            torque_cycles["axis"] = 0
        torque_cycles["axis"] = (
            pd.to_numeric(torque_cycles["axis"], errors="coerce")
            .fillna(0)
            .astype("Int64")
        )

        # Ensure we have a usable peak_torque_pct column.
        # Rules:
        #   1) If peak_torque_pct exists AND has any non-NaN values: keep it.
        #   2) Else, if Peak_Torque_pct_of_rated exists: copy from that.
        #   3) Else, infer from any column containing 'torque' in its name.
        #   4) Else, leave as NaN.
        if "peak_torque_pct" in torque_cycles.columns:
            all_nan = torque_cycles["peak_torque_pct"].isna().all()
        else:
            all_nan = True

        if all_nan:
            if "Peak_Torque_pct_of_rated" in torque_cycles.columns:
                print(
                    "DEBUG – peak_torque_pct is all NaN; "
                    "mapping from 'Peak_Torque_pct_of_rated'."
                )
                torque_cycles["peak_torque_pct"] = torque_cycles[
                    "Peak_Torque_pct_of_rated"
                ]
            else:
                torque_col = None
                for col in torque_cycles.columns:
                    if "torque" in col.lower():
                        torque_col = col
                        break

                if torque_col is not None:
                    print(
                        f"DEBUG – peak_torque_pct is all NaN; "
                        f"mapping torque column '{torque_col}' -> 'peak_torque_pct'."
                    )
                    torque_cycles["peak_torque_pct"] = torque_cycles[torque_col]
                else:
                    print(
                        "DEBUG – no torque-like column found; "
                        "leaving peak_torque_pct as NaN."
                    )

    # 2) Filter error logs for "interesting" events
    interesting_mask = errors["message_raw"].str.contains(
        "collision|torque limit|overtravel|singularity|e-stop|fence open",
        case=False,
        na=False,
    )
    events = errors[interesting_mask].copy()

    # 3) Drop rows without timestamps
    events = events.dropna(subset=["timestamp"]).reset_index(drop=True)

    # 4) Axis handling – always have an axis column, default 0 for unknown
    if "axis" not in events.columns:
        events["axis"] = 0
    else:
        events["axis"] = events["axis"].fillna(0)

    events["axis"] = (
        pd.to_numeric(events["axis"], errors="coerce").fillna(0).astype("Int64")
    )

    # 5) Match events to torque cycles by timestamp ONLY; inherit axis from cycles
    if not torque_cycles.empty:
        events = events.sort_values("timestamp")
        torque_cycles = torque_cycles.sort_values("cycle_start")

        merged_rows = []
        for _, ev in events.iterrows():
            ts = ev["timestamp"]

            # Find any cycle that contains this timestamp
            candidates = torque_cycles[
                (torque_cycles["cycle_start"] <= ts)
                & (torque_cycles["cycle_end"] >= ts)
            ]

            ev_dict = ev.to_dict()

            if not candidates.empty:
                tc = candidates.iloc[0]
                ev_dict["cycle_id"] = tc.get("cycle_id")
                ev_dict["peak_torque_pct"] = tc.get("peak_torque_pct")

                # If axis is still 0 (unknown), inherit from torque cycle
                tc_axis = tc.get("axis")
                if ev_dict.get("axis", 0) in (0, None, pd.NA) and pd.notna(tc_axis):
                    ev_dict["axis"] = int(tc_axis)
            else:
                ev_dict["cycle_id"] = None
                ev_dict["peak_torque_pct"] = None

            merged_rows.append(ev_dict)

        events = pd.DataFrame(merged_rows)
    else:
        events["cycle_id"] = None
        events["peak_torque_pct"] = None

    # 6) Attach nearest system alert within +/- 30 seconds
    if not alerts.empty:
        alerts = alerts.dropna(subset=["timestamp"]).copy()
        alerts = alerts.sort_values("timestamp")

        alert_rows = []
        for _, ev in events.iterrows():
            ts = ev["timestamp"]
            window = alerts[
                (alerts["timestamp"] >= ts - timedelta(seconds=30))
                & (alerts["timestamp"] <= ts + timedelta(seconds=30))
            ]
            if not window.empty:
                # pick the highest severity-ish (CRITICAL > ALERT > WARN > NOTICE > INFO)
                def _sev_rank(level):
                    order = {
                        "CRITICAL": 5,
                        "ALERT": 4,
                        "WARN": 3,
                        "NOTICE": 2,
                        "INFO": 1,
                    }
                    return order.get(str(level).upper(), 0)

                window = window.copy()
                window["sev_rank"] = window["alert_level"].apply(_sev_rank)
                best = window.sort_values("sev_rank", ascending=False).iloc[0]
                alert_rows.append(
                    {
                        "alert_level": best["alert_level"],
                        "alert_type": best["alert_type"],
                        "alert_message": best["alert_message"],
                    }
                )
            else:
                alert_rows.append(
                    {"alert_level": None, "alert_type": None, "alert_message": None}
                )

        events = pd.concat(
            [events.reset_index(drop=True), pd.DataFrame(alert_rows)], axis=1
        )
    else:
        events["alert_level"] = None
        events["alert_type"] = None
        events["alert_message"] = None

    # 7) Attach last maintenance for that axis before event
    events["last_maintenance_date"] = None
    events["last_maintenance_task"] = None
    events["days_since_last_maintenance"] = None

    if not maint.empty:
        maint["axis"] = pd.to_numeric(maint["axis"], errors="coerce").astype("Int64")
        for i, ev in events.iterrows():
            axis = ev.get("axis", pd.NA)
            ts = ev["timestamp"]
            if pd.isna(axis) or pd.isna(ts):
                continue

            # Make ts tz-naive before comparing with maint["date"]
            ts_val = ts
            if getattr(ts_val, "tzinfo", None) is not None:
                ts_val = ts_val.tz_convert(None)
            ts_date = ts_val.normalize()

            candidates = maint[
                (maint["axis"] == axis) & (maint["date"] <= ts_date)
            ]
            if not candidates.empty:
                last = candidates.sort_values("date").iloc[-1]
                events.at[i, "last_maintenance_date"] = last["date"]
                events.at[i, "last_maintenance_task"] = last["task_type"]
                delta = ts_date.date() - last["date"].date()
                events.at[i, "days_since_last_maintenance"] = delta.days

    # 8) Compute severity from torque + alert + message
    def compute_severity(row):
        pct = row.get("peak_torque_pct")
        level = str(row.get("alert_level") or "").upper()
        msg = str(row.get("message_raw") or "").lower()

        if pd.notna(pct):
            try:
                p = float(pct)
            except Exception:
                p = None
        else:
            p = None

        if "collision" in msg or "e-stop" in msg:
            if p is not None and p >= TORQUE_CRITICAL_THRESHOLD:
                return "critical"
            return "high"

        if p is not None:
            if p >= TORQUE_CRITICAL_THRESHOLD:
                return "critical"
            elif p >= TORQUE_MEDIUM_THRESHOLD:
                return "medium"

        if level == "CRITICAL":
            return "critical"
        if level in ("ALERT", "WARN"):
            return "medium"
        if level in ("NOTICE", "INFO"):
            return "low"
        return "low"

    events["severity"] = events.apply(compute_severity, axis=1)

    # 9) Compute repeats in last REPEAT_WINDOW_HOURS for same axis + error_code
    events = events.sort_values("timestamp").reset_index(drop=True)
    repeats = []
    window = timedelta(hours=REPEAT_WINDOW_HOURS)
    for i, ev in events.iterrows():
        axis = ev.get("axis", pd.NA)
        code = ev.get("error_code", None)
        ts = ev["timestamp"]
        if pd.isna(axis) or pd.isna(ts) or pd.isna(code):
            repeats.append(0)
            continue
        mask = (
            (events["axis"] == axis)
            & (events["error_code"] == code)
            & (events["timestamp"] >= ts - window)
            & (events["timestamp"] < ts)
        )
        repeats.append(mask.sum())

    events["repeats_24h"] = repeats

    # 10) Collision type categorization
    def classify_collision(row):
        msg = str(row.get("message_raw") or "").lower()
        code = str(row.get("error_code") or "").upper()
        if "collision" in msg:
            return "hard_impact"
        if "torque limit" in msg:
            return "torque_limit"
        if "overtravel" in msg:
            return "overtravel"
        if "singularity" in msg:
            return "path_singularity"
        if "fence open" in msg:
            return "safety_fence"
        if "e-stop" in msg or "estop" in msg:
            return "emergency_stop"
        if code.startswith("SRVO"):
            return "servo_fault"
        if code.startswith("MOTN"):
            return "motion_fault"
        return "other"

    events["collision_type"] = events.apply(classify_collision, axis=1)

    # 11) Location derived from axis (J1–J6, J0 for unknown)
    def axis_to_location(a):
        try:
            a_int = int(a)
        except Exception:
            return "J0"
        if a_int <= 0:
            return "J0"
        return f"J{a_int}"

    events["location"] = events["axis"].apply(axis_to_location)

    # 12) Force value derived from torque (simple mapping)
    events["force_value"] = pd.to_numeric(
        events.get("peak_torque_pct", 0.0), errors="coerce"
    ).fillna(0.0)

    # 13) Final fields / IDs
    events.insert(0, "event_id", range(1, len(events) + 1))
    events["status"] = "pending_inspection"

    # 14) DEBUG – show what we’re about to save
    print("DEBUG – events dtypes before saving:")
    print(events.dtypes)

    print("\nDEBUG – sample events before saving to EVENTS_FILE:")
    debug_cols = [
        "event_id",
        "timestamp",
        "axis",
        "cycle_id",
        "peak_torque_pct",
        "force_value",
        "severity",
        "collision_type",
    ]
    existing_debug_cols = [c for c in debug_cols if c in events.columns]
    print(events[existing_debug_cols].head(10))

    # 15) Save
    events.to_csv(EVENTS_FILE, index=False)
    return events


if __name__ == "__main__":
    df = build_events()
    print(f"Built {len(df)} events -> {EVENTS_FILE}")