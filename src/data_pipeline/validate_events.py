import json

import pandas as pd

from src.config import EVENTS_FILE, VALIDATION_REPORT_FILE, VALIDATION_SUMMARY_FILE


def validate_events():
    try:
        df = pd.read_csv(EVENTS_FILE, parse_dates=["timestamp"])
    except FileNotFoundError:
        raise SystemExit(f"{EVENTS_FILE} not found. Run build_events.py first.")

    report: dict = {}
    report["total_events"] = int(len(df))

    # Timestamp quality
    missing_ts = int(df["timestamp"].isna().sum())
    report["missing_timestamps"] = missing_ts

    # Error code completeness
    missing_err = int(df["error_code"].isna().sum()) if "error_code" in df.columns else 0
    report["missing_error_code"] = missing_err

    # Axis completeness
    if "axis" in df.columns:
        missing_axis = int((df["axis"].isna() | (df["axis"] <= 0)).sum())
    else:
        missing_axis = 0
    report["missing_axis"] = missing_axis

    # Severity distribution
    if "severity" in df.columns:
        sev_counts = df["severity"].value_counts(dropna=False).to_dict()
    else:
        sev_counts = {}
    report["severity_counts"] = sev_counts

    # Collision type distribution
    if "collision_type" in df.columns:
        col_counts = df["collision_type"].value_counts(dropna=False).to_dict()
    else:
        col_counts = {}
    report["collision_type_counts"] = col_counts

    # Confidence flags
    if "confidence_flag" in df.columns:
        conf_counts = df["confidence_flag"].value_counts(dropna=False).to_dict()
    else:
        conf_counts = {}
    report["confidence_flag_counts"] = conf_counts

    # Force value quality
    if "force_value" in df.columns:
        fv = pd.to_numeric(df["force_value"], errors="coerce")
        out_of_range = int(((fv < 0) | (fv > 10000)).sum())
        missing_force = int(fv.isna().sum())
    else:
        out_of_range = 0
        missing_force = 0
    report["force_value_out_of_range"] = out_of_range
    report["force_value_missing"] = missing_force

    # Simple coverage ratio similar to docs: how many have both ts + error_code
    if "error_code" in df.columns:
        good_mask = df["timestamp"].notna() & df["error_code"].notna()
        coverage_ratio = float(good_mask.mean()) if len(df) else 0.0
    else:
        coverage_ratio = 0.0
    report["coverage_ratio_timestamp_and_error"] = coverage_ratio

    # Save JSON
    VALIDATION_REPORT_FILE.write_text(json.dumps(report, indent=2), encoding="utf-8")

    # Human-readable summary
    lines: list[str] = []
    lines.append(f"Total events: {report['total_events']}")
    lines.append(f"Missing timestamps: {report['missing_timestamps']}")
    lines.append(f"Missing error_code: {report['missing_error_code']}")
    lines.append(f"Missing/unknown axis (<=0): {report['missing_axis']}")
    lines.append(f"Coverage (timestamp + error_code present): {coverage_ratio:.2%}")
    lines.append("")

    lines.append("Severity counts:")
    for k, v in report["severity_counts"].items():
        lines.append(f"  {k}: {v}")

    lines.append("")
    lines.append("Collision type counts:")
    for k, v in report["collision_type_counts"].items():
        lines.append(f"  {k}: {v}")

    if conf_counts:
        lines.append("")
        lines.append("Confidence flag counts:")
        for k, v in conf_counts.items():
            lines.append(f"  {k}: {v}")

    lines.append("")
    lines.append(f"Force values missing: {missing_force}")
    lines.append(f"Force values out of [0,10000]N: {out_of_range}")

    VALIDATION_SUMMARY_FILE.write_text("\n".join(lines), encoding="utf-8")

    return report


if __name__ == "__main__":
    rep = validate_events()
    print(f"Validation report saved to {VALIDATION_REPORT_FILE}")
    print(f"Summary saved to {VALIDATION_SUMMARY_FILE}")