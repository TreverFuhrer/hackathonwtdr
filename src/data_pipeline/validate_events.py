import json

import pandas as pd

from src.config import EVENTS_FILE, VALIDATION_REPORT_FILE, VALIDATION_SUMMARY_FILE


def validate_events():
    try:
        df = pd.read_csv(EVENTS_FILE, parse_dates=["timestamp"])
    except FileNotFoundError:
        raise SystemExit(f"{EVENTS_FILE} not found. Run build_events.py first.")

    report = {}

    report["total_events"] = len(df)
    report["missing_timestamps"] = int(df["timestamp"].isna().sum())
    report["missing_error_code"] = int(df["error_code"].isna().sum())
    report["missing_axis"] = int(df["axis"].isna().sum())
    report["severity_counts"] = df["severity"].value_counts(dropna=False).to_dict()
    report["collision_type_counts"] = (
        df["collision_type"].value_counts(dropna=False).to_dict()
    )

    # simple coverage metric: how many events have key fields
    key_mask = (~df["timestamp"].isna()) & (~df["error_code"].isna())
    report["coverage_ratio"] = float(key_mask.mean())

    with VALIDATION_REPORT_FILE.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    # Human-readable summary
    lines = [
        f"Total events: {report['total_events']}",
        f"Missing timestamps: {report['missing_timestamps']}",
        f"Missing error_code: {report['missing_error_code']}",
        f"Missing axis: {report['missing_axis']}",
        f"Coverage ratio: {report['coverage_ratio']:.2%}",
        "",
        "Severity counts:",
    ]
    for k, v in report["severity_counts"].items():
        lines.append(f"  {k}: {v}")
    lines.append("")
    lines.append("Collision type counts:")
    for k, v in report["collision_type_counts"].items():
        lines.append(f"  {k}: {v}")

    VALIDATION_SUMMARY_FILE.write_text("\n".join(lines), encoding="utf-8")

    return report


if __name__ == "__main__":
    rep = validate_events()
    print(f"Validation report saved to {VALIDATION_REPORT_FILE}")
    print(f"Summary saved to {VALIDATION_SUMMARY_FILE}")
