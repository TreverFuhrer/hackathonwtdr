from pathlib import Path
from datetime import date

# Base directories
BASE_DIR = Path(__file__).resolve().parent.parent  # project_root/src -> project_root
RAW_DIR = BASE_DIR / "data_raw"
STAGE_DIR = BASE_DIR / "data_stage"
STRUCTURED_DIR = BASE_DIR / "data_structured"
VALIDATION_DIR = BASE_DIR / "validation"

for d in (RAW_DIR, STAGE_DIR, STRUCTURED_DIR, VALIDATION_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Severity thresholds (tweak as needed)
TORQUE_MEDIUM_THRESHOLD = 60.0  # percent of rated, example
TORQUE_CRITICAL_THRESHOLD = 80.0

# How far back to look when counting repeat events (hours)
REPEAT_WINDOW_HOURS = 24

# Default date for logs/alerts that only contain a time (no date in the line)
# This should match the date used in the sample robot logs / torque cycles.
DEFAULT_LOG_DATE = date(2025, 11, 17)

# File names (raw)
ERROR_LOGS_FILE = RAW_DIR / "error_logs.txt"
SYSTEM_ALERTS_FILE = RAW_DIR / "system_alerts.txt"
MAINT_NOTES_FILE = RAW_DIR / "maintenance_notes.txt"
SENSOR_READINGS_FILE = RAW_DIR / "sensor_readings.csv"
TORQUE_TIMESERIES_FILE = RAW_DIR / "Torque Timeseries.csv"
TORQUE_CYCLES_FILE = RAW_DIR / "Torque Events by Cycle.csv"
PERF_METRICS_FILE = RAW_DIR / "performance_metrics.csv"

# Outputs (staged / structured)
ERROR_LOGS_PARSED = STAGE_DIR / "error_logs_parsed.csv"
SYSTEM_ALERTS_PARSED = STAGE_DIR / "system_alerts_parsed.csv"
MAINT_NOTES_PARSED = STAGE_DIR / "maintenance_notes_parsed.csv"
SENSOR_READINGS_CLEAN = STAGE_DIR / "sensor_readings_clean.csv"
TORQUE_TIMESERIES_CLEAN = STAGE_DIR / "torque_timeseries_clean.csv"
TORQUE_CYCLES_CLEAN = STAGE_DIR / "torque_cycles_clean.csv"
PERF_METRICS_CLEAN = STAGE_DIR / "performance_metrics_clean.csv"

EVENTS_FILE = STRUCTURED_DIR / "events.csv"
AI_RECOMMENDATIONS_FILE = STRUCTURED_DIR / "ai_recommendations.csv"
VALIDATION_REPORT_FILE = VALIDATION_DIR / "events_quality_report.json"
VALIDATION_SUMMARY_FILE = VALIDATION_DIR / "events_quality_summary.txt"