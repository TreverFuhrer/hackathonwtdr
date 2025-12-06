from src.config import DEFAULT_LOG_DATE
from src.data_pipeline.parse_error_logs import parse_error_logs
from src.data_pipeline.parse_system_alerts import parse_system_alerts
from src.data_pipeline.parse_maintenance_notes import parse_maintenance_notes
from src.data_pipeline.parse_sensor_streams import parse_sensor_streams
from src.data_pipeline.parse_torque_cycles import parse_torque_cycles
from src.data_pipeline.build_events import build_events
from src.data_pipeline.validate_events import validate_events


def main():
    print("Parsing error logs...")
    parse_error_logs(DEFAULT_LOG_DATE)

    print("Parsing system alerts...")
    parse_system_alerts(DEFAULT_LOG_DATE)

    print("Parsing maintenance notes...")
    parse_maintenance_notes()

    print("Parsing sensor streams (optional)...")
    parse_sensor_streams()

    print("Parsing torque cycles...")
    parse_torque_cycles()

    print("Building events...")
    build_events()

    print("Validating events...")
    validate_events()

    print("Pipeline complete.")


if __name__ == "__main__":
    main()