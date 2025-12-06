import pandas as pd

df = pd.read_csv(r"C:\Hackathon\cleaned_sensor_readings.csv")

maintenance_notes = []

# Temperature
if df['Temperature_C'].max() > 50:
    maintenance_notes.append("Inspect cooling system; possible overheating.")
if df['Temperature_C'].min() < 10:
    maintenance_notes.append("Check sensor calibration; abnormal low reading.")

# Vibration
if df['Vibration_g'].max() > 0.3:
    maintenance_notes.append("Inspect mechanical joints; possible instability.")

# Orientation
for axis in ['Axis1_deg','Axis2_deg','Axis3_deg']:
    if df[axis].max() > 60:
        maintenance_notes.append(f"Check {axis} control; unsafe tilt detected.")

# Data Quality
missing_ratio = df.isnull().sum().sum() / (len(df) * len(df.columns))
if missing_ratio > 0.05:
    maintenance_notes.append("Review sensor wiring/logging; data integrity issue.")

print("\n=== Maintenance Notes ===")
if maintenance_notes:
    for note in maintenance_notes:
        print("🛠️", note)
else:
    print("✅ No maintenance required")