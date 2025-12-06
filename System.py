import pandas as pd

df = pd.read_csv(r"C:\Hackathon\cleaned_sensor_readings.csv")

alerts = []

# Temperature alerts
if df['Temperature_C'].max() > 50:
    alerts.append("⚠️ Overheating detected: Temperature exceeded 50 °C")
if df['Temperature_C'].min() < 10:
    alerts.append("⚠️ Too cold: Temperature dropped below 10 °C")

# Vibration alerts
if df['Vibration_g'].max() > 0.3:
    alerts.append("⚠️ Excessive vibration detected (>0.3 g)")

# Orientation alerts
for axis in ['Axis1_deg','Axis2_deg','Axis3_deg']:
    if df[axis].max() > 60:
        alerts.append(f"⚠️ {axis} exceeded safe orientation range (>60°)")

# Data quality alerts
missing_ratio = df.isnull().sum().sum() / (len(df) * len(df.columns))
if missing_ratio > 0.05:
    alerts.append("⚠️ More than 5% missing values in dataset")

print("\n=== System Alerts ===")
if alerts:
    for a in alerts:
        print(a)
else:
    print("✅ All systems stable")