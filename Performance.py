import pandas as pd

# Load cleaned dataset (or raw if clean not created yet)
try:
    df = pd.read_csv(r"C:\Hackathon\cleaned_sensor_readings.csv")
except FileNotFoundError:
    df = pd.read_csv(r"C:\Hackathon\sensor_readings.csv")

print("\n=== Dataset Quality ===")
print(f"Total rows: {len(df)}")
print("Missing values per column:")
print(df.isnull().sum())

print("\n=== Temperature Metrics ===")
print(f"Average: {df['Temperature_C'].mean():.2f} °C")
print(f"Min: {df['Temperature_C'].min():.2f} °C")
print(f"Max: {df['Temperature_C'].max():.2f} °C")
print(f"Std Dev: {df['Temperature_C'].std():.2f}")

print("\n=== Vibration Metrics ===")
print(f"Average: {df['Vibration_g'].mean():.3f} g")
print(f"Max: {df['Vibration_g'].max():.3f} g")

for axis in ['Axis1_deg','Axis2_deg','Axis3_deg']:
    print(f"\n=== {axis} Metrics ===")
    print(f"Average: {df[axis].mean():.2f}°")
    print(f"Min: {df[axis].min():.2f}°")
    print(f"Max: {df[axis].max():.2f}°")