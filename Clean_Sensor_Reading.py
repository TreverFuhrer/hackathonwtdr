import os
import pandas as pd

csv_path = r"C:\Hackathon\sensor_readings.csv"

if not os.path.exists(csv_path):
    print(f"File not found: {csv_path}")
else:
    df = pd.read_csv(csv_path)

    # Print first 50 rows
    print(df.head(50))

    # Export first 50 rows
    df.head(50).to_csv(r"C:\Hackathon\sensor_readings_first50.csv", index=False)
    df.head(50).to_excel(r"C:\Hackathon\sensor_readings_first50.xlsx", index=False)

    # Show missing values summary
    print("\nMissing values per column:")
    print(df.isnull().sum())