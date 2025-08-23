# demo_outputs.py
import pandas as pd

# Load outputs
summary_path = "outputs/summary.csv"
anomalies_path = "outputs/anomalies.csv"

try:
    summary = pd.read_csv(summary_path)
    anomalies = pd.read_csv(anomalies_path)
except FileNotFoundError:
    print("❌ Outputs not found. Run etl_anomaly.py first!")
    exit()

# Display top 10 daily summaries
print("=== Top 10 Daily Summary ===")
print(summary.head(10))

# Display top 10 anomalies
print("\n=== Top 10 Anomalies ===")
print(anomalies.head(10))

# Quick stats on summary
print("\n=== Summary Stats ===")
print(summary.describe())

# Total anomalies by type
anomaly_cols = [col for col in anomalies.columns if col.startswith("anomaly_")]
print("\n=== Total Anomalies by Type ===")
print(anomalies[anomaly_cols].sum())
