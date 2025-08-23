
# Python ETL & Anomaly Detection

## Overview
This project consolidates orders, payments, and refunds data, performs ETL cleaning, computes daily metrics, and detects anomalies in e-commerce transactions.

## Requirements
- Python 3.9+
- pandas
- openpyxl (for reading Excel files)
- argparse (built-in)

Install dependencies:
```bash
pip install pandas openpyxl
```

## Folder Structure
```
ETL_Anomaly_Detection/
├── dataset/
│   ├── orders.xlsx
│   ├── payments.xlsx
│   └── refunds.xlsx
├── outputs/           # Auto-generated
├── etl_anomaly.py     # Main script
└── README.md
```

## How to Run
Basic run command:
```bash
python etl_anomaly.py --orders dataset/orders.xlsx --payments dataset/payments.xlsx --refunds dataset/refunds.xlsx
```

Optional arguments:
- `--outdir` : specify output folder (default: outputs)
- `--date-from YYYY-MM-DD` : filter orders from this date
- `--date-to YYYY-MM-DD` : filter orders until this date
- `--offhours "HH:MM-HH:MM"` : flag orders in off-hours
- `--weekend` : flag orders on weekend

Example with all options:
```bash
python etl_anomaly.py --orders dataset/orders.xlsx --payments dataset/payments.xlsx --refunds dataset/refunds.xlsx --outdir outputs --date-from 2025-06-15 --date-to 2025-07-15 --offhours "21:00-09:00" --weekend
```
or 

suggested commands:
```bash
----------------------------
Basic Run (default output folder):

python etl_anomaly.py --orders dataset/orders.xlsx --payments dataset/payments.xlsx --refunds dataset/refunds.xlsx
----------------------------
Specify Output Folder:

  python etl_anomaly.py --orders dataset/orders.xlsx --payments dataset/payments.xlsx --refunds dataset/refunds.xlsx --outdir outputs_custom
----------------------------
Filter by Date Range:

  python etl_anomaly.py --orders dataset/orders.xlsx --payments dataset/payments.xlsx --refunds dataset/refunds.xlsx --date-from 2025-06-15 --date-to 2025-07-15
----------------------------
Enable Off-Hours Detection:

  python etl_anomaly.py --orders dataset/orders.xlsx --payments dataset/payments.xlsx --refunds dataset/refunds.xlsx --offhours "21:00-09:00"
----------------------------
Enable Weekend Detection:

  python etl_anomaly.py --orders dataset/orders.xlsx --payments dataset/payments.xlsx --refunds dataset/refunds.xlsx --weekend
----------------------------
Full Command (All Options Combined):

  python etl_anomaly.py --orders dataset/orders.xlsx --payments dataset/payments.xlsx --refunds dataset/refunds.xlsx --outdir outputs --date-from 2025-06-15 --date-to 2025-07-15 --offhours "21:00-09:00" --weekend
----------------------------


## Outputs
After running, `outputs/` will contain:
- `summary.csv` : daily metrics and anomaly counts
- `anomalies.csv` : detailed anomalies per order
- `report.json` : structured JSON report with detailed anomalies and metadata
