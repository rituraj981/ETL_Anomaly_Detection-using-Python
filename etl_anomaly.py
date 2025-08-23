import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import argparse

# ---------------------------
# 1. ReconciliationETL Class
# ---------------------------
class ReconciliationETL:
    def __init__(self, orders_path, payments_path, refunds_path):
        self.orders_path = orders_path
        self.payments_path = payments_path
        self.refunds_path = refunds_path
        self.orders = None
        self.payments = None
        self.refunds = None
        self.order_view = None

    def load_and_clean(self):
        # Load Excel/CSV files
        self.orders = pd.read_excel(self.orders_path)
        self.payments = pd.read_excel(self.payments_path)
        self.refunds = pd.read_excel(self.refunds_path)

        # Convert datetimes
        self.orders["order_datetime"] = pd.to_datetime(
            self.orders["order_datetime"], format="%d-%m-%Y %H:%M", errors="coerce"
        )
        self.payments["payment_datetime"] = pd.to_datetime(
            self.payments["payment_datetime"], format="%d-%m-%Y %H:%M", errors="coerce"
        )
        self.refunds["refund_datetime"] = pd.to_datetime(
            self.refunds["refund_datetime"], format="%d-%m-%Y %H:%M", errors="coerce"
        )

        # Convert numeric amounts
        self.orders["order_amount"] = pd.to_numeric(self.orders["order_amount"], errors="coerce")
        self.payments["paid_amount"] = pd.to_numeric(self.payments["paid_amount"], errors="coerce")
        self.refunds["refund_amount"] = pd.to_numeric(self.refunds["refund_amount"], errors="coerce")

        # Drop duplicates
        self.orders.drop_duplicates(inplace=True)
        self.payments.drop_duplicates(inplace=True)
        self.refunds.drop_duplicates(inplace=True)

        # Drop rows with missing essential IDs
        self.orders.dropna(subset=["order_id"], inplace=True)
        self.payments.dropna(subset=["payment_id", "order_id"], inplace=True)
        self.refunds.dropna(subset=["refund_id", "order_id"], inplace=True)

        return self

    def aggregate(self):
        # Aggregate successful payments
        payments_success = self.payments[self.payments["payment_status"] == "SUCCESS"]
        payment_agg = payments_success.groupby("order_id").agg(
            success_sum=("paid_amount", "sum"),
            first_success_time=("payment_datetime", "min"),
            gateways_used=("gateway", lambda x: list(set(x)))
        ).reset_index()

        # Aggregate refunds
        refund_agg = self.refunds.groupby("order_id").agg(
            refund_sum=("refund_amount", "sum"),
            first_refund_time=("refund_datetime", "min")
        ).reset_index()

        # Merge all together
        self.order_view = (
            self.orders
            .merge(payment_agg, on="order_id", how="left")
            .merge(refund_agg, on="order_id", how="left")
        )

        # Fill NaN for missing amounts
        self.order_view["success_sum"] = self.order_view["success_sum"].fillna(0)
        self.order_view["refund_sum"] = self.order_view["refund_sum"].fillna(0)

        return self.order_view

# ---------------------------
# 2. AnomalyDetector Class
# ---------------------------
class AnomalyDetector:
    def __init__(self, order_view, offhours=None, weekend=False):
        self.order_view = order_view
        self.offhours = offhours
        self.weekend = weekend

    def detect(self):
        df = self.order_view

        # Payment mismatch
        df["anomaly_payment_mismatch"] = df["order_amount"].round(2) != df["success_sum"].round(2)

        # Over refund
        df["anomaly_over_refund"] = df["refund_sum"] > df["order_amount"]

        # Late refund (>7 days)
        df["anomaly_late_refund"] = (df["first_refund_time"] - df["order_datetime"]).dt.days > 7

        # Multi-gateway
        df["anomaly_multigateway"] = df["gateways_used"].apply(lambda x: isinstance(x, list) and len(x) > 1)

        # Outlier amounts (IQR)
        Q1 = df["order_amount"].quantile(0.25)
        Q3 = df["order_amount"].quantile(0.75)
        IQR = Q3 - Q1
        df["anomaly_outlier_amount"] = (df["order_amount"] < Q1 - 1.5 * IQR) | (df["order_amount"] > Q3 + 1.5 * IQR)
        # Negative or invalid amounts
        df["anomaly_invalid_amount"] = (df["order_amount"] <= 0) | (df["success_sum"] < 0) | (df["refund_sum"] < 0)

        # Off-hours detection
        if self.offhours:
            start_str, end_str = self.offhours.split("-")
            start_hour, start_minute = map(int, start_str.split(":"))
            end_hour, end_minute = map(int, end_str.split(":"))

            df["hour"] = df["order_datetime"].dt.hour
            df["minute"] = df["order_datetime"].dt.minute

            def is_offhour(row):
                start = start_hour * 60 + start_minute
                end = end_hour * 60 + end_minute
                current = row["hour"] * 60 + row["minute"]
                if start < end:
                    return start <= current < end
                else:  # overnight range (e.g., 21:00-09:00)
                    return current >= start or current < end

            df["anomaly_offhours"] = df.apply(is_offhour, axis=1)
        else:
            df["anomaly_offhours"] = False

        # Weekend detection
        df["weekday"] = df["order_datetime"].dt.weekday
        df["anomaly_weekend"] = df["weekday"].isin([5, 6]) if self.weekend else False

        self.order_view = df
        return df

# ---------------------------
# 3. ReportGenerator Class
# ---------------------------
class ReportGenerator:
    def __init__(self, order_view, payments, refunds, outdir="outputs"):
        self.order_view = order_view
        self.payments = payments
        self.refunds = refunds
        self.outdir = Path(outdir)
        self.outdir.mkdir(exist_ok=True)

    def daily_summary(self):
        daily = (
            self.order_view
            .groupby(self.order_view["order_datetime"].dt.date)
            .agg(
                orders_placed=("order_id", "count"),
                payments_success_sum=("success_sum", "sum"),
                refunds_sum=("refund_sum", "sum")
            )
            .reset_index()
            .rename(columns={"order_datetime": "date"})
        )
        daily["net_revenue"] = daily["payments_success_sum"] - daily["refunds_sum"]
        return daily

    def daily_anomaly_summary(self):
        anomaly_cols = [col for col in self.order_view.columns if col.startswith("anomaly_")]
        daily_anomalies = (
            self.order_view
            .groupby(self.order_view["order_datetime"].dt.date)[anomaly_cols]
            .sum()
            .reset_index()
            .rename(columns={"order_datetime": "date"})
        )
        return daily_anomalies

    def generate_detailed_anomalies(self):
        anomalies = self.order_view[self.order_view.filter(like="anomaly_").any(axis=1)].copy()
        detailed = []

        for _, row in anomalies.iterrows():
            anomaly_reasons = []
            if row.get("anomaly_payment_mismatch"):
                anomaly_reasons.append({
                    "reason": "Payment mismatch",
                    "evidence": f"Order amount: {row['order_amount']}, Successful payments sum: {row['success_sum']}",
                    "suggested_action": "Verify payment records and reconcile discrepancies"
                })
            if row.get("anomaly_over_refund"):
                anomaly_reasons.append({
                    "reason": "Over-refund",
                    "evidence": f"Refund sum: {row['refund_sum']} exceeds order amount: {row['order_amount']}",
                    "suggested_action": "Check refund process for errors or fraud"
                })
            if row.get("anomaly_late_refund"):
                anomaly_reasons.append({
                    "reason": "Late refund",
                    "evidence": f"First refund at {row['first_refund_time']}, order placed at {row['order_datetime']}",
                    "suggested_action": "Ensure refunds are processed within 7 days"
                })
            if row.get("anomaly_offhours"):
                anomaly_reasons.append({
                    "reason": "Off-hours order",
                    "evidence": f"Order placed at {row['order_datetime'].time()}",
                    "suggested_action": "Verify legitimacy of off-hours transactions"
                })
            if row.get("anomaly_weekend"):
                anomaly_reasons.append({
                    "reason": "Weekend order",
                    "evidence": f"Order placed on {row['order_datetime'].strftime('%A')}",
                    "suggested_action": "Check if weekend policy was followed"
                })
            if row.get("anomaly_multigateway"):
                anomaly_reasons.append({
                    "reason": "Multi-gateway success",
                    "evidence": f"Gateways used: {row['gateways_used']}",
                    "suggested_action": "Verify if multiple payments were intended"
                })
            if row.get("anomaly_outlier_amount"):
                anomaly_reasons.append({
                    "reason": "Outlier amount",
                    "evidence": f"Order amount: {row['order_amount']}",
                    "suggested_action": "Review unusual transaction amounts"
                })

            if row.get("anomaly_invalid_amount"):
                anomaly_reasons.append({
                    "reason": "Invalid/negative amount",
                    "evidence": f"Order amount: {row['order_amount']}, Payment sum: {row['success_sum']}, Refund sum: {row['refund_sum']}",
                    "suggested_action": "Check data source for incorrect negative amounts"
                })

            detailed.append({
                "order_id": row["order_id"],
                "customer_id": row.get("customer_id", None),
                "anomalies": anomaly_reasons
            })

        return detailed

    def save_outputs(self):
        # Daily summary CSV
        daily = self.daily_summary()
        daily_anomalies = self.daily_anomaly_summary()
        daily_full = daily.merge(daily_anomalies, on="date", how="left")
        daily_full.to_csv(self.outdir / "summary.csv", index=False)

        # Anomalies CSV
        anomalies = self.order_view[self.order_view.filter(like="anomaly_").any(axis=1)]
        anomalies.to_csv(self.outdir / "anomalies.csv", index=False)

        # JSON report with detailed anomalies
        report = {
            "metadata": {
                "run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total_orders": len(self.order_view),
                "total_payments": len(self.payments),
                "total_refunds": len(self.refunds),
                "filtered_orders": len(self.order_view),  # NEW
                "anomaly_summary": anomalies.filter(like="anomaly_").sum().to_dict(),  # NEW
                "filters": {
                    "date_from": str(args.date_from) if args.date_from else None,
                    "date_to": str(args.date_to) if args.date_to else None,
                    "offhours": args.offhours,
                    "weekend": args.weekend
                }
            },
            "overall_metrics": daily_full.to_dict(orient="records"),
            "anomaly_counts": anomalies.filter(like="anomaly_").sum().to_dict(),
            "detailed_anomalies": self.generate_detailed_anomalies()
        }
        with open(self.outdir / "report.json", "w") as f:
            json.dump(report, f, indent=4, default=str)

        print(f"✅ summary.csv, anomalies.csv, and report.json saved in {self.outdir}/")

# ---------------------------
# 4. Main runner
# ---------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL + Anomaly Detection Pipeline")
    parser.add_argument("--orders", required=True, help="Path to orders file (xlsx or csv)")
    parser.add_argument("--payments", required=True, help="Path to payments file (xlsx or csv)")
    parser.add_argument("--refunds", required=True, help="Path to refunds file (xlsx or csv)")
    parser.add_argument("--outdir", default="outputs", help="Directory to save outputs")
    parser.add_argument("--date-from", help="Start date filter (YYYY-MM-DD)")
    parser.add_argument("--date-to", help="End date filter (YYYY-MM-DD)")
    parser.add_argument("--offhours", help="Detect off-hours orders, format HH:MM-HH:MM")
    parser.add_argument("--weekend", action="store_true", help="Flag to detect weekend anomalies")

    args = parser.parse_args()

    # Run ETL
    etl = ReconciliationETL(args.orders, args.payments, args.refunds).load_and_clean()

    # Optional date filters
    if args.date_from:
        date_from = pd.to_datetime(args.date_from)
        etl.orders = etl.orders[etl.orders["order_datetime"] >= date_from]
    if args.date_to:
        date_to = pd.to_datetime(args.date_to)
        etl.orders = etl.orders[etl.orders["order_datetime"] <= date_to]

    order_view = etl.aggregate()

    # Detect anomalies
    detector = AnomalyDetector(order_view, offhours=args.offhours, weekend=args.weekend)
    order_view = detector.detect()

    # Generate reports
    reporter = ReportGenerator(order_view, etl.payments, etl.refunds, outdir=args.outdir)
    reporter.save_outputs()


# ------------------------------------------------------
# ------------------------------------------------------
# ------------------------------------------------------

import pandas as pd

# Load outputs
summary = pd.read_csv("outputs/summary.csv")
anomalies = pd.read_csv("outputs/anomalies.csv")

# Inspect the first few rows
print("=== Daily Summary ===")
print(summary.head(10))

print("\n=== Anomalies ===")
print(anomalies.head(10))

# Quick stats
print("\nSummary stats:")
print(summary.describe())

print("\nTotal anomalies by type:")
anomaly_cols = [col for col in anomalies.columns if col.startswith("anomaly_")]
print(anomalies[anomaly_cols].sum())
