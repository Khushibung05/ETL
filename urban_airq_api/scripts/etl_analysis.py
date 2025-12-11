"""
etl_analysis.py

Purpose:
  - Read air_quality_data from Supabase
  - Compute KPI metrics
  - Produce CSV summaries:
      * data/processed/summary_metrics.csv
      * data/processed/city_risk_distribution.csv
      * data/processed/pollution_trends.csv
  - Produce PNG visualizations in data/processed/plots:
      * histogram_pm25.png
      * risk_flags_by_city.png
      * hourly_pm25_trends.png
      * severity_vs_pm25_scatter.png

Style:
  - Mirrors the Telco example style (supabase client via supabase-py)
  - Defensive about missing columns and empty datasets
"""

import os
from pathlib import Path
from typing import Tuple

import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from supabase import create_client, Client

# -----------------------------
# Config / Paths
# -----------------------------
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = os.getenv("AQ_TABLE", "air_quality_data")

PROCESSED_DIR = Path(os.getenv("PROCESSED_DIR", Path(__file__).resolve().parents[0] / "data" / "processed"))
PLOTS_DIR = PROCESSED_DIR / "plots"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

SUMMARY_CSV = PROCESSED_DIR / "summary_metrics.csv"
RISK_DIST_CSV = PROCESSED_DIR / "city_risk_distribution.csv"
TRENDS_CSV = PROCESSED_DIR / "pollution_trends.csv"

# -----------------------------
# Supabase client
# -----------------------------
def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("❌ SUPABASE_URL or SUPABASE_KEY missing in environment (.env)")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------------
# Fetch data from Supabase
# -----------------------------
def fetch_data(table: str = TABLE_NAME) -> pd.DataFrame:
    supabase = get_supabase_client()
    print(f"📥 Fetching data from Supabase table '{table}' ...")
    res = supabase.table(table).select("*").execute()

    # supabase-py may return object with .data / .error or a dict; handle both.
    if hasattr(res, "error") and res.error:
        raise RuntimeError(f"❌ Error fetching data: {res.error}")
    data = None
    if hasattr(res, "data"):
        data = res.data
    elif isinstance(res, dict) and res.get("data") is not None:
        data = res.get("data")
    else:
        # Some client versions return (data, count)
        if isinstance(res, tuple) and len(res) >= 1:
            data = res[0]
    if data is None:
        raise RuntimeError("❌ Unexpected response from Supabase client when fetching data.")

    df = pd.DataFrame(data)
    print(f"✅ Retrieved {len(df)} rows.")
    return df

# -----------------------------
# Cleaning + helpers
# -----------------------------
def prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names, convert time to datetime, ensure numeric columns exist."""
    if df.empty:
        return df

    # Normalize column names to lower-case
    df.columns = [c.lower() for c in df.columns]

    # Standardize expected column names
    # expected: city, time, pm10, pm2_5, ozone, sulphur_dioxide, nitrogen_dioxide,
    # carbon_monoxide, uv_index, aqi_category, severity_score, risk_flag, hour
    # Make sure columns exist
    expected = [
        "city", "time", "pm10", "pm2_5", "ozone", "sulphur_dioxide",
        "nitrogen_dioxide", "carbon_monoxide", "uv_index",
        "aqi_category", "severity_score", "risk_flag", "hour"
    ]
    for col in expected:
        if col not in df.columns:
            df[col] = None

    # Parse time
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # numeric conversions
    numeric_cols = ["pm10", "pm2_5", "ozone", "sulphur_dioxide", "nitrogen_dioxide", "carbon_monoxide", "uv_index", "severity_score"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # hour fallback: if hour missing, extract from time
    if df["hour"].isnull().all():
        df["hour"] = df["time"].dt.hour

    # Ensure city is string
    df["city"] = df["city"].astype("string")

    return df

# -----------------------------
# A. KPI Metrics
# -----------------------------
def compute_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """Return a small DataFrame with key metrics (one row per metric)."""
    metrics = []

    if df.empty:
        metrics.append({"metric": "note", "value": "No data available"})
        return pd.DataFrame(metrics)

    # 1) City with highest average PM2.5
    pm25_by_city = df.groupby("city")["pm2_5"].mean().dropna()
    if not pm25_by_city.empty:
        city_max_pm25 = pm25_by_city.idxmax()
        metrics.append({"metric": "city_highest_avg_pm2_5", "value": city_max_pm25})
        metrics.append({"metric": "highest_avg_pm2_5_value", "value": round(pm25_by_city.max(), 3)})
    else:
        metrics.append({"metric": "city_highest_avg_pm2_5", "value": None})

    # 2) City with highest average severity_score
    sev_by_city = df.groupby("city")["severity_score"].mean().dropna()
    if not sev_by_city.empty:
        city_max_sev = sev_by_city.idxmax()
        metrics.append({"metric": "city_highest_avg_severity", "value": city_max_sev})
        metrics.append({"metric": "highest_avg_severity_value", "value": round(sev_by_city.max(), 3)})
    else:
        metrics.append({"metric": "city_highest_avg_severity", "value": None})

    # 3) Percentage of High/Moderate/Low risk hours (global)
    # Normalize risk_flag text
    df["risk_flag_norm"] = df["risk_flag"].astype("string").str.strip().str.title().fillna("Unknown")
    risk_counts = df["risk_flag_norm"].value_counts(dropna=False)
    total = risk_counts.sum() if not risk_counts.empty else 0
    for label in ["High Risk", "Moderate Risk", "Low Risk", "Unknown"]:
        pct = None
        if total > 0:
            pct = round((risk_counts.get(label, 0) / total) * 100, 2)
        metrics.append({"metric": f"pct_{label.replace(' ', '_').lower()}", "value": pct})

    # 4) Hour of day with worst AQI (use avg pm2_5 as proxy)
    hourly_pm25 = df.groupby("hour")["pm2_5"].mean().dropna()
    if not hourly_pm25.empty:
        worst_hour = int(hourly_pm25.idxmax())
        metrics.append({"metric": "hour_worst_avg_pm2_5", "value": worst_hour})
        metrics.append({"metric": "worst_hour_avg_pm2_5_value", "value": round(hourly_pm25.max(), 3)})
    else:
        metrics.append({"metric": "hour_worst_avg_pm2_5", "value": None})

    return pd.DataFrame(metrics)

# -----------------------------
# B. City Pollution Trend Report
# -----------------------------
def build_pollution_trends(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each city generate time -> pm2_5, pm10, ozone.
    Returns a long-form DataFrame with columns: city, time, pm2_5, pm10, ozone
    """
    if df.empty:
        return pd.DataFrame(columns=["city", "time", "pm2_5", "pm10", "ozone"])

    trends = df[["city", "time", "pm2_5", "pm10", "ozone"]].copy()
    # drop rows with no timestamp
    trends = trends.dropna(subset=["time"])
    # sort for nice plotting
    trends = trends.sort_values(["city", "time"]).reset_index(drop=True)
    return trends

# -----------------------------
# C. City Risk Distribution
# -----------------------------
def build_risk_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a pivot-like DataFrame with counts and percentages of risk flags per city.
    """
    if df.empty:
        return pd.DataFrame()

    dist = (df.assign(risk=df["risk_flag"].astype("string").str.strip().str.title().fillna("Unknown"))
              .groupby(["city", "risk"])
              .size()
              .reset_index(name="count"))
    total_by_city = dist.groupby("city")["count"].transform("sum")
    dist["pct"] = (dist["count"] / total_by_city * 100).round(2)
    # pivot so each risk is a column (optional), but save long form as requested
    return dist.sort_values(["city", "risk"])

# -----------------------------
# D. Visualizations
# -----------------------------
def plot_histogram_pm25(df: pd.DataFrame, out_path: Path):
    plt.figure(figsize=(8, 5))
    vals = df["pm2_5"].dropna()
    if vals.empty:
        plt.text(0.5, 0.5, "No PM2.5 data", ha="center")
    else:
        plt.hist(vals, bins=30)
        plt.xlabel("PM2.5 (µg/m³)")
        plt.ylabel("Frequency")
        plt.title("Histogram of PM2.5")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()
    print(f"Saved: {out_path}")

def plot_risk_flags_by_city(df: pd.DataFrame, out_path: Path):
    # Count risk flags per city (stacked bar)
    if df.empty:
        pd.DataFrame().to_csv(out_path)  # touch file
        return
    pivot = (df.assign(risk=df["risk_flag"].astype("string").str.strip().str.title().fillna("Unknown"))
               .pivot_table(index="city", columns="risk", values="time", aggfunc="count", fill_value=0))
    pivot.plot(kind="bar", stacked=True, figsize=(10, 6))
    plt.title("Risk Flags by City (counts)")
    plt.ylabel("Count")
    plt.xlabel("City")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()
    print(f"Saved: {out_path}")

def plot_hourly_pm25_trends(trends_df: pd.DataFrame, out_path: Path):
    # plot average hourly pm2_5 per city (time series)
    if trends_df.empty:
        plt.figure(); plt.text(0.5, 0.5, "No trend data", ha="center"); plt.axis("off")
        plt.savefig(out_path); plt.close(); return

    # For readability, resample to hourly average per city (if multiple per hour)
    trends_df = trends_df.dropna(subset=["time"])
    trends_df["time_round"] = pd.to_datetime(trends_df["time"]).dt.floor("H")
    agg = trends_df.groupby(["city", "time_round"])["pm2_5"].mean().reset_index()
    plt.figure(figsize=(12, 6))
    for city, g in agg.groupby("city"):
        plt.plot(g["time_round"], g["pm2_5"], label=city)
    plt.legend()
    plt.xlabel("Time")
    plt.ylabel("PM2.5 (µg/m³)")
    plt.title("Hourly PM2.5 Trends by City")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()
    print(f"Saved: {out_path}")

def plot_severity_vs_pm25(df: pd.DataFrame, out_path: Path):
    plt.figure(figsize=(8, 6))
    sub = df.dropna(subset=["severity_score", "pm2_5"])
    if sub.empty:
        plt.text(0.5, 0.5, "Not enough data", ha="center")
    else:
        plt.scatter(sub["pm2_5"], sub["severity_score"], alpha=0.6)
        plt.xlabel("PM2.5 (µg/m³)")
        plt.ylabel("Severity Score")
        plt.title("Severity Score vs PM2.5")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()
    print(f"Saved: {out_path}")

# -----------------------------
# Export CSVs
# -----------------------------
def export_csv(df_summary: pd.DataFrame, df_risk: pd.DataFrame, df_trends: pd.DataFrame):
    df_summary.to_csv(SUMMARY_CSV, index=False)
    print(f"Saved summary metrics CSV: {SUMMARY_CSV}")
    df_risk.to_csv(RISK_DIST_CSV, index=False)
    print(f"Saved city risk distribution CSV: {RISK_DIST_CSV}")
    df_trends.to_csv(TRENDS_CSV, index=False)
    print(f"Saved pollution trends CSV: {TRENDS_CSV}")

# -----------------------------
# Main: orchestration
# -----------------------------
def main():
    # 1) fetch
    df = fetch_data(TABLE_NAME)

    # 2) prepare/clean
    df = prepare_df(df)

    # 3) compute metrics
    df_metrics = compute_kpis(df)

    # 4) build trends and risk distribution
    df_trends = build_pollution_trends(df)
    df_risk = build_risk_distribution(df)

    # 5) export CSVs
    export_csv(df_metrics, df_risk, df_trends)

    # 6) visualizations
    plot_histogram_pm25(df, PLOTS_DIR / "histogram_pm25.png")
    plot_risk_flags_by_city(df, PLOTS_DIR / "risk_flags_by_city.png")
    plot_hourly_pm25_trends(df_trends, PLOTS_DIR / "hourly_pm25_trends.png")
    plot_severity_vs_pm25(df, PLOTS_DIR / "severity_vs_pm25_scatter.png")

    print("\n🎉 ETL Analysis completed. CSVs and PNGs saved under:", PROCESSED_DIR)

if __name__ == "__main__":
    main()
