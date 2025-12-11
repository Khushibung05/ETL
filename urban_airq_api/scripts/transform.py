"""
transform.py

Transforms raw air-quality JSON files (OpenAQ v3 or Open-Meteo fallback)
into a clean tabular CSV with engineered features.

Input:
    data/raw/*.json

Output:
    data/staged/air_quality_transformed.csv
"""

import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

RAW_DIR = Path("data/raw/")
STAGED_DIR = Path("data/staged/")
STAGED_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = STAGED_DIR / "air_quality_transformed.csv"


# ---------------------------------------------------
# Helper Functions
# ---------------------------------------------------

def load_json_files():
    """Load all JSON files from data/raw directory."""
    files = list(RAW_DIR.glob("*.json"))
    print(f"📁 Found {len(files)} raw files.")
    return files


def flatten_openaq(payload, city):
    """
    Flatten OpenAQ v3 payload into a DataFrame.
    Many stations → multiple measurements → extract pollutants.
    """

    if "results" not in payload:
        return pd.DataFrame()

    rows = []

    for station in payload["results"]:
        if "measurements" not in station:
            continue

        for m in station["measurements"]:
            rows.append({
                "city": city,
                "time": m.get("lastUpdated") or m.get("date", {}).get("utc"),
                m.get("parameter"): m.get("value")
            })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    # Pivot to wide format: one row per timestamp
    df = df.pivot_table(
        index=["city", "time"],
        aggfunc="first"
    ).reset_index()

    return df


def flatten_open_meteo(payload, city):
    """
    Flatten Open-Meteo hourly AQ dataset.
    One row per hour.
    """

    if "hourly" not in payload:
        return pd.DataFrame()

    data = payload["hourly"]

    df = pd.DataFrame(data)
    df["city"] = city

    # Convert time column
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")

    return df


def detect_api_format(payload):
    """
    Determines if JSON is OpenAQ v3 or Open-Meteo.
    """
    if "results" in payload:
        return "openaq"
    if "hourly" in payload:
        return "openmeteo"
    return "unknown"


# ---------------------------------------------------
# Feature Engineering
# ---------------------------------------------------

def add_features(df):
    """Add AQI category, severity score, risk levels, and hour."""

    # Ensure numeric values
    pollutants = [
        "pm10", "pm2_5", "carbon_monoxide",
        "nitrogen_dioxide", "sulphur_dioxide",
        "ozone"
    ]

    for col in pollutants:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = None

    # Drop rows where all pollutants are missing
    df = df.dropna(subset=pollutants, how="all")

    # -------------------------
    # 1) AQI Category (PM2.5)
    # -------------------------
    def classify_aqi(pm2_5):
        if pd.isna(pm2_5):
            return "Unknown"
        if pm2_5 <= 50:
            return "Good"
        elif pm2_5 <= 100:
            return "Moderate"
        elif pm2_5 <= 200:
            return "Unhealthy"
        elif pm2_5 <= 300:
            return "Very Unhealthy"
        else:
            return "Hazardous"

    df["AQI_Category"] = df["pm2_5"].apply(classify_aqi)

    # ----------------------------------
    # 2) Pollution Severity Score
    # ----------------------------------
    df["severity_score"] = (
        (df["pm2_5"] * 5) +
        (df["pm10"] * 3) +
        (df["nitrogen_dioxide"] * 4) +
        (df["sulphur_dioxide"] * 4) +
        (df["carbon_monoxide"] * 2) +
        (df["ozone"] * 3)
    )

    # ----------------------------------
    # 3) Risk Classification
    # ----------------------------------
    def classify_risk(s):
        if pd.isna(s):
            return "Unknown"
        if s > 400:
            return "High Risk"
        elif s > 200:
            return "Moderate Risk"
        else:
            return "Low Risk"

    df["Risk_Level"] = df["severity_score"].apply(classify_risk)

    # ----------------------------------
    # 4) Hour Feature
    # ----------------------------------
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["hour"] = df["time"].dt.hour

    return df


# ---------------------------------------------------
# Main Transform Function
# ---------------------------------------------------

def transform():
    files = load_json_files()
    all_data = []

    for file in files:
        try:
            payload = json.loads(file.read_text())
        except:
            continue

        # Infer city from filename
        city = file.stem.split("_")[0]

        fmt = detect_api_format(payload)

        if fmt == "openaq":
            df = flatten_openaq(payload, city)
        elif fmt == "openmeteo":
            df = flatten_open_meteo(payload, city)
        else:
            print(f"⚠️ Unknown format for {file.name}, skipping.")
            continue

        if not df.empty:
            all_data.append(df)

    if not all_data:
        print("❌ No valid raw data found!")
        return

    final_df = pd.concat(all_data, ignore_index=True)

    # Apply cleaning + feature engineering
    final_df = add_features(final_df)

    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved transformed dataset → {OUTPUT_FILE}")


if __name__ == "__main__":
    transform()
