# extract.py

"""
Urban Air Quality Monitoring – Extract Step (OpenAQ v3 + Open-Meteo fallback)

- Primary API: OpenAQ v3
  https://api.openaq.org/v3/latest?city=Delhi

- Fallback API: Open-Meteo Air Quality API
  https://air-quality-api.open-meteo.com/v1/air-quality

- Saves raw data: data/raw/<city>_raw_<timestamp>.json
"""

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List

import requests
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------
# CONFIG
# -----------------------------------------------------

RAW_DIR = Path(os.getenv("RAW_DIR", Path(__file__).resolve().parents[0] / "data" / "raw"))
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Correct OpenAQ v3 endpoint
OPENAQ_BASE = "https://api.openaq.org/v3/latest"

DEFAULT_CITIES = {
    "Delhi": {"lat": 28.7041, "lon": 77.1025},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639},
}

OPEN_METEO_BASE = "https://air-quality-api.open-meteo.com/v1/air-quality"

MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
TIMEOUT = int(os.getenv("TIMEOUT_SECONDS", 10))


# -----------------------------------------------------
# Helpers
# -----------------------------------------------------

def ts():
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def save_raw(city: str, payload):
    filename = f"{city.lower()}_raw_{ts()}.json"
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    return str(path.resolve())


# -----------------------------------------------------
# API Callers
# -----------------------------------------------------

def call_openaq_v3(city: str) -> Optional[dict]:
    """Primary API → OpenAQ v3"""
    url = f"{OPENAQ_BASE}?city={city}"
    resp = requests.get(url, timeout=TIMEOUT)
    if resp.status_code == 200:
        data = resp.json()
        if data.get("results"):
            return data
    return None


def call_open_meteo(lat: float, lon: float) -> Optional[dict]:
    """Fallback API → Open-Meteo Air Quality"""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide",
    }
    resp = requests.get(OPEN_METEO_BASE, params=params, timeout=TIMEOUT)
    if resp.status_code == 200:
        return resp.json()
    return None


# -----------------------------------------------------
# ETL Extract Logic
# -----------------------------------------------------

def fetch_city(city: str, info: dict):
    """Fetch using OpenAQ v3 → fallback Open-Meteo"""
    print(f"\n➡️ Fetching: {city}")

    # Try OpenAQ with retries
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"   🌐 OpenAQ v3 attempt {attempt}/{MAX_RETRIES}...")
            data = call_openaq_v3(city)
            if data:
                path = save_raw(city, data)
                print(f"   ✅ OpenAQ success → saved: {path}")
                return {"city": city, "source": "OpenAQ", "raw_path": path}
        except Exception as e:
            print(f"   ⚠️ OpenAQ error: {e}")

        wait = 2 ** (attempt - 1)
        print(f"   ⏳ retrying in {wait}s...")
        time.sleep(wait)

    print(f"   ❌ OpenAQ failed → trying fallback Open-Meteo")

    # fallback
    lat = info["lat"]
    lon = info["lon"]
    fallback = call_open_meteo(lat, lon)

    if fallback:
        path = save_raw(city, fallback)
        print(f"   🟡 Fallback Open-Meteo success → saved: {path}")
        return {"city": city, "source": "Open-Meteo", "raw_path": path}

    print(f"   🔥 BOTH APIs FAILED for {city}")
    return {"city": city, "source": None, "raw_path": None}


def fetch_all():
    results = []
    for city, info in DEFAULT_CITIES.items():
        results.append(fetch_city(city, info))
    return results


# -----------------------------------------------------
# MAIN
# -----------------------------------------------------

if __name__ == "__main__":
    results = fetch_all()

    print("\n🟦 FINAL EXTRACTION SUMMARY")
    for r in results:
        print(f"{r['city']} → {r['source']} → {r['raw_path']}")
