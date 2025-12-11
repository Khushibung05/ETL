# load.py
"""
Load transformed air-quality dataset into Supabase using the supabase client.

Creates table (if possible) and inserts rows in batches with retries.
"""

import os
import math
import time
from typing import List, Dict, Any
from pathlib import Path

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# CONFIG
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
RETRY_COUNT = int(os.getenv("RETRY_COUNT", "2"))  # number of retries in addition to first attempt
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
STAGED_DEFAULT = os.getenv("STAGED_CSV", str(Path(__file__).resolve().parents[0] / "data" / "staged" / "air_quality_transformed.csv"))

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("❌ SUPABASE_URL and SUPABASE_KEY must be set in environment (.env)")

def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def create_table_if_not_exists():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.air_quality_data (
      id BIGSERIAL PRIMARY KEY,
      city TEXT,
      time TIMESTAMP,
      pm10 DOUBLE PRECISION,
      pm2_5 DOUBLE PRECISION,
      carbon_monoxide DOUBLE PRECISION,
      nitrogen_dioxide DOUBLE PRECISION,
      sulphur_dioxide DOUBLE PRECISION,
      ozone DOUBLE PRECISION,
      uv_index DOUBLE PRECISION,
      aqi_category TEXT,
      severity_score DOUBLE PRECISION,
      risk_flag TEXT,
      hour INTEGER
    );
    """
    try:
        supabase = get_supabase_client()
        try:
            # Some projects expose an RPC named execute_sql; try to call it to run ad-hoc SQL
            supabase.rpc('execute_sql', {'query': create_table_sql}).execute()
            print("✅ Table 'air_quality_data' created or already exists (via RPC).")
        except Exception as e:
            # Not fatal — Supabase project may not allow RPC; table will often be created on first insert if allowed
            print(f"ℹ️ Could not create table via RPC: {e}")
            print("ℹ️ The table may be created automatically on first insert if your project permits.")
    except Exception as e:
        print(f"⚠️ Error checking/creating table: {e}")
        print("ℹ️ Will continue and attempt insertion (may fail if table does not exist).")

def _read_staged(staged_path: str) -> pd.DataFrame:
    path = Path(staged_path)
    if not path.exists():
        raise FileNotFoundError(f"Staged CSV not found at: {path}")
    df = pd.read_csv(path)
    print(f"📥 Loaded staged CSV: {path}  rows={len(df)} cols={len(df.columns)}")
    return df

def _normalize_for_insert(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Normalize column names to match DB (lowercase).
    - Ensure required columns exist (add None if missing).
    - Convert time -> ISO strings (or None).
    - Replace NaN with None.
    - Ensure numeric columns are numeric.
    """
    # canonical column mapping: expected DB column names
    expected_cols = [
        "city", "time", "pm10", "pm2_5", "carbon_monoxide",
        "nitrogen_dioxide", "sulphur_dioxide", "ozone", "uv_index",
        "aqi_category", "severity_score", "risk_flag", "hour"
    ]

    # lowercase existing columns to make matching easier
    df = df.rename(columns={c: c.lower() for c in df.columns})

    # if transform used different names, attempt some common maps
    rename_map = {}
    if "aqi_category" not in df.columns and "aqi_category".upper() in df.columns:
        rename_map["AQI_Category"] = "aqi_category"
    if "risk_flag" not in df.columns:
        if "risk_level" in df.columns:
            rename_map["risk_level"] = "risk_flag"
        if "risk" in df.columns:
            rename_map["risk"] = "risk_flag"
    if rename_map:
        df = df.rename(columns=rename_map)

    # ensure columns exist
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    # normalize time -> ISO string (Postgres accepts ISO-8601)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["time"] = df["time"].apply(lambda t: t.isoformat() if pd.notnull(t) else None)

    # numeric conversions
    num_cols = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "uv_index", "severity_score", "hour"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # hour integer-safe
    if "hour" in df.columns:
        try:
            df["hour"] = df["hour"].astype("Int64")
        except Exception:
            pass

    # convert pandas NA/NaN -> None
    df = df.where(pd.notnull(df), None)

    return df[expected_cols]  # return only DB columns in canonical order

def _normalize_record_types(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert numpy scalars and other types to native Python types and lower-case keys.
    """
    normalized = []
    for rec in records:
        r2 = {}
        for k, v in rec.items():
            key = k.lower()
            if v is None:
                r2[key] = None
                continue
            # numpy types
            if isinstance(v, (np.integer,)):
                r2[key] = int(v)
            elif isinstance(v, (np.floating,)):
                # convert floats that are integral to int
                if float(v).is_integer():
                    r2[key] = int(float(v))
                else:
                    r2[key] = float(v)
            elif isinstance(v, (np.bool_,)):
                r2[key] = bool(v)
            elif isinstance(v, (np.datetime64,)):
                # ensure ISO string
                try:
                    ts = pd.to_datetime(v).isoformat()
                    r2[key] = ts
                except Exception:
                    r2[key] = str(v)
            else:
                r2[key] = v
        normalized.append(r2)
    return normalized

def load_to_supabase(staged_path: str = STAGED_DEFAULT, table_name: str = "air_quality_data"):
    try:
        df = _read_staged(staged_path)
    except Exception as e:
        print(f"❌ Failed to read staged CSV: {e}")
        return

    df_norm = _normalize_for_insert(df)
    records = df_norm.to_dict(orient="records")
    total = len(records)
    if total == 0:
        print("❌ No records to insert.")
        return

    supabase = get_supabase_client()
    inserted = 0
    failed_batches = []

    print(f"🔄 Inserting {total} records into '{table_name}' in batches of {BATCH_SIZE} (retries={RETRY_COUNT})")

    batch_count = math.ceil(total / BATCH_SIZE)
    for i in range(0, total, BATCH_SIZE):
        batch_no = (i // BATCH_SIZE) + 1
        batch = records[i: i + BATCH_SIZE]
        # defensive cleaning: replace float('nan') with None
        for rec in batch:
            for k, v in list(rec.items()):
                if isinstance(v, float) and math.isnan(v):
                    rec[k] = None

        # normalize types and keys
        batch = _normalize_record_types(batch)

        attempt = 0
        success = False
        last_err = None
        while attempt <= RETRY_COUNT and not success:
            attempt += 1
            try:
                response = supabase.table(table_name).insert(batch).execute()
                # supabase client returns object with status_code, data, error in older/new versions; handle both patterns
                err = None
                if hasattr(response, "error") and response.error:
                    err = response.error
                elif isinstance(response, dict) and response.get("error"):
                    err = response.get("error")
                elif isinstance(response, tuple) and len(response) >= 2:
                    # some client versions return (data, count) or similar; assume success
                    err = None

                if err:
                    last_err = err
                    print(f"⚠️ [batch {batch_no}/{batch_count}] attempt {attempt} failed: {err}")
                    if attempt <= RETRY_COUNT:
                        backoff = 2 ** (attempt - 1)
                        print(f"   ⏳ Retrying batch {batch_no} in {backoff}s ...")
                        time.sleep(backoff)
                else:
                    # success
                    inserted += len(batch)
                    success = True
                    start = i + 1
                    end = min(i + BATCH_SIZE, total)
                    print(f"✅ Inserted rows {start}-{end} (batch {batch_no}/{batch_count})")
            except Exception as e:
                last_err = str(e)
                print(f"⚠️ [batch {batch_no}/{batch_count}] attempt {attempt} exception: {e}")
                if attempt <= RETRY_COUNT:
                    backoff = 2 ** (attempt - 1)
                    print(f"   ⏳ Retrying batch {batch_no} in {backoff}s ...")
                    time.sleep(backoff)

        if not success:
            failed_batches.append({"batch": batch_no, "size": len(batch), "error": last_err})

    # summary
    print("\n=== LOAD SUMMARY ===")
    print(f"Total records: {total}")
    print(f"Inserted: {inserted}")
    print(f"Failed batches: {len(failed_batches)}")
    if failed_batches:
        print("Failed batch details:")
        for fb in failed_batches[:10]:
            print(f" - batch {fb['batch']} size={fb['size']} error={fb['error']}")

    return {
        "total": total,
        "inserted": inserted,
        "failed_batches": failed_batches
    }

if __name__ == "__main__":
    # create table (best-effort)
    create_table_if_not_exists()
    staged = str(Path(__file__).resolve().parents[0] / "data" / "staged" / "air_quality_transformed.csv")
    load_to_supabase(staged)
