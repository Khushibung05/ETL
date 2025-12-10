# load.py
# Purpose: Load transformed Telco dataset into Supabase using Supabase client

import os
import math
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

BATCH_SIZE = 200  # adjust as needed

def get_supabase_client() -> Client:
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")
    return create_client(url, key)

def create_table_if_not_exists():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.telco_customer_churn (
        id BIGSERIAL PRIMARY KEY,
        gender TEXT,
        SeniorCitizen INTEGER,
        Partner TEXT,
        Dependents TEXT,
        tenure INTEGER,
        PhoneService TEXT,
        MultipleLines TEXT,
        InternetService TEXT,
        OnlineSecurity TEXT,
        OnlineBackup TEXT,
        DeviceProtection TEXT,
        TechSupport TEXT,
        StreamingTV TEXT,
        StreamingMovies TEXT,
        Contract TEXT,
        PaperlessBilling TEXT,
        PaymentMethod TEXT,
        MonthlyCharges DOUBLE PRECISION,
        TotalCharges DOUBLE PRECISION,
        Churn TEXT,
        tenure_group TEXT,
        monthly_charge_segment TEXT,
        has_internet_service INTEGER,
        is_multi_line_user INTEGER,
        contract_type_code INTEGER
    );
    """
    try:
        supabase = get_supabase_client()
        try:
            supabase.rpc('execute_sql', {'query': create_table_sql}).execute()
            print("✅ Table 'telco_customer_churn' created or already exists (via RPC).")
        except Exception as e:
            print(f"ℹ️  Note: {e}")
            print("ℹ️  Table will be created on first insert if project allows.")
    except Exception as e:
        print(f"⚠️  Error checking/creating table: {e}")
        print("ℹ️  Trying to continue with data insertion...")

def load_to_supabase(staged_path: str, table_name: str = "telco_customer_churn"):
    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(os.path.join(os.path.dirname(__file__), staged_path))

    print(f"🔍 Looking for data file at: {staged_path}")
    if not os.path.exists(staged_path):
        print(f"❌ Error: File not found at {staged_path}")
        print("ℹ️  Please run transform.py first to generate the transformed data")
        return

    try:
        supabase = get_supabase_client()
    except Exception as e:
        print(f"❌ Could not initialize Supabase client: {e}")
        return

    try:
        df = pd.read_csv(staged_path)
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return

    total_rows = len(df)
    print(f"📊 Loading {total_rows} rows into '{table_name}'...")

    for i in range(0, total_rows, BATCH_SIZE):
        batch = df.iloc[i:i + BATCH_SIZE].copy()

        # Convert NaN -> None (JSON-safe)
        batch = batch.where(pd.notnull(batch), None)

        # Cast integer-like float columns if they exist in batch
        for col in ["tenure", "SeniorCitizen", "contract_type_code", "is_multi_line_user", "has_internet_service"]:
            if col in batch.columns:
                try:
                    if batch[col].dtype == "float64":
                        if batch[col].dropna().apply(lambda x: float(x).is_integer()).all():
                            batch[col] = batch[col].astype("Int64")
                except Exception:
                    pass

        records = batch.to_dict("records")

        # Replace lingering float('nan') with None (defensive)
        for rec in records:
            for k, v in list(rec.items()):
                if isinstance(v, float) and math.isnan(v):
                    rec[k] = None

        # Ensure numpy scalar -> python builtins for simple types
        def normalize_value(v):
            import numpy as _np
            if v is None:
                return None
            if isinstance(v, _np.integer):
                return int(v)
            if isinstance(v, _np.floating):
                if float(v).is_integer():
                    return int(v)
                return float(v)
            if isinstance(v, _np.bool_):
                return bool(v)
            return v

        for rec in records:
            for k in list(rec.keys()):
                rec[k] = normalize_value(rec[k])

        try:
            
            # Convert dict keys to lowercase so they match Postgres column names (unquoted -> lowercase)
            normalized_records = []
            for rec in records:
                lower_rec = {k.lower(): v for k, v in rec.items()}
                normalized_records.append(lower_rec)

# use normalized_records for insertion
            response = supabase.table(table_name).insert(normalized_records).execute()

            err = None
            if hasattr(response, "error") and response.error:
                err = response.error
            elif isinstance(response, dict) and response.get("error"):
                err = response.get("error")

            if err:
                batch_no = (i // BATCH_SIZE) + 1
                print(f"⚠️  Error in batch {batch_no}: {err}")
                if records:
                    print("Sample record:", records[0])
            else:
                end = min(i + BATCH_SIZE, total_rows)
                print(f"✅ Inserted rows {i+1}-{end} of {total_rows}")
        except Exception as e:
            batch_no = (i // BATCH_SIZE) + 1
            print(f"⚠️  Exception inserting batch {batch_no}: {e}")
            if records:
                print("Sample record:", records[0])
            continue

    print(f"🎯 Finished loading data into '{table_name}'.")

if __name__ == "__main__":
    staged_csv_path = os.path.join("..", "data", "staged", "telco_transformed.csv")
    create_table_if_not_exists()
    load_to_supabase(staged_csv_path)
