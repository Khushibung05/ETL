# ===========================
# validate.py  (Telco dataset)
# ===========================
# Purpose:
# - Validate transformed Telco data (CSV + Supabase)
# - Checks:
#   ✔ No missing values in tenure, MonthlyCharges, TotalCharges
#   ✔ Unique row count == original dataset row count
#   ✔ Transformed row count == Supabase row count
#   ✔ All segments exist in tenure_group and monthly_charge_segment
#   ✔ contract_type_code only in {0, 1, 2}
#   ✔ Print validation summary

import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv


# ---------- Supabase client helper ----------
def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")

    return create_client(url, key)


def validate_telco():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # project root

    # Paths
    raw_path = os.path.join(base_dir, "data", "raw", "WA_Fn-UseC_-Telco-Customer-Churn.csv")
    staged_path = os.path.join(base_dir, "data", "staged", "telco_transformed.csv")

    print("🔍 Starting Telco dataset validation...\n")

    # ---------- 1️⃣ Load datasets ----------
    if not os.path.exists(raw_path):
        print(f"❌ Raw dataset not found at: {raw_path}")
        return

    if not os.path.exists(staged_path):
        print(f"❌ Transformed dataset not found at: {staged_path}")
        print("ℹ️ Please run transform_telco.py first.")
        return

    raw_df = pd.read_csv(raw_path)
    df = pd.read_csv(staged_path)

    print(f"📁 Raw dataset rows:        {len(raw_df)}")
    print(f"📁 Transformed dataset rows:{len(df)}\n")

    # ---------- 2️⃣ No missing values in key numeric columns ----------
    required_numeric_cols = ["tenure", "MonthlyCharges", "TotalCharges"]
    missing_issues = {}

    for col in required_numeric_cols:
        if col not in df.columns:
            missing_issues[col] = "COLUMN MISSING"
        else:
            missing_count = df[col].isnull().sum()
            if missing_count > 0:
                missing_issues[col] = missing_count

    if not missing_issues:
        print("✅ No missing values in tenure, MonthlyCharges, TotalCharges")
    else:
        print("❌ Missing value issues in numeric columns:")
        for col, val in missing_issues.items():
            print(f"   - {col}: {val}")
    print()

    # ---------- 3️⃣ Unique row count vs original ----------
    unique_rows = len(df.drop_duplicates())
    raw_rows = len(raw_df)

    print(f"📊 Unique rows in transformed data: {unique_rows}")
    print(f"📊 Rows in original raw data:       {raw_rows}")

    if unique_rows == raw_rows:
        print("✅ Unique row count matches original dataset")
    else:
        print("⚠️ Unique row count does NOT match original dataset")
    print()

    # ---------- 4️⃣ Supabase row count ----------
    try:
        supabase = get_supabase_client()

        # Use exact count from Supabase
        response = supabase.table("telco_customer_churn").select("*", count="exact").execute()
        supabase_rows = response.count if hasattr(response, "count") else len(response.data)

        print(f"🗄️ Rows in Supabase table 'telco_customer_churn': {supabase_rows}")

        if supabase_rows == len(df):
            print("✅ Supabase row count matches transformed CSV")
        else:
            print("⚠️ Supabase row count does NOT match transformed CSV")

    except Exception as e:
        print(f"❌ Error fetching Supabase row count: {e}")
    print()

    # ---------- 5️⃣ Segment coverage checks ----------
    # tenure_group should have all: New, Regular, Loyal, Champion
    expected_tenure_groups = {"New", "Regular", "Loyal", "Champion"}
    if "tenure_group" in df.columns:
        actual_tenure_groups = set(df["tenure_group"].dropna().unique())
        missing_tenure_groups = expected_tenure_groups - actual_tenure_groups

        print(f"📌 tenure_group values present: {sorted(actual_tenure_groups)}")
        if not missing_tenure_groups:
            print("✅ All tenure_group segments present: New, Regular, Loyal, Champion")
        else:
            print(f"⚠️ Missing tenure_group segments: {sorted(missing_tenure_groups)}")
    else:
        print("❌ Column 'tenure_group' not found in transformed data")
    print()

    # monthly_charge_segment should have: Low, Medium, High
    expected_charge_segments = {"Low", "Medium", "High"}
    if "monthly_charge_segment" in df.columns:
        actual_charge_segments = set(df["monthly_charge_segment"].dropna().unique())
        missing_charge_segments = expected_charge_segments - actual_charge_segments

        print(f"📌 monthly_charge_segment values present: {sorted(actual_charge_segments)}")
        if not missing_charge_segments:
            print("✅ All monthly_charge_segment segments present: Low, Medium, High")
        else:
            print(f"⚠️ Missing monthly_charge_segment segments: {sorted(missing_charge_segments)}")
    else:
        print("❌ Column 'monthly_charge_segment' not found in transformed data")
    print()

    # ---------- 6️⃣ contract_type_code only in {0, 1, 2} ----------
    if "contract_type_code" in df.columns:
        unique_codes = set(df["contract_type_code"].dropna().unique())
        print(f"📌 contract_type_code values present: {sorted(unique_codes)}")

        allowed_codes = {0, 1, 2}
        invalid_codes = unique_codes - allowed_codes

        if not invalid_codes:
            print("✅ contract_type_code contains only {0, 1, 2}")
        else:
            print(f"❌ Invalid contract_type_code values found: {sorted(invalid_codes)}")
    else:
        print("❌ Column 'contract_type_code' not found in transformed data")
    print()

    # ---------- 7️⃣ Final summary ----------
    print("🎯 Validation complete.\n")
    print("👉 Please review warnings/❌ messages above and fix transform/load if needed.")


if __name__ == "__main__":
    validate_telco()
