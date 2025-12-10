# transform.py
import os
import pandas as pd
import numpy as np

def transform_data(raw_path):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)

    df = pd.read_csv(raw_path)

    # --- Cleaning Tasks ---
    # Convert TotalCharges to numeric (spaces become NaN)
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")

    # Fill missing numeric values using median for tenure, MonthlyCharges, TotalCharges
    for col in ["tenure", "MonthlyCharges", "TotalCharges"]:
        if col in df.columns:
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)

    # Replace missing categorical values with "Unknown"
    # Consider object (string) columns and categorical-like columns
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].fillna("Unknown")
    # If some categorical columns are not object dtype, handle them too
    for col in ["InternetService", "MultipleLines", "Contract"]:
        if col in df.columns and df[col].isnull().any():
            df[col] = df[col].fillna("Unknown")

    # --- Feature Engineering ---
    # 1. tenure_group
    def tenure_group_fn(t):
        if t <= 12:
            return "New"
        elif 13 <= t <= 36:
            return "Regular"
        elif 37 <= t <= 60:
            return "Loyal"
        else:
            return "Champion"

    df["tenure_group"] = df["tenure"].apply(tenure_group_fn)

    # 2. monthly_charge_segment
    def monthly_segment(m):
        if m < 30:
            return "Low"
        elif 30 <= m <= 70:
            return "Medium"
        else:
            return "High"

    df["monthly_charge_segment"] = df["MonthlyCharges"].apply(monthly_segment)

    # 3. has_internet_service
    # "DSL" / "Fiber optic" -> 1 ; "No" -> 0 ; unknown/others -> 0
    df["has_internet_service"] = df["InternetService"].apply(
        lambda x: 1 if str(x).strip().lower() in {"dsl", "fiber optic", "fiber"} else 0
    )

    # 4. is_multi_line_user
    df["is_multi_line_user"] = df["MultipleLines"].apply(lambda x: 1 if str(x).strip().lower() == "yes" else 0)

    # 5. contract_type_code
    mapping = {
        "Month-to-month": 0,
        "One year": 1,
        "Two year": 2,
        # Add unknown/default mapping:
        "Unknown": None
    }
    # Normalize strings and map
    df["Contract"] = df["Contract"].astype(str)
    df["contract_type_code"] = df["Contract"].apply(lambda x: mapping.get(x, None))

    # --- Drop unnecessary fields ---
    # Remove: customerID, gender
    df.drop(columns=["customerID", "gender"], inplace=True, errors="ignore")

    # --- Save transformed data ---
    staged_path = os.path.join(staged_dir, "telco_transformed.csv")
    # Ensure no numpy NaN remain as raw NaN (we'll keep them; loader will convert NaN->None)
    df.to_csv(staged_path, index=False)
    print(f"✅ Data transformed and saved at: {staged_path}")
    return staged_path

if __name__ == "__main__":
    # assume extract.py saved raw CSV to data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_path = os.path.join(base_dir, "data", "raw", "WA_Fn-UseC_-Telco-Customer-Churn.csv")
    if not os.path.exists(raw_path):
        # try the other name
        raw_path = os.path.join(base_dir, "data", "raw", "telco_raw.csv")
        if not os.path.exists(raw_path):
            raise FileNotFoundError(f"Raw CSV not found at expected locations. Please run extract.py or place the CSV at {raw_path}")
    transform_data(raw_path)
