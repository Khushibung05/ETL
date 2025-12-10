# =============================
# etl_analysis.py
# =============================
# Purpose:
# Read Telco Customer Churn table from Supabase,
# Perform metrics analysis,
# Produce summary CSV and optional visualizations
# =============================

import os
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from supabase import create_client, Client

# ---------------------------------------------------------
# Initialize Supabase Client
# ---------------------------------------------------------
def get_supabase_client() -> Client:
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ SUPABASE_URL or SUPABASE_KEY missing in .env")

    return create_client(url, key)

# ---------------------------------------------------------
# Read entire table from Supabase
# ---------------------------------------------------------
def fetch_data(table: str = "telco_customer_churn") -> pd.DataFrame:
    supabase = get_supabase_client()

    print("📥 Fetching data from Supabase...")
    response = supabase.table(table).select("*").execute()

    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"❌ Error fetching data: {response.error}")

    df = pd.DataFrame(response.data)
    print(f"✅ Retrieved {len(df)} rows.")
    return df

# ---------------------------------------------------------
# Perform Analysis
# ---------------------------------------------------------
def generate_analysis(df: pd.DataFrame):
    analysis = {}

    # 1️⃣ Churn percentage
    churn_rate = (df["churn"].str.lower() == "yes").mean() * 100
    analysis["churn_percentage"] = round(churn_rate, 2)

    # 2️⃣ Average monthly charges per contract
    avg_monthly_contract = df.groupby("contract")["monthlycharges"].mean()

    # 3️⃣ Customer tenure groups count
    tenure_counts = df["tenure_group"].value_counts()

    # 4️⃣ Internet service distribution
    internet_dist = df["internetservice"].value_counts()

    # 5️⃣ Pivot: Churn vs Tenure Group
    churn_tenure_pivot = pd.crosstab(df["tenure_group"], df["churn"])

    # Build final summary dict → DataFrame
    summary = {
        "Metric": [
            "Churn Percentage",
            "New Customers",
            "Regular Customers",
            "Loyal Customers",
            "Champion Customers",
        ],
        "Value": [
            f"{analysis['churn_percentage']}%",
            tenure_counts.get("New", 0),
            tenure_counts.get("Regular", 0),
            tenure_counts.get("Loyal", 0),
            tenure_counts.get("Champion", 0),
        ]
    }

    summary_df = pd.DataFrame(summary)

    return summary_df, avg_monthly_contract, internet_dist, churn_tenure_pivot

# ---------------------------------------------------------
# Save CSV Output
# ---------------------------------------------------------
def save_summary_csv(summary_df):
    processed_dir = os.path.join("..", "data", "processed")
    os.makedirs(processed_dir, exist_ok=True)

    output_path = os.path.join(processed_dir, "analysis_summary.csv")
    summary_df.to_csv(output_path, index=False)

    print(f"📁 Summary saved to: {output_path}")

# ---------------------------------------------------------
# Optional Visualizations
# ---------------------------------------------------------
def create_visualizations(df):
    viz_dir = os.path.join("..", "data", "processed", "plots")
    os.makedirs(viz_dir, exist_ok=True)

    # 🔸 Churn Rate by Monthly Charge Segment
    churn_by_segment = df.groupby("monthly_charge_segment")["churn"].apply(
        lambda x: (x.str.lower() == "yes").mean() * 100
    )

    plt.figure()
    churn_by_segment.plot(kind="bar")
    plt.title("Churn Rate by Monthly Charge Segment")
    plt.ylabel("Churn %")
    plt.savefig(os.path.join(viz_dir, "churn_by_segment.png"))
    plt.close()

    # 🔸 Histogram of TotalCharges
    plt.figure()
    df["totalcharges"].hist(bins=30)
    plt.title("Distribution of Total Charges")
    plt.xlabel("Total Charges")
    plt.ylabel("Frequency")
    plt.savefig(os.path.join(viz_dir, "totalcharges_hist.png"))
    plt.close()

    # 🔸 Bar plot of Contract Types
    plt.figure()
    df["contract"].value_counts().plot(kind="bar")
    plt.title("Contract Type Distribution")
    plt.ylabel("Count")
    plt.savefig(os.path.join(viz_dir, "contract_distribution.png"))
    plt.close()

    print("📊 Visualizations saved in /data/processed/plots")

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
if __name__ == "__main__":
    df = fetch_data()
    summary_df, avg_monthly_contract, internet_dist, pivot = generate_analysis(df)

    print("\n======= SUMMARY REPORT =======")
    print(summary_df)
    print("\nAverage Monthly Charges per Contract:")
    print(avg_monthly_contract)
    print("\nInternet Service Distribution:")
    print(internet_dist)
    print("\nChurn vs Tenure Group Pivot:")
    print(pivot)

    save_summary_csv(summary_df)
    create_visualizations(df)

    print("\n🎉 Analysis Completed Successfully!")
