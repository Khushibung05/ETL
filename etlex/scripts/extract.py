# extract.py
import os
import requests

def extract_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data", "raw")
    os.makedirs(data_dir, exist_ok=True)

    # Google Drive file id from the link you provided
    file_id = "1bfEHWnCVRXjSBhxyHllSqyycR3sDoXhC"
    dl_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    raw_path = os.path.join(data_dir, "WA_Fn-UseC_-Telco-Customer-Churn.csv")

    try:
        print(f"🔍 Attempting to download dataset from Google Drive to: {raw_path}")
        resp = requests.get(dl_url, timeout=30)
        resp.raise_for_status()
        with open(raw_path, "wb") as f:
            f.write(resp.content)
        print(f"✅ Data extracted and saved at: {raw_path}")
    except Exception as e:
        # If download fails, check if file exists locally already
        if os.path.exists(raw_path):
            print(f"ℹ️  Download failed but file already exists at: {raw_path}")
        else:
            print(f"⚠️  Could not download dataset automatically: {e}")
            print("ℹ️  Please download the CSV manually from Kaggle or the provided Google Drive link and place it at:")
            print(f"    {raw_path}")
            raise

    return raw_path

if __name__ == "__main__":
    extract_data()
