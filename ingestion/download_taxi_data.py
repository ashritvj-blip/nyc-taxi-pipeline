import os
import requests

# ── Config ────────────────────────────────────────────────────
# NYC TLC publishes free taxi data as Parquet files
# We'll download 3 months of Yellow Taxi data
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MONTHS   = ["2024-01", "2024-02", "2024-03"]
BRONZE   = os.path.join(os.path.dirname(__file__), "..", "data", "bronze")

# ── Create bronze folder if it doesn't exist ──────────────────
os.makedirs(BRONZE, exist_ok=True)

# ── Download each file ────────────────────────────────────────
def download_taxi_data():
    for month in MONTHS:
        filename = f"yellow_tripdata_{month}.parquet"
        url      = f"{BASE_URL}/{filename}"
        dest     = os.path.join(BRONZE, filename)

        # Skip if already downloaded
        if os.path.exists(dest):
            print(f"Already exists — skipping: {filename}")
            continue

        print(f"Downloading {filename}...")
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            with open(dest, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Saved to bronze: {filename}")
        else:
            print(f"Failed to download {filename} — status: {response.status_code}")

if __name__ == "__main__":
    download_taxi_data()