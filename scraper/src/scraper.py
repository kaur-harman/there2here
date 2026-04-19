import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import storage
from io import StringIO
from datetime import datetime

BASE_URL = "https://tradestat.commerce.gov.in/meidb/country_wise_all_commodities_import"

YEAR = int(os.getenv("YEAR", "2025"))
MONTH = int(os.getenv("MONTH", "1"))

BUCKET_NAME = "de-zoomcamp-2026-486917-trade-raw"
REQUEST_TIMEOUT = 60
MAX_RETRIES = 3

session = requests.Session()

CREDENTIALS_PATH = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "/app/credentials/service-account.json"
)

gcs_client = storage.Client.from_service_account_json(CREDENTIALS_PATH)
bucket = gcs_client.bucket(BUCKET_NAME)

# Log file path inside container
LOG_DIR = f"/app/logs/{YEAR}/{str(MONTH).zfill(2)}"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = f"{LOG_DIR}/failed.log"


def log_failure(country, metric, reason):
    """Write failed scrape to log file and upload to GCS."""
    timestamp = datetime.utcnow().isoformat()
    metric_name = "usd" if metric == 1 else "quantity"
    line = f"{timestamp} | {country} | {metric_name} | {reason}\n"
    with open(LOG_FILE, "a") as f:
        f.write(line)


def upload_log_to_gcs():
    """Upload the failure log to GCS at end of run."""
    if not os.path.exists(LOG_FILE):
        return
    gcs_path = f"logs/{YEAR}/{str(MONTH).zfill(2)}/failed.log"
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(LOG_FILE)
    print(f"Failure log uploaded to GCS: {gcs_path}")


def upload_to_gcs(local_path, gcs_path):
    blob = bucket.blob(gcs_path)
    if blob.exists():
        print(f"Skipping upload (already exists): {gcs_path}")
        return
    blob.upload_from_filename(local_path)
    print(f"Uploaded to GCS: {gcs_path}")


def get_token_and_countries():
    response = session.get(BASE_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    token = soup.find("input", {"name": "_token"})["value"]
    country_select = soup.find("select", {"name": "cwcimallcount"})
    countries = {}
    for option in country_select.find_all("option"):
        value = option.get("value")
        name = option.text.strip()
        if value and value != "0":
            countries[value] = name
    return token, countries


def fetch_table(token, month, year, country_code, metric):
    payload = {
        "_token": token,
        "cwcimMonth": str(month),
        "cwcimYear": str(year),
        "cwcimallcount": country_code,
        "cwcimCommodityLevel": "2",
        "cwcimReportVal": str(metric),
        "cwcimReportYear": "2"
    }
    for attempt in range(MAX_RETRIES):
        try:
            response = session.post(BASE_URL, data=payload, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table", {"id": "example1"})
            if table is None:
                return None
            df = pd.read_html(StringIO(str(table)))[0]
            return df
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise e
            print(f"Retry {attempt+1}/{MAX_RETRIES} for {country_code}")
            time.sleep(5)


def clean_country_name(name):
    return (
        name.lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "")
        .replace("(", "")
        .replace(")", "")
        .replace(",", "")
    )


def save_csv(df, country, month, year, metric):
    metric_name = "usd" if metric == 1 else "quantity"
    country_clean = clean_country_name(country)
    local_dir = f"data/{year}/{str(month).zfill(2)}"
    os.makedirs(local_dir, exist_ok=True)
    filename = f"{country_clean}_{metric_name}.csv"
    local_path = f"{local_dir}/{filename}"
    df.to_csv(local_path, index=False)
    print(f"Saved locally: {local_path}")
    gcs_path = f"raw/{year}/{str(month).zfill(2)}/{filename}"
    upload_to_gcs(local_path, gcs_path)


def main():
    print("=" * 60)
    print(f"Starting scraper for {YEAR}-{MONTH:02d}")
    print("=" * 60)

    token, countries = get_token_and_countries()
    print(f"Countries discovered: {len(countries)}")

    total_jobs = len(countries) * 2
    job_counter = 0
    success_count = 0
    fail_count = 0

    for country_code, country_name in countries.items():
        for metric in [1, 2]:
            job_counter += 1
            print(f"[{job_counter}/{total_jobs}] {country_name} metric={metric}")

            # Refresh token before every request
            try:
                token, _ = get_token_and_countries()
            except Exception as e:
                reason = f"Token refresh failed: {e}"
                print(reason)
                log_failure(country_name, metric, reason)
                fail_count += 1
                time.sleep(5)
                continue

            try:
                df = fetch_table(token, MONTH, YEAR, country_code, metric)
                if df is None:
                    reason = "No table returned"
                    print(f"{reason}: {country_name}")
                    log_failure(country_name, metric, reason)
                    fail_count += 1
                    continue
                save_csv(df, country_name, MONTH, YEAR, metric)
                success_count += 1
            except Exception as e:
                reason = str(e)
                print(f"Failed: {country_name} metric={metric} error={reason}")
                log_failure(country_name, metric, reason)
                fail_count += 1

            time.sleep(2)

    # Upload log to GCS
    upload_log_to_gcs()

    # Final summary
    print("\n" + "=" * 60)
    print(f"COMPLETED: {YEAR}-{MONTH:02d}")
    print(f" Success: {success_count}/{total_jobs}")
    print(f" Failed:  {fail_count}/{total_jobs}")
    print(f"Log saved to GCS: logs/{YEAR}/{str(MONTH).zfill(2)}/failed.log")
    print("=" * 60)


if __name__ == "__main__":
    main()