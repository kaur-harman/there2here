import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import storage
from io import StringIO


BASE_URL = "https://tradestat.commerce.gov.in/meidb/country_wise_all_commodities_import"

YEAR = int(os.getenv("YEAR", "2025"))
MONTH = int(os.getenv("MONTH", "1"))

BUCKET_NAME = "de-zoomcamp-2026-486917-trade-raw"

REQUEST_TIMEOUT = 60
MAX_RETRIES = 3

session = requests.Session()

gcs_client = storage.Client()
bucket = gcs_client.bucket(BUCKET_NAME)


def upload_to_gcs(local_path, gcs_path):
    """Upload file to GCS if it doesn't already exist."""
    blob = bucket.blob(gcs_path)

    if blob.exists():
        print(f"Skipping upload (already exists): {gcs_path}")
        return

    blob.upload_from_filename(local_path)
    print(f"Uploaded to GCS: {gcs_path}")


def get_token_and_countries():
    """Fetch CSRF token and country list."""
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
    """Fetch trade table with retry logic."""

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

            response = session.post(
                BASE_URL,
                data=payload,
                timeout=REQUEST_TIMEOUT
            )

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
            time.sleep(2)


def clean_country_name(name):
    """Normalize country name for filenames."""
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
    """Save CSV locally and upload to GCS."""

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

    for country_code, country_name in countries.items():

        for metric in [1, 2]:

            job_counter += 1

            print(
                f"[{job_counter}/{total_jobs}] "
                f"{country_name} metric={metric}"
            )

            try:

                df = fetch_table(
                    token,
                    MONTH,
                    YEAR,
                    country_code,
                    metric
                )

                if df is None:
                    print(f"No table returned: {country_name}")
                    continue

                save_csv(df, country_name, MONTH, YEAR, metric)

            except Exception as e:

                print(
                    f"Failed: {country_name} "
                    f"metric={metric} error={e}"
                )

            time.sleep(1) 


if __name__ == "__main__":
    main()