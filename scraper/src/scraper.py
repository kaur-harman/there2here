import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import storage

BASE_URL = "https://tradestat.commerce.gov.in/meidb/country_wise_all_commodities_import"


YEAR = int(os.getenv("YEAR", "2025"))
MONTH = int(os.getenv("MONTH", "1"))

BUCKET_NAME = "de-zoomcamp-2026-486917-trade-raw"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0"
})


def upload_to_gcs(local_path, gcs_path):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)

    if blob.exists():
        print(f"Already exists in GCS: {gcs_path}")
        return

    blob.upload_from_filename(local_path)
    print(f"Uploaded: {gcs_path}")


def get_token_and_countries():
    response = session.get(BASE_URL)
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
        "cwcimReportVal": str(metric),  # 1 = USD, 2 = Quantity
        "cwcimReportYear": "2"
    }

    response = session.post(BASE_URL, data=payload)

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"id": "example1"})

    if table is None:
        return None

    df = pd.read_html(str(table))[0]

    return df


def save_csv(df, country, month, year, metric):
    metric_name = "usd" if metric == 1 else "quantity"

    country_clean = country.lower().replace(" ", "_")

    local_dir = f"data/{year}/{str(month).zfill(2)}"
    os.makedirs(local_dir, exist_ok=True)

    filename = f"{country_clean}_{metric_name}.csv"
    local_path = f"{local_dir}/{filename}"

    if os.path.exists(local_path):
        print(f"Already exists locally: {filename}")
        return

    df.to_csv(local_path, index=False)
    print(f"Saved: {local_path}")

    gcs_path = f"raw/{year}/{str(month).zfill(2)}/{filename}"
    upload_to_gcs(local_path, gcs_path)


def main():
    print("Starting scraper")

    token, countries = get_token_and_countries()

    print(f"Countries found: {len(countries)}")

    for country_code, country_name in countries.items():

        for metric in [1, 2]:

            try:
                df = fetch_table(token, MONTH, YEAR, country_code, metric)

                if df is None:
                    print(f"No data: {country_name}")
                    continue

                save_csv(df, country_name, MONTH, YEAR, metric)

            except Exception as e:
                print(f"Failed: {country_name} metric={metric} error={e}")

            time.sleep(1)


if __name__ == "__main__":
    main()