import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import time
from io import StringIO
from google.cloud import storage


BASE_URL = "https://tradestat.commerce.gov.in/meidb/country_wise_all_commodities_import"

YEAR = 2025
MONTH = 1

BUCKET_NAME = "de-zoomcamp-2026-486917-trade-raw"

session = requests.Session()


def upload_to_gcs(local_file, gcs_path):

    client = storage.Client()

    bucket = client.bucket(BUCKET_NAME)

    blob = bucket.blob(gcs_path)

    blob.upload_from_filename(local_file)

    print("Uploaded:", gcs_path)



def clean_country(name):

    return (
        name.lower()
        .replace(" ", "_")
        .replace(".", "")
        .replace("'", "")
    )


def get_token():

    response = session.get(BASE_URL)

    soup = BeautifulSoup(response.text, "html.parser")

    token = soup.find("input", {"name": "_token"})["value"]

    return token


def get_countries():

    response = session.get(BASE_URL)

    soup = BeautifulSoup(response.text, "html.parser")

    options = soup.select("#cwcimallcount option")

    countries = {}

    for o in options:

        code = o.get("value")
        name = o.text.strip()

        if code and name:
            countries[name] = code

    print("Countries found:", len(countries))

    return countries


def fetch_table(token, month, year, country_code, metric):

    payload = {
        "_token": token,
        "cwcimMonth": str(month),
        "cwcimYear": str(year),
        "cwcimallcount": str(country_code),
        "cwcimCommodityLevel": "2",
        "cwcimReportVal": str(metric),
        "cwcimReportYear": "2"
    }

    response = session.post(BASE_URL, data=payload)

    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", {"id": "example1"})

    if table is None:
        return None

    df = pd.read_html(StringIO(str(table)))[0]

    return df


def save_csv(df, country, month, year, metric):

    metric_name = "usd" if metric == 1 else "quantity"

    country = clean_country(country)

    folder = f"data/{year}/{str(month).zfill(2)}"

    os.makedirs(folder, exist_ok=True)

    filename = f"{country}_{metric_name}.csv"

    local_path = f"{folder}/{filename}"

    # ---- resume capability ----
    if os.path.exists(local_path):

        print("Skipping (already exists):", filename)

        return
    # ---------------------------

    df.to_csv(local_path, index=False)

    print("Saved:", local_path)

    gcs_path = f"raw/{year}/{str(month).zfill(2)}/{filename}"

    upload_to_gcs(local_path, gcs_path)


def main():

    print("Starting scraper")

    countries = get_countries()

    for country, code in countries.items():

        for metric in [1, 2]:

            token = get_token()

            df = fetch_table(token, MONTH, YEAR, code, metric)

            if df is None:

                print("No data:", country)

                continue

            save_csv(df, country, MONTH, YEAR, metric)

            time.sleep(1)


if __name__ == "__main__":
    main()