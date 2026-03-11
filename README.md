# **There to Here: Mapping Imports to India**

## Project Overview

This project builds an **end-to-end cloud-native data engineering pipeline** to analyze India's import dependencies.

### Core Question

As India moves toward becoming a **$5T economy**, key questions arise:

- Where is India sourcing its imports from?
- What commodities are being imported?
- Are these dependencies shifting over time?

This pipeline collects, stores, and prepares trade data to answer these questions.


---

### Architecture
```mermaid
graph TD
    A[Kestra Orchestrator] -->|Triggers| B(Dockerized Python Scraper)
    B -->|Uploads CSVs| C[Google Cloud Storage <br/> Raw Data Lake]
    C -->|Loads to| D[BigQuery <br/> Data Warehouse]
    D -->|Transforms via| E[dbt]
    E -->|Feeds| F[Analytics Dashboard]
  ```


## 🗂 Dataset

Source: Directorate General of Commercial Intelligence and Statistics (DGCIS)  
Report: Import – Country-wise All Commodities; https://tradestat.commerce.gov.in/meidb/

Configuration:

- HS Code Level: **2-digit**
- Frequency: **Monthly**
- Metrics: **Import Value (USD)** and **Quantity**
- Coverage: **All countries**
- Time Period: **Recent monthly data**

---


###  Infrastructure Provisioned via Terraform

The following resources have been created using Infrastructure as Code:

- **Google Cloud Storage bucket**  
  Used as the raw data lake.

- **BigQuery dataset: `india_trade_warehouse`**  
  Used as the data warehouse layer.

Region: `asia-south2` (Delhi)

All infrastructure was provisioned using Terraform and is fully reproducible.

---
## ☁ Cloud Setup Details

### Terraform Resources

Defined in `/terraform`:

- `google_storage_bucket`
- `google_bigquery_dataset`

The infrastructure can be recreated by running:
```
cd terraform
terraform init
terraform apply
```

---

## 🛠 Tech Stack

- Terraform (Infrastructure as Code)
- Google Cloud Storage (Data Lake)
- BigQuery (Data Warehouse)
- Google Cloud Platform (GCP)

---

## 🔁 Reproducibility Guide

### Prerequisites

- Google Cloud account (billing enabled)
- gcloud CLI installed
- Terraform installed

---

### 1️⃣ Set Active Project
```
gcloud config set project <your-project-id>
```

---

### 2️⃣ Authenticate

```
gcloud auth application-default login
```

---

### 3️⃣ Provision Infrastructure

```
cd terraform
terraform init
terraform apply
```

This will create:
- A GCS bucket for raw data storage
- A BigQuery dataset for analytics

---
### Data Ingestion Pipeline

Trade data is collected using a **Python scraper**.

The scraper:

- Discovers all countries dynamically from the source website
- Retrieves import tables for each country
- Extracts both **USD value** and **quantity metrics**
- Saves results as CSV files
- Uploads files to **Google Cloud Storage**
