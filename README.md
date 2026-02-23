# **There to Here: Mapping Imports to India**

This project aims to build a cloud-native data engineering pipeline to analyse India’s monthly import trends over the last 2 years.

The goal is to demonstrate production-style data engineering practices including:

- Infrastructure provisioning using Terraform
- Cloud data lake setup
- Cloud data warehouse setup
- Reproducible project architecture


---

## 🗂 Dataset

Source: Directorate General of Commercial Intelligence and Statistics (DGCIS)  
Report: Import – Country-wise All Commodities  

Planned configuration:
- HS Code Level: 2-digit
- Frequency: Monthly
- Year Type: Calendar Year
- Period: Last 2 years

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

## 🛠 Tech Stack (So Far)

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

