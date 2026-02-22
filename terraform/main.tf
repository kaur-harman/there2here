resource "google_storage_bucket" "trade_raw_bucket" {
  name     = "${var.project_id}-trade-raw"
  location = var.region

  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "trade_dataset" {
  dataset_id = "india_trade_warehouse"
  location   = var.region
}