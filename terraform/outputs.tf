output "bucket_name" {
  value = google_storage_bucket.trade_raw_bucket.name
}

output "dataset_id" {
  value = google_bigquery_dataset.trade_dataset.dataset_id
}