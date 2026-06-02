output "dataflow_bucket" { value = google_storage_bucket.dataflow.name }
output "dataflow_temp_location" { value = "gs://${google_storage_bucket.dataflow.name}/temp" }
output "dataflow_staging_location" { value = "gs://${google_storage_bucket.dataflow.name}/staging" }
output "artifacts_bucket" { value = google_storage_bucket.artifacts.name }
