output "dataflow_sa_email" { value = google_service_account.dataflow.email }
output "composer_sa_email" { value = google_service_account.composer.email }
output "snowflake_secret_id" { value = google_secret_manager_secret.snowflake.id }
output "snowflake_secret_resource" { value = "${google_secret_manager_secret.snowflake.id}/versions/latest" }
