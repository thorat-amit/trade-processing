output "dag_gcs_prefix" {
  value = var.enable_composer ? google_composer_environment.this[0].config[0].dag_gcs_prefix : ""
}
output "airflow_uri" {
  value = var.enable_composer ? google_composer_environment.this[0].config[0].airflow_uri : ""
}
