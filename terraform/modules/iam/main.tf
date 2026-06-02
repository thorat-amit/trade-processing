# Dedicated, least-privilege service accounts for Dataflow and Composer,
# plus the Snowflake credentials secret.

# --- Dataflow worker SA ---
resource "google_service_account" "dataflow" {
  account_id   = "trade-etl-dataflow-${var.env}"
  display_name = "Trade ETL Dataflow worker SA"
}

# Roles the Dataflow worker needs (least privilege).
resource "google_project_iam_member" "df_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow.email}"
}
resource "google_project_iam_member" "df_pubsub" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.dataflow.email}"
}
resource "google_project_iam_member" "df_pubsub_viewer" {
  project = var.project_id
  role    = "roles/pubsub.viewer"
  member  = "serviceAccount:${google_service_account.dataflow.email}"
}

# Secret access for Snowflake creds (scoped to the specific secret below).
resource "google_secret_manager_secret" "snowflake" {
  secret_id = "trade-etl-snowflake-${var.env}"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "snowflake" {
  secret      = google_secret_manager_secret.snowflake.id
  secret_data = var.snowflake_credentials_json
}

resource "google_secret_manager_secret_iam_member" "df_secret_access" {
  secret_id = google_secret_manager_secret.snowflake.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.dataflow.email}"
}

# Dataflow worker must read/write the temp bucket.
resource "google_storage_bucket_iam_member" "df_bucket" {
  bucket = var.dataflow_bucket
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow.email}"
}

# Dataflow worker must pull the Flex Template container image from Artifact Registry.
resource "google_project_iam_member" "df_artifactregistry" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.dataflow.email}"
}

# --- Composer SA ---
resource "google_service_account" "composer" {
  account_id   = "trade-etl-composer-${var.env}"
  display_name = "Trade ETL Composer SA"
}

resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer.email}"
}
# Composer needs to launch/monitor Dataflow jobs.
resource "google_project_iam_member" "composer_dataflow_admin" {
  project = var.project_id
  role    = "roles/dataflow.admin"
  member  = "serviceAccount:${google_service_account.composer.email}"
}
# Allow Composer SA to act as the Dataflow worker SA when launching jobs.
resource "google_service_account_iam_member" "composer_act_as_df" {
  service_account_id = google_service_account.dataflow.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.composer.email}"
}
resource "google_secret_manager_secret_iam_member" "composer_secret_access" {
  secret_id = google_secret_manager_secret.snowflake.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.composer.email}"
}
