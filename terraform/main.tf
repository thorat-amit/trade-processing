# =====================================================================
# Trade ETL Pipeline - root Terraform configuration
# =====================================================================

# --- Enable required GCP APIs ---
locals {
  required_apis = [
    "compute.googleapis.com",
    "dataflow.googleapis.com",
    "pubsub.googleapis.com",
    "secretmanager.googleapis.com",
    "composer.googleapis.com",
    "storage.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "iam.googleapis.com",
  ]
}

resource "google_project_service" "apis" {
  for_each           = toset(local.required_apis)
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# --- Networking ---
module "network" {
  source     = "./modules/network"
  project_id = var.project_id
  region     = var.region
  env        = var.env

  depends_on = [google_project_service.apis]
}

# --- Storage ---
module "storage" {
  source     = "./modules/storage"
  project_id = var.project_id
  region     = var.region
  env        = var.env
  labels     = var.labels

  depends_on = [google_project_service.apis]
}

# --- Pub/Sub ---
module "pubsub" {
  source = "./modules/pubsub"
  env    = var.env
  labels = var.labels

  depends_on = [google_project_service.apis]
}

# --- IAM + Secrets ---
module "iam" {
  source                     = "./modules/iam"
  project_id                 = var.project_id
  env                        = var.env
  dataflow_bucket            = module.storage.dataflow_bucket
  snowflake_credentials_json = var.snowflake_credentials_json

  depends_on = [google_project_service.apis, module.storage]
}

# --- Monitoring + Alerting ---
module "monitoring" {
  source      = "./modules/monitoring"
  env         = var.env
  alert_email = var.alert_email

  depends_on = [google_project_service.apis]
}

# --- Orchestration (Cloud Composer) ---
module "composer" {
  source              = "./modules/composer"
  project_id          = var.project_id
  region              = var.region
  env                 = var.env
  enable_composer     = var.enable_composer
  image_version       = var.composer_image_version
  network_id          = module.network.network_id
  subnet_id           = module.network.subnet_id
  composer_sa_email   = module.iam.composer_sa_email
  alert_email         = var.alert_email
  pods_range_name     = module.network.composer_pods_range_name
  services_range_name = module.network.composer_services_range_name
  sendgrid_api_key    = var.sendgrid_api_key
  labels              = var.labels

  depends_on = [google_project_service.apis, module.network, module.iam]
}
