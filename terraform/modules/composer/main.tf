# Cloud Composer 2 environment for orchestration. Small/resilient config.

locals {
  # SendGrid is optional - wire it up only if a key is provided, otherwise rely
  # on the Cloud Monitoring email alerts. Avoids a broken email backend that
  # would only fail at the first task failure.
  sendgrid_enabled = var.sendgrid_api_key != ""

  base_env = {
    # Read by the DAG at parse time via os.environ.
    ALERT_EMAIL       = var.alert_email
    SNOWFLAKE_CONN_ID = "snowflake_default"
  }
  sendgrid_env = local.sendgrid_enabled ? {
    SENDGRID_API_KEY   = var.sendgrid_api_key
    SENDGRID_MAIL_FROM = var.alert_email
  } : {}

  base_pypi = {
    "apache-airflow-providers-snowflake" = ""
    "snowflake-connector-python"         = ""
    # Used by the DAG's health-check callable (googleapiclient.discovery.build).
    "google-api-python-client" = ""
  }
  sendgrid_pypi = local.sendgrid_enabled ? { "apache-airflow-providers-sendgrid" = "" } : {}

  overrides = local.sendgrid_enabled ? {
    "email-email_backend" = "airflow.providers.sendgrid.utils.emailer.send_email"
  } : {}
}

resource "google_composer_environment" "this" {
  count   = var.enable_composer ? 1 : 0
  name    = "trade-etl-composer-${var.env}"
  region  = var.region
  project = var.project_id

  config {
    software_config {
      image_version = var.image_version

      airflow_config_overrides = local.overrides
      env_variables            = merge(local.base_env, local.sendgrid_env)
      pypi_packages            = merge(local.base_pypi, local.sendgrid_pypi)
    }

    # Small environment = cost-effective; autoscaling gives resilience.
    workloads_config {
      scheduler {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
        min_count  = 1
        max_count  = 3
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      network         = var.network_id
      subnetwork      = var.subnet_id
      service_account = var.composer_sa_email

      # Use the subnet's named secondary ranges for GKE pods/services rather
      # than letting Composer auto-allocate.
      ip_allocation_policy {
        cluster_secondary_range_name  = var.pods_range_name
        services_secondary_range_name = var.services_range_name
      }
    }
  }

  labels = var.labels
}
