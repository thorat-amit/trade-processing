variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Primary GCP region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Primary GCP zone"
  type        = string
  default     = "us-central1-a"
}

variable "env" {
  description = "Environment name (dev/stg/prod) used as a resource suffix"
  type        = string
  default     = "dev"
}

variable "alert_email" {
  description = "Email address to receive pipeline failure alerts"
  type        = string
}

variable "labels" {
  description = "Common resource labels"
  type        = map(string)
  default = {
    app     = "trade-etl"
    managed = "terraform"
  }
}

# Snowflake credentials are provided out-of-band and stored in Secret Manager.
# Pass via a tfvars file or -var; NEVER commit real values.
variable "snowflake_credentials_json" {
  description = "JSON blob of Snowflake connection details (account,user,password/private_key,role,warehouse,database,schema)"
  type        = string
  sensitive   = true
}

variable "composer_image_version" {
  description = "Cloud Composer image version"
  type        = string
  default     = "composer-2.9.7-airflow-2.9.3"
}

variable "enable_composer" {
  description = "Whether to provision Cloud Composer (it is the most expensive resource; can disable for local-only testing)"
  type        = bool
  default     = true
}

variable "sendgrid_api_key" {
  description = "Optional SendGrid API key to enable Airflow-native email_on_failure alerts. Leave empty to rely solely on the Cloud Monitoring email alerts (which fully satisfy the alerting requirement). Note: stored in state when set; prefer a secret backend in production."
  type        = string
  default     = ""
  sensitive   = true
}
