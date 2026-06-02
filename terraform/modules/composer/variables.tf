variable "project_id" { type = string }
variable "region" { type = string }
variable "env" { type = string }
variable "enable_composer" { type = bool }
variable "image_version" { type = string }
variable "network_id" { type = string }
variable "subnet_id" { type = string }
variable "composer_sa_email" { type = string }
variable "labels" { type = map(string) }

variable "alert_email" {
  description = "Email surfaced to the DAG (ALERT_EMAIL env var) for email_on_failure"
  type        = string
}

variable "pods_range_name" {
  description = "Name of the subnet secondary range for GKE pods"
  type        = string
}

variable "services_range_name" {
  description = "Name of the subnet secondary range for GKE services"
  type        = string
}

variable "sendgrid_api_key" {
  description = "Optional SendGrid API key to enable Airflow-native email alerts. If empty, rely on Cloud Monitoring email alerts."
  type        = string
  default     = ""
  sensitive   = true
}
