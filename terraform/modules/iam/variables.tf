variable "project_id" { type = string }
variable "env" { type = string }
variable "dataflow_bucket" { type = string }
variable "snowflake_credentials_json" {
  type      = string
  sensitive = true
}
