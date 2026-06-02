# GCS buckets: one for Dataflow temp/staging + Flex Template, one for misc artifacts.
resource "google_storage_bucket" "dataflow" {
  name                        = "${var.project_id}-trade-etl-dataflow-${var.env}"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = var.labels

  # Cost control: expire temp objects automatically.
  lifecycle_rule {
    condition { age = 7 }
    action { type = "Delete" }
  }
}

resource "google_storage_bucket" "artifacts" {
  name                        = "${var.project_id}-trade-etl-artifacts-${var.env}"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = var.labels

  versioning { enabled = true }
}
