# Pub/Sub ingestion topic + subscription with a dead-letter topic for resilience.

resource "google_pubsub_topic" "trades" {
  name                       = "trades-ingest-${var.env}"
  labels                     = var.labels
  message_retention_duration = "86400s" # 1 day retention for replay
}

# Dead-letter topic: messages that fail repeatedly land here instead of being lost.
resource "google_pubsub_topic" "dlq" {
  name   = "trades-ingest-dlq-${var.env}"
  labels = var.labels
}

resource "google_pubsub_subscription" "trades" {
  name  = "trades-ingest-sub-${var.env}"
  topic = google_pubsub_topic.trades.id

  # No message ordering: Dataflow won't start against an ordering-enabled
  # subscription, and the MERGE is order-independent anyway.

  ack_deadline_seconds       = 60
  message_retention_duration = "86400s"
  retain_acked_messages      = false

  expiration_policy {
    ttl = "" # never expire
  }

  # Resilience: retry with backoff, then route poison messages to the DLQ.
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq.id
    max_delivery_attempts = 5
  }
}

# Subscription on the DLQ so operators can inspect/replay poison messages.
resource "google_pubsub_subscription" "dlq" {
  name                 = "trades-ingest-dlq-sub-${var.env}"
  topic                = google_pubsub_topic.dlq.id
  ack_deadline_seconds = 60
}

# Pub/Sub service account needs publish rights to the DLQ for dead-lettering.
data "google_project" "current" {}

resource "google_pubsub_topic_iam_member" "dlq_publisher" {
  topic  = google_pubsub_topic.dlq.id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription_iam_member" "sub_subscriber" {
  subscription = google_pubsub_subscription.trades.id
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}
