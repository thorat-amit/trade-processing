# Email alerting + alert policies for pipeline health.

resource "google_monitoring_notification_channel" "email" {
  display_name = "Trade ETL Alerts (${var.env})"
  type         = "email"
  labels = {
    email_address = var.alert_email
  }
}

# Alert: Dataflow job failed.
resource "google_monitoring_alert_policy" "dataflow_failed" {
  display_name = "Trade ETL - Dataflow job failed (${var.env})"
  combiner     = "OR"
  conditions {
    display_name = "Dataflow job state is failed"
    condition_threshold {
      filter = join(" AND ", [
        "resource.type=\"dataflow_job\"",
        "metric.type=\"dataflow.googleapis.com/job/is_failed\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "60s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }
  notification_channels = [google_monitoring_notification_channel.email.id]
  alert_strategy {
    auto_close = "1800s"
  }
}

# Alert: system lag (event freshness) too high -> pipeline falling behind.
resource "google_monitoring_alert_policy" "dataflow_lag" {
  display_name = "Trade ETL - Dataflow system lag high (${var.env})"
  combiner     = "OR"
  conditions {
    display_name = "System lag > 300s"
    condition_threshold {
      filter = join(" AND ", [
        "resource.type=\"dataflow_job\"",
        "metric.type=\"dataflow.googleapis.com/job/system_lag\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 300
      duration        = "300s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }
  notification_channels = [google_monitoring_notification_channel.email.id]
}

# Alert: messages piling up in the dead-letter queue.
resource "google_monitoring_alert_policy" "dlq_backlog" {
  display_name = "Trade ETL - DLQ backlog (${var.env})"
  combiner     = "OR"
  conditions {
    display_name = "Undelivered messages in DLQ subscription"
    condition_threshold {
      filter = join(" AND ", [
        "resource.type=\"pubsub_subscription\"",
        "resource.label.subscription_id=\"trades-ingest-dlq-sub-${var.env}\"",
        "metric.type=\"pubsub.googleapis.com/subscription/num_undelivered_messages\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "300s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }
  notification_channels = [google_monitoring_notification_channel.email.id]
}
