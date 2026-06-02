output "topic_id" { value = google_pubsub_topic.trades.id }
output "topic_name" { value = google_pubsub_topic.trades.name }
output "subscription_id" { value = google_pubsub_subscription.trades.id }
output "subscription_name" { value = google_pubsub_subscription.trades.name }
output "dlq_topic_name" { value = google_pubsub_topic.dlq.name }
