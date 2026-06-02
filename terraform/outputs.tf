output "network_name" { value = module.network.network_name }
output "subnet_self_link" { value = module.network.subnet_self_link }

output "dataflow_bucket" { value = module.storage.dataflow_bucket }
output "dataflow_temp_location" { value = module.storage.dataflow_temp_location }
output "dataflow_staging_location" { value = module.storage.dataflow_staging_location }
output "artifacts_bucket" { value = module.storage.artifacts_bucket }

output "pubsub_topic" { value = module.pubsub.topic_name }
output "pubsub_subscription" { value = module.pubsub.subscription_name }
output "pubsub_subscription_id" { value = module.pubsub.subscription_id }
output "pubsub_dlq_topic" { value = module.pubsub.dlq_topic_name }

output "dataflow_sa_email" { value = module.iam.dataflow_sa_email }
output "composer_sa_email" { value = module.iam.composer_sa_email }
output "snowflake_secret_resource" { value = module.iam.snowflake_secret_resource }

output "airflow_uri" { value = module.composer.airflow_uri }
output "dag_gcs_prefix" { value = module.composer.dag_gcs_prefix }

output "alert_notification_channel" { value = module.monitoring.notification_channel_id }
