output "network_id" { value = google_compute_network.vpc.id }
output "network_name" { value = google_compute_network.vpc.name }
output "subnet_id" { value = google_compute_subnetwork.subnet.id }
output "subnet_self_link" { value = google_compute_subnetwork.subnet.self_link }

# Names of the subnet secondary ranges, consumed by the Composer module's
# ip_allocation_policy so the dedicated ranges are actually used.
output "composer_pods_range_name" { value = local.pods_range_name }
output "composer_services_range_name" { value = local.services_range_name }
