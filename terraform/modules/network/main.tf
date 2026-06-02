# Private VPC for Dataflow + Composer. Dataflow workers run without public IPs
# (security best practice); egress to Snowflake goes via Cloud NAT.

locals {
  # Named once so the resource block and the outputs always agree (avoids
  # depending on secondary_ip_range list/set ordering).
  pods_range_name     = "composer-pods"
  services_range_name = "composer-services"
}

resource "google_compute_network" "vpc" {
  name                    = "trade-etl-vpc-${var.env}"
  auto_create_subnetworks = false
  project                 = var.project_id
}

resource "google_compute_subnetwork" "subnet" {
  name                     = "trade-etl-subnet-${var.env}"
  ip_cidr_range            = "10.10.0.0/20"
  region                   = var.region
  network                  = google_compute_network.vpc.id
  private_ip_google_access = true # workers reach Google APIs privately

  # Secondary ranges for Composer (GKE) pods/services.
  secondary_ip_range {
    range_name    = local.pods_range_name
    ip_cidr_range = "10.20.0.0/16"
  }
  secondary_ip_range {
    range_name    = local.services_range_name
    ip_cidr_range = "10.30.0.0/20"
  }
}

# Cloud Router + NAT so private-IP workers can reach Snowflake over the internet.
resource "google_compute_router" "router" {
  name    = "trade-etl-router-${var.env}"
  region  = var.region
  network = google_compute_network.vpc.id
}

resource "google_compute_router_nat" "nat" {
  name                               = "trade-etl-nat-${var.env}"
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

# Allow internal communication between Dataflow workers (required by Beam).
resource "google_compute_firewall" "internal" {
  name    = "trade-etl-allow-internal-${var.env}"
  network = google_compute_network.vpc.id

  allow {
    protocol = "tcp"
    ports    = ["12345-12346"] # Dataflow worker-to-worker
  }
  source_ranges = ["10.10.0.0/20"]
}
