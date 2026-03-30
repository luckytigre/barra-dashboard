terraform {
  required_providers {
    cloudflare = {
      source = "cloudflare/cloudflare"
    }
    google = {
      source = "hashicorp/google"
    }
  }
}

data "cloudflare_zone" "ceiora" {
  count = var.enabled ? 1 : 0

  filter = {
    name   = var.cloudflare_zone_name
    match  = "all"
    status = "active"
  }
}

resource "google_compute_global_address" "cloud_app" {
  count = var.enabled ? 1 : 0

  name    = "${var.name_prefix}-cloud-app-ip"
  project = var.project_id
}

resource "google_compute_region_network_endpoint_group" "frontend" {
  count = var.enabled ? 1 : 0

  name                  = "${var.name_prefix}-frontend-neg"
  project               = var.project_id
  region                = var.region
  network_endpoint_type = "SERVERLESS"

  cloud_run {
    service = var.service_names.frontend
  }
}

resource "google_compute_region_network_endpoint_group" "serve" {
  count = var.enabled ? 1 : 0

  name                  = "${var.name_prefix}-serve-neg"
  project               = var.project_id
  region                = var.region
  network_endpoint_type = "SERVERLESS"

  cloud_run {
    service = var.service_names.serve
  }
}

resource "google_compute_region_network_endpoint_group" "control" {
  count = var.enabled ? 1 : 0

  name                  = "${var.name_prefix}-control-neg"
  project               = var.project_id
  region                = var.region
  network_endpoint_type = "SERVERLESS"

  cloud_run {
    service = var.service_names.control
  }
}

resource "google_compute_backend_service" "frontend" {
  count = var.enabled ? 1 : 0

  name                  = "${var.name_prefix}-frontend-backend"
  project               = var.project_id
  protocol              = "HTTP"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  timeout_sec           = 30
  enable_cdn            = false

  backend {
    group = google_compute_region_network_endpoint_group.frontend[0].id
  }
}

resource "google_compute_backend_service" "serve" {
  count = var.enabled ? 1 : 0

  name                  = "${var.name_prefix}-serve-backend"
  project               = var.project_id
  protocol              = "HTTP"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  timeout_sec           = 30
  enable_cdn            = false

  backend {
    group = google_compute_region_network_endpoint_group.serve[0].id
  }
}

resource "google_compute_backend_service" "control" {
  count = var.enabled ? 1 : 0

  name                  = "${var.name_prefix}-control-backend"
  project               = var.project_id
  protocol              = "HTTP"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  timeout_sec           = 30
  enable_cdn            = false

  backend {
    group = google_compute_region_network_endpoint_group.control[0].id
  }
}

resource "google_compute_managed_ssl_certificate" "cloud_app" {
  count = var.enabled ? 1 : 0

  name    = "${var.name_prefix}-cloud-app-cert"
  project = var.project_id

  managed {
    domains = [
      var.hostnames.frontend,
      var.hostnames.serve,
      var.hostnames.control,
    ]
  }
}

resource "google_compute_url_map" "https" {
  count = var.enabled ? 1 : 0

  name            = "${var.name_prefix}-https-map"
  project         = var.project_id
  default_service = google_compute_backend_service.frontend[0].id

  host_rule {
    hosts        = [var.hostnames.frontend]
    path_matcher = "frontend"
  }

  host_rule {
    hosts        = [var.hostnames.serve]
    path_matcher = "serve"
  }

  host_rule {
    hosts        = [var.hostnames.control]
    path_matcher = "control"
  }

  path_matcher {
    name            = "frontend"
    default_service = google_compute_backend_service.frontend[0].id
  }

  path_matcher {
    name            = "serve"
    default_service = google_compute_backend_service.serve[0].id
  }

  path_matcher {
    name            = "control"
    default_service = google_compute_backend_service.control[0].id
  }
}

resource "google_compute_target_https_proxy" "https" {
  count = var.enabled ? 1 : 0

  name             = "${var.name_prefix}-https-proxy"
  project          = var.project_id
  url_map          = google_compute_url_map.https[0].id
  ssl_certificates = [google_compute_managed_ssl_certificate.cloud_app[0].id]
}

resource "google_compute_global_forwarding_rule" "https" {
  count = var.enabled ? 1 : 0

  name                  = "${var.name_prefix}-https-forwarding"
  project               = var.project_id
  ip_address            = google_compute_global_address.cloud_app[0].id
  load_balancing_scheme = "EXTERNAL_MANAGED"
  port_range            = "443"
  target                = google_compute_target_https_proxy.https[0].id
}

resource "google_compute_url_map" "http_redirect" {
  count = var.enabled ? 1 : 0

  name    = "${var.name_prefix}-http-redirect"
  project = var.project_id

  default_url_redirect {
    https_redirect         = true
    redirect_response_code = "MOVED_PERMANENTLY_DEFAULT"
    strip_query            = false
  }
}

resource "google_compute_target_http_proxy" "http_redirect" {
  count = var.enabled ? 1 : 0

  name    = "${var.name_prefix}-http-redirect-proxy"
  project = var.project_id
  url_map = google_compute_url_map.http_redirect[0].id
}

resource "google_compute_global_forwarding_rule" "http_redirect" {
  count = var.enabled ? 1 : 0

  name                  = "${var.name_prefix}-http-forwarding"
  project               = var.project_id
  ip_address            = google_compute_global_address.cloud_app[0].id
  load_balancing_scheme = "EXTERNAL_MANAGED"
  port_range            = "80"
  target                = google_compute_target_http_proxy.http_redirect[0].id
}

resource "cloudflare_dns_record" "frontend" {
  count = var.enabled ? 1 : 0

  zone_id = data.cloudflare_zone.ceiora[0].zone_id
  name    = "app"
  type    = "A"
  ttl     = 1
  content = google_compute_global_address.cloud_app[0].address
  proxied = var.cloudflare_proxied
  comment = "Ceiora frontend HTTPS load balancer"
}

resource "cloudflare_dns_record" "serve" {
  count = var.enabled ? 1 : 0

  zone_id = data.cloudflare_zone.ceiora[0].zone_id
  name    = "api"
  type    = "A"
  ttl     = 1
  content = google_compute_global_address.cloud_app[0].address
  proxied = var.cloudflare_proxied
  comment = "Ceiora serve HTTPS load balancer"
}

resource "cloudflare_dns_record" "control" {
  count = var.enabled ? 1 : 0

  zone_id = data.cloudflare_zone.ceiora[0].zone_id
  name    = "control"
  type    = "A"
  ttl     = 1
  content = google_compute_global_address.cloud_app[0].address
  proxied = var.cloudflare_proxied
  comment = "Ceiora control HTTPS load balancer"
}
