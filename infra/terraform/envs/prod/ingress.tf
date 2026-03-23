data "cloudflare_zone" "ceiora" {
  filter = {
    name   = var.cloudflare_zone_name
    match  = "all"
    status = "active"
  }
}

resource "google_compute_global_address" "cloud_app" {
  name    = "${local.name_prefix}-cloud-app-ip"
  project = var.project_id
}

resource "google_compute_region_network_endpoint_group" "frontend" {
  name                  = "${local.name_prefix}-frontend-neg"
  project               = var.project_id
  region                = var.region
  network_endpoint_type = "SERVERLESS"

  cloud_run {
    service = google_cloud_run_v2_service.frontend.name
  }
}

resource "google_compute_region_network_endpoint_group" "serve" {
  name                  = "${local.name_prefix}-serve-neg"
  project               = var.project_id
  region                = var.region
  network_endpoint_type = "SERVERLESS"

  cloud_run {
    service = google_cloud_run_v2_service.serve.name
  }
}

resource "google_compute_region_network_endpoint_group" "control" {
  name                  = "${local.name_prefix}-control-neg"
  project               = var.project_id
  region                = var.region
  network_endpoint_type = "SERVERLESS"

  cloud_run {
    service = google_cloud_run_v2_service.control.name
  }
}

resource "google_compute_backend_service" "frontend" {
  name                  = "${local.name_prefix}-frontend-backend"
  project               = var.project_id
  protocol              = "HTTP"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  timeout_sec           = 30
  enable_cdn            = false

  backend {
    group = google_compute_region_network_endpoint_group.frontend.id
  }
}

resource "google_compute_backend_service" "serve" {
  name                  = "${local.name_prefix}-serve-backend"
  project               = var.project_id
  protocol              = "HTTP"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  timeout_sec           = 30
  enable_cdn            = false

  backend {
    group = google_compute_region_network_endpoint_group.serve.id
  }
}

resource "google_compute_backend_service" "control" {
  name                  = "${local.name_prefix}-control-backend"
  project               = var.project_id
  protocol              = "HTTP"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  timeout_sec           = 30
  enable_cdn            = false

  backend {
    group = google_compute_region_network_endpoint_group.control.id
  }
}

resource "google_compute_managed_ssl_certificate" "cloud_app" {
  name    = "${local.name_prefix}-cloud-app-cert"
  project = var.project_id

  managed {
    domains = [
      local.hostnames.frontend,
      local.hostnames.serve,
      local.hostnames.control,
    ]
  }
}

resource "google_compute_url_map" "https" {
  name            = "${local.name_prefix}-https-map"
  project         = var.project_id
  default_service = google_compute_backend_service.frontend.id

  host_rule {
    hosts        = [local.hostnames.frontend]
    path_matcher = "frontend"
  }

  host_rule {
    hosts        = [local.hostnames.serve]
    path_matcher = "serve"
  }

  host_rule {
    hosts        = [local.hostnames.control]
    path_matcher = "control"
  }

  path_matcher {
    name            = "frontend"
    default_service = google_compute_backend_service.frontend.id
  }

  path_matcher {
    name            = "serve"
    default_service = google_compute_backend_service.serve.id
  }

  path_matcher {
    name            = "control"
    default_service = google_compute_backend_service.control.id
  }
}

resource "google_compute_target_https_proxy" "https" {
  name             = "${local.name_prefix}-https-proxy"
  project          = var.project_id
  url_map          = google_compute_url_map.https.id
  ssl_certificates = [google_compute_managed_ssl_certificate.cloud_app.id]
}

resource "google_compute_global_forwarding_rule" "https" {
  name                  = "${local.name_prefix}-https-forwarding"
  project               = var.project_id
  ip_address            = google_compute_global_address.cloud_app.id
  load_balancing_scheme = "EXTERNAL_MANAGED"
  port_range            = "443"
  target                = google_compute_target_https_proxy.https.id
}

resource "google_compute_url_map" "http_redirect" {
  name    = "${local.name_prefix}-http-redirect"
  project = var.project_id

  default_url_redirect {
    https_redirect         = true
    redirect_response_code = "MOVED_PERMANENTLY_DEFAULT"
    strip_query            = false
  }
}

resource "google_compute_target_http_proxy" "http_redirect" {
  name    = "${local.name_prefix}-http-redirect-proxy"
  project = var.project_id
  url_map = google_compute_url_map.http_redirect.id
}

resource "google_compute_global_forwarding_rule" "http_redirect" {
  name                  = "${local.name_prefix}-http-forwarding"
  project               = var.project_id
  ip_address            = google_compute_global_address.cloud_app.id
  load_balancing_scheme = "EXTERNAL_MANAGED"
  port_range            = "80"
  target                = google_compute_target_http_proxy.http_redirect.id
}

resource "cloudflare_dns_record" "frontend" {
  zone_id = data.cloudflare_zone.ceiora.zone_id
  name    = "app"
  type    = "A"
  ttl     = 1
  content = google_compute_global_address.cloud_app.address
  proxied = var.cloudflare_proxied
  comment = "Ceiora frontend HTTPS load balancer"
}

resource "cloudflare_dns_record" "serve" {
  zone_id = data.cloudflare_zone.ceiora.zone_id
  name    = "api"
  type    = "A"
  ttl     = 1
  content = google_compute_global_address.cloud_app.address
  proxied = var.cloudflare_proxied
  comment = "Ceiora serve HTTPS load balancer"
}

resource "cloudflare_dns_record" "control" {
  zone_id = data.cloudflare_zone.ceiora.zone_id
  name    = "control"
  type    = "A"
  ttl     = 1
  content = google_compute_global_address.cloud_app.address
  proxied = var.cloudflare_proxied
  comment = "Ceiora control HTTPS load balancer"
}
