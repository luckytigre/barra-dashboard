moved {
  from = google_compute_global_address.cloud_app
  to   = module.edge.google_compute_global_address.cloud_app[0]
}

moved {
  from = google_compute_region_network_endpoint_group.frontend
  to   = module.edge.google_compute_region_network_endpoint_group.frontend[0]
}

moved {
  from = google_compute_region_network_endpoint_group.serve
  to   = module.edge.google_compute_region_network_endpoint_group.serve[0]
}

moved {
  from = google_compute_region_network_endpoint_group.control
  to   = module.edge.google_compute_region_network_endpoint_group.control[0]
}

moved {
  from = google_compute_backend_service.frontend
  to   = module.edge.google_compute_backend_service.frontend[0]
}

moved {
  from = google_compute_backend_service.serve
  to   = module.edge.google_compute_backend_service.serve[0]
}

moved {
  from = google_compute_backend_service.control
  to   = module.edge.google_compute_backend_service.control[0]
}

moved {
  from = google_compute_managed_ssl_certificate.cloud_app
  to   = module.edge.google_compute_managed_ssl_certificate.cloud_app[0]
}

moved {
  from = google_compute_url_map.https
  to   = module.edge.google_compute_url_map.https[0]
}

moved {
  from = google_compute_target_https_proxy.https
  to   = module.edge.google_compute_target_https_proxy.https[0]
}

moved {
  from = google_compute_global_forwarding_rule.https
  to   = module.edge.google_compute_global_forwarding_rule.https[0]
}

moved {
  from = google_compute_url_map.http_redirect
  to   = module.edge.google_compute_url_map.http_redirect[0]
}

moved {
  from = google_compute_target_http_proxy.http_redirect
  to   = module.edge.google_compute_target_http_proxy.http_redirect[0]
}

moved {
  from = google_compute_global_forwarding_rule.http_redirect
  to   = module.edge.google_compute_global_forwarding_rule.http_redirect[0]
}

moved {
  from = cloudflare_dns_record.frontend
  to   = module.edge.cloudflare_dns_record.frontend[0]
}

moved {
  from = cloudflare_dns_record.serve
  to   = module.edge.cloudflare_dns_record.serve[0]
}

moved {
  from = cloudflare_dns_record.control
  to   = module.edge.cloudflare_dns_record.control[0]
}
