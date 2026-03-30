module "edge" {
  source = "../../modules/cloud_edge"

  enabled              = local.edge_enabled
  project_id           = var.project_id
  region               = var.region
  name_prefix          = local.name_prefix
  cloudflare_zone_name = var.cloudflare_zone_name
  cloudflare_proxied   = var.cloudflare_proxied
  hostnames            = local.hostnames
  service_names = {
    frontend = google_cloud_run_v2_service.frontend.name
    serve    = google_cloud_run_v2_service.serve.name
    control  = google_cloud_run_v2_service.control.name
  }
}
