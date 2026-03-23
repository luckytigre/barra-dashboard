resource "google_logging_project_bucket_config" "default_logs" {
  project        = var.project_id
  location       = "global"
  bucket_id      = "_Default"
  retention_days = var.default_log_retention_days
}

resource "google_monitoring_uptime_check_config" "frontend" {
  project      = var.project_id
  display_name = "${local.name_prefix}-frontend-uptime"
  timeout      = "10s"
  period       = "300s"

  monitored_resource {
    type = "uptime_url"
    labels = {
      project_id = var.project_id
      host       = local.hostnames.frontend
    }
  }

  http_check {
    path         = "/"
    port         = 443
    use_ssl      = true
    validate_ssl = true
  }
}

resource "google_monitoring_uptime_check_config" "serve" {
  project      = var.project_id
  display_name = "${local.name_prefix}-serve-uptime"
  timeout      = "10s"
  period       = "300s"

  monitored_resource {
    type = "uptime_url"
    labels = {
      project_id = var.project_id
      host       = local.hostnames.serve
    }
  }

  http_check {
    path         = "/api/cpar/meta"
    port         = 443
    use_ssl      = true
    validate_ssl = true
  }
}
