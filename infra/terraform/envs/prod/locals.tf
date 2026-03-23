locals {
  name_prefix        = "ceiora-${var.environment}"
  registry_base      = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_registry_repository_id}"
  frontend_image_ref = var.frontend_image_ref != "" ? var.frontend_image_ref : "${local.registry_base}/frontend:${var.image_tag}"
  serve_image_ref    = var.serve_image_ref != "" ? var.serve_image_ref : "${local.registry_base}/serve:${var.image_tag}"
  control_image_ref  = var.control_image_ref != "" ? var.control_image_ref : "${local.registry_base}/control:${var.image_tag}"

  hostnames = {
    frontend = "app.${var.cloudflare_zone_name}"
    serve    = "api.${var.cloudflare_zone_name}"
    control  = "control.${var.cloudflare_zone_name}"
  }

  frontend_backend_api_origin = (
    var.frontend_backend_api_origin != ""
    ? var.frontend_backend_api_origin
    : "https://${local.hostnames.serve}"
  )

  frontend_backend_control_origin = (
    var.frontend_backend_control_origin != ""
    ? var.frontend_backend_control_origin
    : "https://${local.hostnames.control}"
  )

  public_cors_allow_origins = join(
    ",",
    [
      "https://${local.hostnames.frontend}",
      "https://${local.hostnames.serve}",
      "https://${local.hostnames.control}",
    ],
  )

  secret_ids = {
    neon_database_url  = "${local.name_prefix}-neon-database-url"
    operator_api_token = "${local.name_prefix}-operator-api-token"
    editor_api_token   = "${local.name_prefix}-editor-api-token"
  }

  service_accounts = {
    frontend = {
      account_id   = "${replace(local.name_prefix, "-", "")}frontend"
      display_name = "ceiora prod frontend"
      description  = "Cloud Run service account for the frontend app."
    }
    serve = {
      account_id   = "${replace(local.name_prefix, "-", "")}serve"
      display_name = "ceiora prod serve"
      description  = "Cloud Run service account for the serve API."
    }
    control = {
      account_id   = "${replace(local.name_prefix, "-", "")}control"
      display_name = "ceiora prod control"
      description  = "Cloud Run service account for the control API."
    }
    jobs = {
      account_id   = "${replace(local.name_prefix, "-", "")}jobs"
      display_name = "ceiora prod jobs"
      description  = "Cloud Run Jobs service account for control-plane execution."
    }
  }

  secret_accessors = {
    frontend_operator = {
      secret_key          = "operator_api_token"
      service_account_key = "frontend"
    }
    frontend_editor = {
      secret_key          = "editor_api_token"
      service_account_key = "frontend"
    }
    serve_neon = {
      secret_key          = "neon_database_url"
      service_account_key = "serve"
    }
    serve_operator = {
      secret_key          = "operator_api_token"
      service_account_key = "serve"
    }
    serve_editor = {
      secret_key          = "editor_api_token"
      service_account_key = "serve"
    }
    control_neon = {
      secret_key          = "neon_database_url"
      service_account_key = "control"
    }
    control_operator = {
      secret_key          = "operator_api_token"
      service_account_key = "control"
    }
    jobs_neon = {
      secret_key          = "neon_database_url"
      service_account_key = "jobs"
    }
    jobs_operator = {
      secret_key          = "operator_api_token"
      service_account_key = "jobs"
    }
  }
}
