output "artifact_registry_repository" {
  description = "Artifact Registry repository for cloud app images."
  value       = module.artifact_registry.repository_url
}

output "service_account_emails" {
  description = "Service account emails for the cloud stack."
  value       = module.service_accounts.email_by_key
}

output "secret_ids" {
  description = "Secret Manager secret ids created for the cloud stack."
  value       = module.secret_manager.secret_ids
}

output "hostnames" {
  description = "Custom-domain hostnames for the cloud stack when endpoint_mode=custom_domains."
  value       = local.hostnames
}

output "service_names" {
  description = "Cloud Run service names for the cloud app surfaces."
  value = {
    frontend = google_cloud_run_v2_service.frontend.name
    serve    = google_cloud_run_v2_service.serve.name
    control  = google_cloud_run_v2_service.control.name
  }
}

output "service_urls" {
  description = "Cloud Run run.app URLs for the live services. These remain useful in every topology mode."
  value = {
    frontend = google_cloud_run_v2_service.frontend.uri
    serve    = google_cloud_run_v2_service.serve.uri
    control  = google_cloud_run_v2_service.control.uri
  }
}

output "endpoint_mode" {
  description = "Current public topology contract for the cloud app."
  value       = local.endpoint_mode
}

output "public_origins" {
  description = "Canonical public origins for the current topology contract. In run_app mode these must be explicit inputs."
  value       = local.public_origins
}

output "service_image_refs" {
  description = "Image refs pinned into the Cloud Run service definitions. endpoint_mode=run_app requires these to be explicit inputs."
  value = {
    frontend = local.frontend_image_ref
    serve    = local.serve_image_ref
    control  = local.control_image_ref
  }
}

output "frontend_build_contract" {
  description = "Frontend build/runtime proxy inputs. BACKEND_API_ORIGIN must match the image build input; changing the service env alone will not retarget the compiled rewrite."
  value = {
    endpoint_mode          = local.endpoint_mode
    public_frontend_origin = local.frontend_public_origin
    build_api_origin       = local.frontend_backend_api_origin
    runtime_control_origin = local.frontend_backend_control_origin
  }
}

output "serve_refresh_job_name" {
  description = "Cloud Run Job name for serve-refresh execution."
  value       = google_cloud_run_v2_job.serve_refresh.name
}

output "load_balancer_ip" {
  description = "Global load balancer IPv4 address for app/api/control when the custom-domain edge is in use."
  value       = google_compute_global_address.cloud_app.address
}

output "load_balancer_dns_records" {
  description = "Cloudflare-managed DNS records for the custom-domain edge."
  value = {
    frontend = {
      hostname  = local.hostnames.frontend
      record_id = cloudflare_dns_record.frontend.id
    }
    serve = {
      hostname  = local.hostnames.serve
      record_id = cloudflare_dns_record.serve.id
    }
    control = {
      hostname  = local.hostnames.control
      record_id = cloudflare_dns_record.control.id
    }
  }
}

output "load_balancer_host_routing" {
  description = "Host-based routing contract for the shared HTTPS load balancer when endpoint_mode=custom_domains."
  value = {
    frontend = local.hostnames.frontend
    serve    = local.hostnames.serve
    control  = local.hostnames.control
  }
}

output "control_service_job_env" {
  description = "Environment values the control service will need for Cloud Run Job dispatch."
  value = {
    CLOUD_RUN_JOBS_ENABLED           = "true"
    CLOUD_RUN_PROJECT_ID             = var.project_id
    CLOUD_RUN_REGION                 = var.region
    SERVE_REFRESH_CLOUD_RUN_JOB_NAME = google_cloud_run_v2_job.serve_refresh.name
  }
}
