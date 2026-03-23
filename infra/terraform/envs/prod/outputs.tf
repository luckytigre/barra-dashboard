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
  description = "Frozen public hostnames for the cloud stack."
  value       = local.hostnames
}

output "serve_refresh_job_name" {
  description = "Cloud Run Job name for serve-refresh execution."
  value       = google_cloud_run_v2_job.serve_refresh.name
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
