project_id                      = "project-4e18de12-63a3-4206-aaa"
environment                     = "prod"
region                          = "us-east4"
artifact_registry_repository_id = "ceiora-images"
cloudflare_zone_name            = "ceiora.com"
cloudflare_proxied              = false
endpoint_mode                   = "run_app"
edge_enabled                    = false
frontend_public_origin          = "https://app.ceiora.com"
frontend_backend_api_origin     = "https://ceiora-prod-serve-i5znti5joq-uk.a.run.app"
frontend_backend_control_origin = "https://ceiora-prod-control-i5znti5joq-uk.a.run.app"
private_backend_invocation_enabled = true

# Pinned rollout image refs.
# Update these only when intentionally publishing a new service build.
frontend_image_ref = "us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/frontend:40b5c89-authsession-fb4"
serve_image_ref    = "us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/serve:2012c3a"
control_image_ref  = "us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/control:ab737d3-stagemetrics2"
