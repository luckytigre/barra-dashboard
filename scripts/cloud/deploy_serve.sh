#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PROJECT_ID="${PROJECT_ID:-project-4e18de12-63a3-4206-aaa}"
REGION="${REGION:-us-east4}"
REPOSITORY="${REPOSITORY:-ceiora-images}"
IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"
SERVICE_NAME="${SERVICE_NAME:-ceiora-prod-serve}"
CLOUD_RUN_PLATFORM="${CLOUD_RUN_PLATFORM:-linux/amd64}"
ALLOW_DIRECT_SERVE_DEPLOY="${ALLOW_DIRECT_SERVE_DEPLOY:-0}"

if [[ "${ALLOW_DIRECT_SERVE_DEPLOY}" != "1" ]]; then
  cat >&2 <<'EOF'
scripts/cloud/deploy_serve.sh is a direct Cloud Run drift path for serve-only image rollouts.
It must not be used for topology contract changes or endpoint_mode/run_app cutovers.
Re-run with ALLOW_DIRECT_SERVE_DEPLOY=1 only when you intend to bypass Terraform for a serve-only deploy.
EOF
  exit 1
fi

REGISTRY_BASE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}"
SERVE_IMAGE="${SERVE_IMAGE:-${REGISTRY_BASE}/serve:${IMAGE_TAG}}"

PROJECT_ID="${PROJECT_ID}" \
REGION="${REGION}" \
REPOSITORY="${REPOSITORY}" \
IMAGE_TAG="${IMAGE_TAG}" \
CLOUD_RUN_PLATFORM="${CLOUD_RUN_PLATFORM}" \
BUILD_TARGETS="serve" \
BUILD_OUTPUT="push" \
SERVE_IMAGE="${SERVE_IMAGE}" \
./scripts/cloud/build_and_push_images.sh

gcloud run deploy "${SERVICE_NAME}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --image "${SERVE_IMAGE}" \
  --cpu-throttling \
  --quiet

printf 'Deployed serve image %s to Cloud Run service %s (%s)\n' "${SERVE_IMAGE}" "${SERVICE_NAME}" "${REGION}"
