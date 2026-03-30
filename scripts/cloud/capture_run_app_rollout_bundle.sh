#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

TERRAFORM_BIN="${TERRAFORM_BIN:-terraform}"
TF_PROD_DIR="${TF_PROD_DIR:-${ROOT_DIR}/infra/terraform/envs/prod}"
ROLLOUT_SOURCE_OUTPUT_JSON="${ROLLOUT_SOURCE_OUTPUT_JSON:-}"
ROLLOUT_BUNDLE_DIR="${ROLLOUT_BUNDLE_DIR:-}"
ROLLOUT_ALLOW_NON_CUSTOM_SOURCE="${ROLLOUT_ALLOW_NON_CUSTOM_SOURCE:-0}"

usage() {
  cat <<'EOF'
Usage:
  ROLLOUT_BUNDLE_DIR=backend/runtime/cloud_rollouts/<name> ./scripts/cloud/capture_run_app_rollout_bundle.sh

Environment:
  TF_PROD_DIR                     Terraform prod root. Default: infra/terraform/envs/prod
  TERRAFORM_BIN                   Terraform binary. Default: terraform
  ROLLOUT_SOURCE_OUTPUT_JSON      Optional saved terraform output -json file to capture from instead of the live prod root
  ROLLOUT_BUNDLE_DIR              Destination bundle directory. Default: backend/runtime/cloud_rollouts/run_app_<timestamp>
  ROLLOUT_ALLOW_NON_CUSTOM_SOURCE Set to 1 to capture from a non-custom_domains source topology for rehearsal/debug only
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

load_terraform_outputs() {
  local destination="$1"
  if [[ -n "${ROLLOUT_SOURCE_OUTPUT_JSON}" ]]; then
    if [[ ! -f "${ROLLOUT_SOURCE_OUTPUT_JSON}" ]]; then
      printf 'ROLLOUT_SOURCE_OUTPUT_JSON does not exist: %s\n' "${ROLLOUT_SOURCE_OUTPUT_JSON}" >&2
      exit 1
    fi
    cp "${ROLLOUT_SOURCE_OUTPUT_JSON}" "${destination}"
    return 0
  fi

  if [[ ! -d "${TF_PROD_DIR}" ]]; then
    printf 'TF_PROD_DIR does not exist: %s\n' "${TF_PROD_DIR}" >&2
    exit 1
  fi

  if ! "${TERRAFORM_BIN}" -chdir="${TF_PROD_DIR}" output -json >"${destination}" 2>"${destination}.err"; then
    cat "${destination}.err" >&2
    printf '\nUnable to read terraform outputs. Run terraform init in %s or set ROLLOUT_SOURCE_OUTPUT_JSON to a saved terraform output -json file.\n' "${TF_PROD_DIR}" >&2
    exit 1
  fi
}

timestamp_utc() {
  date -u +"%Y%m%dT%H%M%SZ"
}

if [[ -z "${ROLLOUT_BUNDLE_DIR}" ]]; then
  ROLLOUT_BUNDLE_DIR="${ROOT_DIR}/backend/runtime/cloud_rollouts/run_app_$(timestamp_utc)"
fi

mkdir -p "${ROLLOUT_BUNDLE_DIR}"

tmp_output="$(mktemp)"
trap 'rm -f "${tmp_output}" "${tmp_output}.err"' EXIT
load_terraform_outputs "${tmp_output}"
cp "${tmp_output}" "${ROLLOUT_BUNDLE_DIR}/terraform-output.json"

python3 - "${ROLLOUT_BUNDLE_DIR}" "${ROLLOUT_ALLOW_NON_CUSTOM_SOURCE}" <<'PY'
import json
import os
import pathlib
import sys
from datetime import datetime, timezone

bundle_dir = pathlib.Path(sys.argv[1]).resolve()
allow_non_custom = sys.argv[2] == "1"
outputs_path = bundle_dir / "terraform-output.json"
outputs = json.loads(outputs_path.read_text(encoding="utf-8"))

endpoint_mode = outputs["endpoint_mode"]["value"]
edge_enabled = bool(outputs["edge_enabled"]["value"])

if not allow_non_custom and not (endpoint_mode == "custom_domains" and edge_enabled):
    raise SystemExit(
        "capture_run_app_rollout_bundle.sh expects a live custom_domains + edge_enabled=true source topology. "
        "Set ROLLOUT_ALLOW_NON_CUSTOM_SOURCE=1 only for rehearsal/debug capture."
    )

service_urls = outputs["service_urls"]["value"]
service_image_refs = outputs["service_image_refs"]["value"]
service_names = outputs["service_names"]["value"]
public_origins = outputs["public_origins"]["value"]
hostnames = outputs["hostnames"]["value"]

captured_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

manifest = {
    "captured_at": captured_at,
    "source_kind": "saved-output-json" if "ROLLOUT_SOURCE_OUTPUT_JSON" in os.environ else "live-terraform-output",
    "source_topology": {
        "endpoint_mode": endpoint_mode,
        "edge_enabled": edge_enabled,
        "public_origins": public_origins,
        "hostnames": hostnames,
    },
    "service_names": service_names,
    "service_urls": service_urls,
    "service_image_refs": service_image_refs,
    "bundle_files": {
        "terraform_output_json": "terraform-output.json",
        "rollback_tfvars": "rollback_custom_domains.tfvars",
        "run_app_soak_base_tfvars": "run_app_soak.base.tfvars",
        "run_app_no_edge_base_tfvars": "run_app_no_edge.base.tfvars",
        "run_app_frontend_image_ref": "run_app_frontend_image_ref.txt",
    },
}

(bundle_dir / "manifest.json").write_text(
    json.dumps(manifest, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)

rollback_tfvars = f"""# Captured from {outputs_path.name} at {captured_at}
# Roll back to the pre-cutover custom-domain topology with pinned image refs.
endpoint_mode = "custom_domains"
edge_enabled = true
frontend_image_ref = "{service_image_refs['frontend']}"
serve_image_ref = "{service_image_refs['serve']}"
control_image_ref = "{service_image_refs['control']}"
"""

run_app_soak_base = f"""# Captured from {outputs_path.name} at {captured_at}
# Base contract for the run_app soak cutover.
# Provide the run.app-built frontend image at plan/apply time through RUN_APP_FRONTEND_IMAGE_REF
# or by creating {manifest['bundle_files']['run_app_frontend_image_ref']} with the built image ref.
endpoint_mode = "run_app"
edge_enabled = true
frontend_public_origin = "{service_urls['frontend']}"
frontend_backend_api_origin = "{service_urls['serve']}"
frontend_backend_control_origin = "{service_urls['control']}"
serve_image_ref = "{service_image_refs['serve']}"
control_image_ref = "{service_image_refs['control']}"
"""

run_app_no_edge_base = f"""# Captured from {outputs_path.name} at {captured_at}
# Base contract for the final run_app no-edge steady state.
# Provide the run.app-built frontend image at plan/apply time through RUN_APP_FRONTEND_IMAGE_REF
# or by creating {manifest['bundle_files']['run_app_frontend_image_ref']} with the built image ref.
endpoint_mode = "run_app"
edge_enabled = false
frontend_public_origin = "{service_urls['frontend']}"
frontend_backend_api_origin = "{service_urls['serve']}"
frontend_backend_control_origin = "{service_urls['control']}"
serve_image_ref = "{service_image_refs['serve']}"
control_image_ref = "{service_image_refs['control']}"
"""

readme = f"""Run.app rollout bundle
=====================

Bundle directory: {bundle_dir}
Captured at: {captured_at}
Source topology: endpoint_mode={endpoint_mode}, edge_enabled={str(edge_enabled).lower()}

Files:
- terraform-output.json
- manifest.json
- rollback_custom_domains.tfvars
- run_app_soak.base.tfvars
- run_app_no_edge.base.tfvars

Recommended next steps:
1. Build the run.app frontend image:
   CUTOVER_ACTION=build-frontend ROLLOUT_BUNDLE_DIR="{bundle_dir}" make cloud-run-app-cutover
2. Plan the soak cutover:
   CUTOVER_ACTION=plan CUTOVER_PHASE=soak ROLLOUT_BUNDLE_DIR="{bundle_dir}" make cloud-run-app-cutover
3. Apply the soak cutover:
   CUTOVER_ACTION=apply CUTOVER_PHASE=soak ROLLOUT_BUNDLE_DIR="{bundle_dir}" ALLOW_TERRAFORM_APPLY=1 make cloud-run-app-cutover
4. Verify the soak state:
   CUTOVER_ACTION=verify ROLLOUT_BUNDLE_DIR="{bundle_dir}" OPERATOR_API_TOKEN=... make cloud-run-app-cutover
5. After soak, plan/apply no-edge with CUTOVER_PHASE=no-edge.

Important:
- run_app plan/apply will fail closed until a run.app-built frontend image ref is supplied.
- rollback_custom_domains.tfvars is the pinned rollback contract captured from the pre-cutover state.
"""

(bundle_dir / "rollback_custom_domains.tfvars").write_text(rollback_tfvars, encoding="utf-8")
(bundle_dir / "run_app_soak.base.tfvars").write_text(run_app_soak_base, encoding="utf-8")
(bundle_dir / "run_app_no_edge.base.tfvars").write_text(run_app_no_edge_base, encoding="utf-8")
(bundle_dir / "README.txt").write_text(readme, encoding="utf-8")
PY

printf 'Captured run_app rollout bundle at %s\n' "${ROLLOUT_BUNDLE_DIR}"
printf 'Bundle files:\n'
printf '  - %s\n' \
  "${ROLLOUT_BUNDLE_DIR}/terraform-output.json" \
  "${ROLLOUT_BUNDLE_DIR}/manifest.json" \
  "${ROLLOUT_BUNDLE_DIR}/rollback_custom_domains.tfvars" \
  "${ROLLOUT_BUNDLE_DIR}/run_app_soak.base.tfvars" \
  "${ROLLOUT_BUNDLE_DIR}/run_app_no_edge.base.tfvars"
