"""Cloud Run Jobs dispatch adapter for control-plane refreshes."""

from __future__ import annotations

import json
import urllib.request
from typing import Any

import google.auth
from google.auth.transport.requests import Request

from backend import config

_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def dispatch_enabled() -> bool:
    return config.serve_refresh_cloud_job_configured()


def _execution_url() -> str:
    if not dispatch_enabled():
        raise RuntimeError("Cloud Run Jobs dispatch is not configured for serve-refresh.")
    return (
        "https://run.googleapis.com/v2/"
        f"projects/{config.CLOUD_RUN_PROJECT_ID}/locations/{config.CLOUD_RUN_REGION}/"
        f"jobs/{config.SERVE_REFRESH_CLOUD_RUN_JOB_NAME}:run"
    )


def _access_token() -> str:
    credentials, _ = google.auth.default(scopes=[_SCOPE])
    credentials.refresh(Request())
    token = str(getattr(credentials, "token", "") or "").strip()
    if not token:
        raise RuntimeError("Google application credentials did not return an access token.")
    return token


def _env_overrides(
    *,
    pipeline_run_id: str,
    profile: str,
    as_of_date: str | None,
    from_stage: str | None,
    to_stage: str | None,
    force_core: bool,
    refresh_scope: str | None,
) -> list[dict[str, str]]:
    env = [
        {"name": "REFRESH_PIPELINE_RUN_ID", "value": pipeline_run_id},
        {"name": "REFRESH_PROFILE", "value": profile},
    ]
    optional = {
        "REFRESH_AS_OF_DATE": as_of_date,
        "REFRESH_FROM_STAGE": from_stage,
        "REFRESH_TO_STAGE": to_stage,
        "REFRESH_SCOPE": refresh_scope,
    }
    for name, value in optional.items():
        clean = str(value or "").strip()
        if clean:
            env.append({"name": name, "value": clean})
    if force_core:
        env.append({"name": "REFRESH_FORCE_CORE", "value": "true"})
    return env


def dispatch_serve_refresh(
    *,
    pipeline_run_id: str,
    profile: str,
    as_of_date: str | None,
    from_stage: str | None,
    to_stage: str | None,
    force_core: bool,
    refresh_scope: str | None,
) -> dict[str, Any]:
    payload = {
        "overrides": {
            "containerOverrides": [
                {
                    "env": _env_overrides(
                        pipeline_run_id=pipeline_run_id,
                        profile=profile,
                        as_of_date=as_of_date,
                        from_stage=from_stage,
                        to_stage=to_stage,
                        force_core=force_core,
                        refresh_scope=refresh_scope,
                    ),
                }
            ]
        }
    }
    req = urllib.request.Request(
        _execution_url(),
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {_access_token()}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        body = json.loads(response.read().decode("utf-8") or "{}")
    return {
        "execution_name": body.get("name"),
        "metadata": body,
    }
