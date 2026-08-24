# Ceiora Risk

Ceiora Risk is a factor-risk and portfolio analytics application. The public
Next.js frontend proxies reads and authorized control actions to separate
FastAPI serving and control surfaces. cUSE is the default model family; cPAR is
parallel and explicitly namespaced.

Start with [the documentation index](docs/README.md), then use the
[architecture and operating model](docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md)
and [operations playbook](docs/operations/OPERATIONS_PLAYBOOK.md) for deeper
context.

## Development Baseline

- Python 3.12 is pinned in `.python-version` and is the recommended local
  version. The backend package supports Python 3.11 or newer; the optional LSEG
  integration requires Python 3.12 or newer.
- Node 20.x is required by the frontend build scripts.

From a clean clone:

```bash
make setup
source .venv_local/bin/activate
make doctor
```

`make setup` installs the backend development dependencies and frontend npm
dependencies. Set `INSTALL_LSEG_RUNTIME=0` when the proprietary LSEG runtime is
not available.

Run the local applications in separate shells:

```bash
source .venv_local/bin/activate
make backend
```

```bash
make frontend
```

Useful safe checks:

```bash
pytest backend/tests -q
cd frontend && npm run typecheck
cd frontend && npm run build
```

## Local Configuration

Copy `.env.example` to an ignored local `.env` only when configuration is
needed. Keep credentials, production data, Terraform state, runtime databases,
and generated artifacts out of Git. Cloud deployment, refresh, scheduler, and
credentialed operator commands are not onboarding steps; follow the
[cloud-native runbook](docs/operations/CLOUD_NATIVE_RUNBOOK.md) and use approved
access before running them.

Backend layout details are in [backend/README.md](backend/README.md).
