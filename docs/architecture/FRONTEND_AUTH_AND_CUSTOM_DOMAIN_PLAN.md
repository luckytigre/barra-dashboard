# Frontend Auth And Custom Domain Plan

Date: 2026-04-14
Owner: Codex
Status: Deferred future proposal; not part of the active cutover path

## Purpose

Define the lowest-complexity path from the current public `run.app` topology to the intended hobby-project shape:

- one public frontend
- the app moved behind auth
- heavy backend surfaces no longer directly internet-reachable
- `app.ceiora.com` restored only where users actually need it

This document is intentionally phased. It replaces the earlier big-bang idea of changing URL topology, auth, backend reachability, and operator workflows all at once.

## Goals

- Keep the project simple and low-cost.
- Preserve scale-to-zero behavior.
- Restore a single clean public custom domain for the frontend.
- Stop random internet traffic from waking the control plane.
- Move the current app home off `/` so a new public landing page can be built later.

## Non-Goals

- Reintroducing three public hostnames (`app`, `api`, `control`) as the default target shape.
- Rebuilding the old edge/load-balancer architecture as the first move.
- Landing a full multi-role auth system in one pass.

## Current State

- Live production topology is `endpoint_mode=custom_domains` with `edge_enabled=true`.
- Frontend, serve, and control are all still public Cloud Run services.
- The frontend currently relies on browser-local operator/editor tokens and forwards them to backend routes.
- The shared frontend shell can still trigger control-plane reads on app pages even when no operator token is present.
- `/` is the current app-facing landing/home surface.

This proposal is intentionally deferred until the full-cloud compute cutover is signed off.
Do not treat it as an active cutover dependency.

## Accepted Direction

The intended end state is:

- public frontend only
- `app.ceiora.com` mapped only to that frontend
- `/` reserved for a future public landing page
- current app home moved to `/home`
- `/home`, `/cuse/*`, `/cpar/*`, `/positions`, `/data`, and `/settings` protected by auth
- `serve` and `control` callable only by the frontend

## Key Decisions

### 1. Keep exactly one public web surface

The frontend stays public.
`serve` and `control` should become private only after the frontend has a real server-to-server auth path.

Reason:
- this preserves a simple user mental model
- this prevents direct random hits on the heavy backends
- this keeps the custom-domain problem smaller

### 2. Stage `/home` before replacing `/`

Create `/home` now as the future authenticated app home.
Do not replace `/` until auth and login-return behavior exist.

Reason:
- route migration and auth migration should not land in the same cut
- bookmarks and shared links need a compatibility window

### 3. Do not start with full viewer/editor/operator RBAC

The first auth cut should aim for one real authenticated app session.
Role splits can layer on later when they are tied to concrete workflows.

Reason:
- this is a hobby project
- the repo currently has no real session subsystem
- multi-role design adds complexity before the base auth boundary exists

### 4. Restore `app.ceiora.com` only for the frontend

Do not restore separate public `api.ceiora.com` and `control.ceiora.com` unless a later requirement proves they are needed.

Reason:
- one public hostname is enough for the intended app
- the old three-host edge shape is more infrastructure than the hobby profile needs

## Phased Rollout

### Phase 0: Immediate wakeup reduction

Status:
- in progress in this change

Changes:
- stop anonymous shared-shell control-plane fetches
- add `/home` as the future authenticated app home while keeping `/` unchanged

Exit criteria:
- anonymous visits to cUSE/positions/data routes no longer hit `/api/operator/status` from the shared shell
- `/home` exists and matches the current app-facing landing/home content

### Phase 1: Auth boundary in the frontend

Changes:
- add a real auth/session boundary in Next
- protect `/home`, `/cuse/*`, `/cpar/*`, `/positions`, `/data`, `/settings`
- protect frontend `/api/*` proxy routes before they forward upstream
- add login, logout, and `returnTo` flow

Constraints:
- this is a new subsystem; it is not a route rename
- mutating flows need CSRF/origin protections if cookie sessions are used
- the old browser-local token editor in `/settings` must be gated or retired as part of this phase

Exit criteria:
- unauthenticated users are redirected away from protected pages
- protected API proxy routes reject unauthenticated calls before contacting backend services
- login returns the user to the originally requested URL

### Phase 2: Backend trust-model cutover

Changes:
- move backend trust from browser-held secrets to frontend-server identity
- make `serve` and `control` private at Cloud Run/IAM
- remove direct browser reliance on raw operator/editor backend secrets
- narrow or remove the global `/api/*` rewrite once App Router proxy routes are the supported path

Constraints:
- do not flip backend privacy until frontend-to-backend service auth exists
- smoke tests and rollback procedures need explicit updates because direct public `run.app` calls go away for private services

Exit criteria:
- browsers cannot directly call `serve` or `control`
- frontend can still proxy successfully to both services
- operator smoke and rollback docs reflect the new private-backend path

### Phase 3: Public URL transition

Changes:
- restore `app.ceiora.com` for the frontend only
- keep the backend services non-public
- cut the branded URL over only after auth and private backend boundaries are stable

Constraints:
- the current repo custom-domain path is a three-host load balancer module
- frontend-only custom domain restoration is therefore a separate infra decision, not a toggle

Exit criteria:
- users reach the frontend via `app.ceiora.com`
- backend services remain non-public
- the project does not regress to the old three-public-host shape unless explicitly chosen

### Phase 4: New public `/`

Changes:
- replace the current `/` content with the future public landing page
- keep `/home` as the authenticated app entry
- preserve login entry from the public landing page

Exit criteria:
- `/` is public and light
- `/home` is the stable authenticated app home

## Tradeoffs

### Why not do all of this at once?

Because the current repo lacks:

- a session subsystem
- route middleware
- service-to-service backend auth from the frontend
- a frontend-only custom-domain module

Landing all of that in one cut would make failures ambiguous and rollback harder.

### Why not restore the old load balancer immediately?

Because the current edge module is intentionally three-host and brings back more surface area than the target shape needs.

Load balancer remains the fallback if a frontend-only custom-domain path proves insufficient.

### Why not keep browser-held operator/editor tokens as the main auth model?

Because that model:

- is not a real app auth wall
- cannot protect page routes before client render
- keeps mutating/control privileges too close to the browser

## Executed In This Change

- `/home` is added as the future authenticated app home
- shared-shell operator/control fetches are suppressed when no operator token is present

These changes intentionally reduce cost/noise now without pretending the full auth and domain migration is already complete.
