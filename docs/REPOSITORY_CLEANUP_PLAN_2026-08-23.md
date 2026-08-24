# Repository Cleanup Plan

Date: 2026-08-23
Status: Adversarially reviewed; implementation in progress
Owner: Repository maintainers

## Objective

Make the repository easier to clone, understand, and maintain without changing
application behavior, weakening operational history, or rewriting Git history.

This is a temporary execution plan. After the work is complete, replace it with
a closeout note of at most 15 lines under `docs/archive/execution-logs/` and
remove it from the active docs index. The closeout records only scope,
decisions, commit IDs, manifest location, and deferred work.

## Baseline

- Git tracks 893 files, including 197 documentation files totaling 17.48 MB.
- `docs/operations/cutover_evidence/` contains 85 files totaling about 16 MB.
- Exact duplicate evidence accounts for at least 6.24 MB in the current tree.
- `docs/architecture/UNIVERSE_REGISTRY_AND_MODEL_GATING_PLAN.md` is 4,260 lines
  and combines current design, dated reviews, remediation plans, and execution
  history.
- `docs/README.md` names one missing temporary workstream and one cutover plan
  whose status has not been refreshed since April 2026.
- The missing cPAR hedge-plan reference also appears in the canonical cPAR
  architecture document.
- Twenty-four tracked Markdown files contain absolute links to an obsolete local
  Dropbox path; both active and archived documents are affected.
- The repository has no root `README.md` for a new contributor.
- The local checkout contains an ignored broken Python environment and generated
  caches. They are not GitHub content, but they consume about 228 MB locally.
- `backend/pyproject.toml` supports Python 3.11 or newer; Python 3.12 is the
  recommended development baseline and is required by the optional LSEG extra.

## Guardrails

- Do not change runtime behavior, API contracts, model logic, schemas, or
  deployment configuration in this cleanup.
- Do not rewrite Git history or force-push.
- Retain operational evidence in its current paths during this cleanup. Do not
  delete or relocate it merely to reduce file count.
- Do not move raw evidence outside Git until its retention needs, destination,
  access controls, recovery procedure, and accountable records owner are
  explicitly approved.
- Never print or copy secret values while reviewing operational captures.
- Preserve the working Python 3.12 environment, `frontend/node_modules/`, local
  environment files, Terraform working data, and runtime data.
- Use one reversible commit per independent cleanup slice.
- Stop a slice if current ownership or retention cannot be established from the
  repository and available operational evidence.
- Classify unknown document status as active; elapsed time is not evidence that
  an operational workstream has closed.
- A positive secret finding is a security incident and a hard stop, not a cleanup
  opportunity. Rotate or revoke through the credential owner, assess reachable
  Git history and deployments, and plan remediation separately using redacted
  identifiers only.
- Before implementation, commit or deliberately set aside the reviewed plan,
  fetch the remote, record local and remote commit IDs, and work on a dedicated
  cleanup branch. Recheck the worktree before every slice and do not push directly
  to `main` while another contributor may be editing it.

## Slice 1: Local Ignored Residue

Delete only confirmed disposable local artifacts from an enumerated and reviewed
target list:

- `backend/.venv.broken-20260823/`
- project Python `__pycache__/` directories outside preserved environments
- `.DS_Store` files
- the empty `__unused_test_data__.db`

Explicitly exclude `backend/.venv/**`, `frontend/node_modules/**`, Terraform
working directories, runtime data, local environment files, and any active
interpreter environment.

Before deletion, resolve and validate every real path, confirm it stays within
the repository, confirm `git check-ignore` succeeds, confirm it is not tracked,
and check running processes/open files. Use Trash where practical. This slice
produces no Git commit.

Acceptance:

- the working Python 3.12 environment still imports the project
- ignored development dependencies and local configuration remain present
- `git status --short` contains no unexpected changes beyond the reviewed plan

## Slice 2: Contributor Entry Point And Docs Routing

Add a concise root `README.md` containing:

- project purpose and high-level topology
- recommended Python 3.12 and required Node 20 development versions, while
  accurately noting the backend's Python 3.11 minimum and LSEG's 3.12 requirement
- minimal setup, test, build, and documentation entry points
- links to canonical architecture and operations documents
- a boundary between safe local setup and credentialed production/runtime steps

Update `docs/README.md` to:

- link this temporary cleanup plan while it is active
- inventory every reference to the missing cPAR hedge workstream and, with the
  model owner's confirmation, atomically restore it, replace all references with
  the actual canonical record, or mark its status unknown
- verify whether the cloud cutover plan is still active
- keep archived plans out of the canonical reading path
- replace obsolete machine-specific absolute links in active docs with portable
  repository-relative links in a bounded mechanical change; archived-doc link
  repair remains separately reviewable

Acceptance:

- every local path named in the two README files exists
- setup commands match current project metadata and scripts
- setup commands run in a clean disposable environment without production
  credentials or data
- canonical ownership remains unambiguous
- no tracked active-doc reference to the missing hedge workstream remains

Rollback: revert the README commit.

## Slice 3: Active Documentation Consolidation

Treat content classification and content reduction as separate operations.

Before moving anything, create a reviewed classification table with the path,
proposed current/historical/unknown status, accountable owner, evidence source or
dated live check, decision date, and every inbound link that must change. Unknown
status remains active and blocks archival.

1. Verify current system statements against code, tests, and active runbooks.
2. Preserve the universe registry plan unchanged first: move the exact file to a
   dated path under `docs/archive/implementation-trackers/`, leave a temporary
   pointer at the original canonical path, and confirm rename detection with
   `git diff --summary --find-renames`.
3. In a separate commit, replace the pointer at the original canonical path with
   a concise current-state architecture document. Use a reviewed handoff table
   mapping each current claim to code, tests, a runbook, and relevant historical
   provenance; require model-owner approval.
4. Archive the cloud cutover plan only after a named operational owner confirms
   every gate with dated evidence and identifies replacement current guidance.
   If approved, keep the unchanged plan under
   `docs/archive/implementation-trackers/`, put only the terse completion record
   under `docs/archive/execution-logs/`, and link the two. Do not create a new
   unindexed archive bucket.
5. Generate a complete inbound-reference report across all tracked text files and
   update every affected link atomically. Unresolved references block the move.

The concise universe document should contain only current authority, selectors,
gating semantics, failure behavior, and maintainer extension rules. Dated review
loops, checklists, rollout evidence, and completed remediation narratives belong
in the archived copy.

Acceptance:

- no current behavior claim is inferred solely from a historical plan
- the unchanged archived source and its exact originating commit are recorded
- `git diff --summary --find-renames` shows the expected archival move
- active docs contain no stale status banners or broken local references
- archived content is clearly marked historical

Rollback: revert the specific archival or current-document commit; no content is
destroyed.

## Slice 4: Cutover Evidence Manifest And Retention Assessment

This cleanup does not delete, move, compress, or rewrite cutover evidence. The
16 MB working-tree cost is disproportionate to the unresolved audit, operational,
and security risks. This slice improves discoverability and establishes the
facts needed for a separately approved records decision.

1. Inventory every evidence file by path, byte size, SHA-256, capture time, and
   referenced endpoint or operation when that can be established safely.
2. Generate a complete inbound-reference graph across all tracked text files.
3. Use a non-logging secret scanner under a restricted reviewer. If it reports a
   candidate, stop and report only the path, detector class, and digest to the
   designated maintainer; do not mutate evidence in this program.
4. Create a compact manifest that records each file and every byte-identical
   group, including canonical display path, SHA-256, all equivalent paths,
   capture timestamps, originating Git blob/commit, reference graph, retention
   classification, and reviewer. All original files remain in place.
5. If repository-size reduction is still desired, open a separate records change
   requiring named owner approval, retention period, immutable archive location,
   access owner, restoration drill, and explicit handling of every inbound or
   external reference.

Acceptance:

- all evidence paths and contents remain unchanged
- the manifest preserves why and where each duplicate was captured
- the inbound-reference graph covers active, archived, and evidence documents
- no tracked reference is made dangling
- a secret scan reports no unaddressed credential exposure

Rollback: revert only the manifest commit. Do not rewrite history.

## Deferred Refactoring

The following are maintenance hotspots, not repository-hygiene deletions, and
must be planned and tested separately:

- `frontend/src/app/globals.css`
- `frontend/src/app/palette-preview/page.tsx`
- `backend/tests/test_operating_model_contract.py`
- family-specific chart wrappers that are byte-identical today

Do not mix their decomposition into this cleanup program.

## Verification And Commit Order

For every tracked slice:

1. record the pre-slice `HEAD` and `origin/main` commit IDs
2. run a baseline-aware checker for tracked Markdown links, anchors, inline local
   paths, archive-index entries, and case-sensitive renamed paths
3. inspect `git diff --check`, `git diff --cached --check`,
   `git diff --cached --name-status`, and `git diff --find-renames`
4. reconcile evidence manifest paths, hashes, counts, and inbound references
5. confirm no generated artifacts became tracked
6. run source-level tests only if source files were unexpectedly touched
7. validate root README commands in a clean disposable environment
8. inspect the staged file list before committing
9. push the branch normally and verify its local and remote commit IDs match

Rollback means reverting the specific slice commit on the branch; never
force-push. Preserve the evidence manifest and recorded baseline during recovery.

Planned commit boundaries:

1. `Add contributor README and repair docs routing`
2. `Archive the universe registry implementation record`
3. `Publish the current universe registry architecture`
4. `Archive completed operational plans` (only with owner approval)
5. `Add cutover evidence retention manifest`

## Review Gate

No cleanup slice may begin until adversarial reviewers have challenged:

- whether the plan could destroy operational or audit value
- whether active guidance could be archived incorrectly
- whether evidence handling could leak secrets or lose provenance
- whether the proposed work is smaller and clearer than the documents it removes
- whether rollback and validation are sufficient

Valid findings must be incorporated into this plan before implementation.

## Adversarial Review Outcome

Two independent reviewers challenged documentation preservation and repository
safety before implementation. Their valid findings changed the plan to:

- exclude preserved environments from cache deletion and require exact target
  validation
- default to retaining all cutover evidence in place
- add a hard-stop incident path for potential secrets
- require exhaustive inbound-reference reporting
- split unchanged archival from current-document derivation
- require named model, operational, and records owners for classification choices
- correct Python-version guidance and expand safe contributor onboarding
- make implementation branch-based with explicit baseline and rollback records

No cleanup implementation occurred before review closed. Execution began only
after both reviewers approved the revised plan.
