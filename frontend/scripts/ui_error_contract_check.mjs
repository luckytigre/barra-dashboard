import assert from "node:assert/strict";
import fs from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import ts from "typescript";

const frontendRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const source = fs.readFileSync(path.join(frontendRoot, "src/lib/uiErrors.ts"), "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.ESNext,
    target: ts.ScriptTarget.ES2022,
  },
}).outputText;
const importless = transpiled.replace(/^import .*;\s*$/gm, "");
const testableModule = `class ApiError extends Error {}\n${importless}`;
const moduleUrl = `data:text/javascript;base64,${Buffer.from(testableModule).toString("base64")}`;
const { accountTypeFromSession, describeUiError, whatIfApplyUiError } = await import(moduleUrl);

const personalContext = {
  surface: "positions",
  accountType: "personal",
  authProvider: "neon",
  isAdmin: false,
};

assert.equal(
  describeUiError({ status: 401, code: "session_required", message: "raw auth text" }, personalContext).kind,
  "session_required",
);

const personalDenial = describeUiError(
  { status: 403, code: "account_access_denied", message: "Account 'private-id' is outside scope." },
  personalContext,
);
assert.equal(personalDenial.kind, "account_access");
assert.match(personalDenial.message, /personal workspace/i);
assert.doesNotMatch(personalDenial.message, /private-id/);
assert.equal(personalDenial.diagnostic, null);

const sharedDenial = describeUiError(
  { status: 403, code: "account_access_denied", message: "outside scope" },
  { ...personalContext, accountType: "shared", authProvider: "shared" },
);
assert.match(sharedDenial.message, /shared session/i);
assert.match(sharedDenial.message, /account owner/i);

const bootstrapDisabled = describeUiError(
  { status: 409, code: "account_bootstrap_disabled", message: "bootstrap disabled" },
  personalContext,
);
assert.equal(bootstrapDisabled.kind, "account_provisioning");
assert.equal(bootstrapDisabled.action, "none");

const cparNotReady = describeUiError(
  { status: 503, error: "cpar_not_ready", message: "internal package message" },
  { ...personalContext, surface: "cPAR risk" },
);
assert.equal(cparNotReady.kind, "not_ready");
assert.doesNotMatch(cparNotReady.message, /internal package message/);

const adminFailure = describeUiError(
  {
    status: 503,
    code: "upstream_unavailable",
    message: "postgresql://db-user:db-password@example.test/app token=very-secret-value Authorization: Bearer opaque-credential-value",
  },
  { ...personalContext, isAdmin: true },
);
assert.equal(adminFailure.kind, "service_unavailable");
assert.match(adminFailure.diagnostic, /\[redacted\]/);
assert.doesNotMatch(adminFailure.diagnostic, /db-password|very-secret-value|opaque-credential-value/);

assert.equal(
  describeUiError({ status: 503, error: "cache_not_ready" }, personalContext).kind,
  "not_ready",
);

const validation = describeUiError(
  { status: 400, message: "Quantity must be numeric and non-zero." },
  { ...personalContext, operation: "write" },
);
assert.equal(validation.kind, "invalid_request");
assert.doesNotMatch(validation.message, /Quantity must be numeric/);

// The middleware returns 401 for an authenticated-but-unprovisioned session while
// preserving the session cookies. Account-state codes must win over the bare status,
// otherwise the UI tells the user to sign in again for a session that is still valid.
const unprovisionedOn401 = describeUiError(
  { status: 401, code: "account_context_unavailable", message: "Ceiora could not confirm the account for this session." },
  personalContext,
);
assert.equal(unprovisionedOn401.kind, "account_context");
assert.equal(unprovisionedOn401.action, "retry");

const provisioningOn401 = describeUiError(
  { status: 401, code: "account_provisioning_required", message: "provisioning" },
  personalContext,
);
assert.equal(provisioningOn401.kind, "account_provisioning");

const adminRequiredOn403 = describeUiError(
  { status: 403, code: "admin_required", message: "Admin session required." },
  personalContext,
);
assert.equal(adminRequiredOn403.kind, "admin_required");

// Invalid-request details never pass through, regardless of whether they appear safe.
const leakyAccountId = describeUiError(
  { status: 400, message: "Requested account acct_7f3a91c2 is not in the selected scope." },
  personalContext,
);
assert.doesNotMatch(leakyAccountId.message, /acct_7f3a91c2/);

const leakyPath = describeUiError(
  { status: 400, message: "Could not open /srv/app/exports/holdings.csv for this account." },
  personalContext,
);
assert.doesNotMatch(leakyPath.message, /srv\/app/);

const leakyUuid = describeUiError(
  { status: 400, message: "Account 3f2504e0-4f89-11d3-9a0c-0305e82c3301 rejected the selected row." },
  personalContext,
);
assert.doesNotMatch(leakyUuid.message, /3f2504e0/);

assert.doesNotMatch(
  describeUiError({ status: 400, message: "Each what-if row requires account_id." }, personalContext).message,
  /account_id/,
);

// Admin is a role, not a persisted holdings account type.
assert.equal(
  accountTypeFromSession(
    { authProvider: "neon", isAdmin: true },
    { auth_provider: "neon", is_admin: true },
  ),
  "personal",
);

assert.equal(
  whatIfApplyUiError(
    {
      status: "rejected",
      rejected_rows: 1,
      rejected: [{ reason_code: "invalid_quantity", message: "raw row details" }],
    },
    "What-if changes were rejected.",
  ),
  "What-if changes were rejected. Enter a numeric, non-zero quantity.",
);
assert.equal(
  whatIfApplyUiError({ status: "ok", rejected_rows: 0, rejected: [] }, "fallback"),
  null,
);

for (const relativePath of [
  "src/features/whatif/useWhatIfScenarioLab.ts",
  "src/features/cpar/components/useCparExploreScenarioLab.ts",
]) {
  const consumer = fs.readFileSync(path.join(frontendRoot, relativePath), "utf8");
  assert.match(consumer, /whatIfApplyUiError\(\s*out,/);
}

console.log("ui error contract ok");
