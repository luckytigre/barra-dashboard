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
const { describeUiError } = await import(moduleUrl);

const personalContext = {
  surface: "positions",
  accountType: "personal",
  authenticated: true,
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
assert.equal(bootstrapDisabled.action, "contact_operator");

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
    message: "postgresql://db-user:db-password@example.test/app token=very-secret-value",
  },
  { ...personalContext, accountType: "admin", isAdmin: true },
);
assert.equal(adminFailure.kind, "service_unavailable");
assert.match(adminFailure.diagnostic, /\[redacted\]/);
assert.doesNotMatch(adminFailure.diagnostic, /db-password|very-secret-value/);

const validation = describeUiError(
  { status: 400, message: "Quantity must be numeric and non-zero." },
  { ...personalContext, operation: "write" },
);
assert.equal(validation.kind, "invalid_request");
assert.equal(validation.message, "Quantity must be numeric and non-zero.");

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

// Validation passthrough must not echo identifiers or internal locations.
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

// A plain validation message must still reach the user.
assert.equal(
  describeUiError({ status: 400, message: "Quantity must be numeric and non-zero." }, personalContext).message,
  "Quantity must be numeric and non-zero.",
);

// Literal field names are not identifier values: these real backend validation
// messages must survive the id/path denylist above.
for (const passthrough of [
  "Each what-if row requires account_id.",
  "Each cPAR explore scenario row requires account_id.",
  "account_id is required when scope=account.",
  "invalid account_id",
]) {
  assert.equal(
    describeUiError({ status: 400, message: passthrough }, personalContext).message,
    passthrough,
    `expected validation passthrough for: ${passthrough}`,
  );
}

console.log("ui error contract ok");
