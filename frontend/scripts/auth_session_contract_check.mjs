import assert from "node:assert/strict";
import fs from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import ts from "typescript";

const frontendRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const source = fs.readFileSync(path.join(frontendRoot, "src/lib/appAuth.ts"), "utf8");
const transpiled = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.ESNext,
    target: ts.ScriptTarget.ES2022,
  },
}).outputText;
const importless = transpiled.replace(/^import .*;\s*$/gm, "");
const shortIdentityTokenLifetimeSeconds = 60;
const testableModule = `
const createLocalJWKSet = () => ({});
const createRemoteJWKSet = () => ({});
const jwtVerify = async () => ({
  payload: {
    sub: "neon-user",
    email: "user@example.test",
    exp: Math.floor(Date.now() / 1000) + ${shortIdentityTokenLifetimeSeconds},
  },
});
${importless}`;
const moduleUrl = `data:text/javascript;base64,${Buffer.from(testableModule).toString("base64")}`;

process.env.APP_AUTH_PROVIDER = "neon";
process.env.CEIORA_SESSION_SECRET = "test-session-secret";
process.env.NEON_AUTH_ISSUER = "https://issuer.example.test";
process.env.NEON_AUTH_JWKS_JSON = '{"keys":[]}';

const { authenticateNeonLogin } = await import(moduleUrl);
const beforeLogin = Math.floor(Date.now() / 1000);
const session = await authenticateNeonLogin("short-lived-identity-token");
const afterLogin = Math.floor(Date.now() / 1000);
const expectedLifetimeSeconds = 60 * 60 * 24 * 30;

assert.ok(session, "verified Neon login should create a Ceiora session");
assert.ok(
  session.expiresAt >= beforeLogin + expectedLifetimeSeconds
    && session.expiresAt <= afterLogin + expectedLifetimeSeconds,
  "Ceiora session should last 30 days regardless of the Neon identity token expiry",
);
assert.ok(
  session.expiresAt > afterLogin + shortIdentityTokenLifetimeSeconds,
  "Ceiora session must outlive the short-lived Neon identity token",
);

console.log("auth session contract ok");
