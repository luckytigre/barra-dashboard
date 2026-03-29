import { existsSync, readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";

const buildIdPath = ".next/BUILD_ID";
const contractPath = ".next/build-contract.json";
const maxAttempts = 40;
const waitMs = 100;
const settleMs = 2000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForCompileContract() {
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    if (existsSync(buildIdPath) && existsSync(contractPath)) {
      const buildId = readFileSync(buildIdPath, "utf8").trim();
      const contract = JSON.parse(readFileSync(contractPath, "utf8"));
      if (buildId && contract.buildId === buildId) {
      // On this workspace the BUILD_ID can exist before the next generate run can reopen it.
        await sleep(settleMs);
        return;
      }
    }
    await sleep(waitMs);
  }
  console.error(`Missing fresh compile contract. Run npm run build:compile first.`);
  process.exit(1);
}

await waitForCompileContract();

const result = spawnSync(
  process.execPath,
  ["./node_modules/next/dist/bin/next", "build", "--experimental-build-mode", "generate"],
  {
    env: { ...process.env, NEXT_TELEMETRY_DISABLED: "1" },
    stdio: "inherit",
  },
);

if (result.error) {
  throw result.error;
}

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}
