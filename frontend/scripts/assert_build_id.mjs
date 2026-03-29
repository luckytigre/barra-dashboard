import { existsSync, readFileSync, writeFileSync } from "node:fs";

const buildIdPath = ".next/BUILD_ID";
const contractPath = ".next/build-contract.json";

if (!existsSync(buildIdPath)) {
  console.error(`Missing ${buildIdPath}. The compile phase did not finish cleanly.`);
  process.exit(1);
}

const buildId = readFileSync(buildIdPath, "utf8").trim();
if (!buildId) {
  console.error(`${buildIdPath} exists but is empty.`);
  process.exit(1);
}

writeFileSync(contractPath, JSON.stringify({ buildId }, null, 2));
console.log(`Verified ${buildIdPath}: ${buildId}`);
