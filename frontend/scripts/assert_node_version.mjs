const requiredMajor = 20;
const actualMajor = Number.parseInt(process.versions.node.split(".")[0], 10);

if (actualMajor !== requiredMajor) {
  console.error(
    `Frontend build scripts require Node ${requiredMajor}.x; current runtime is ${process.versions.node}.`,
  );
  process.exit(1);
}
