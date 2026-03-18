import type { ExposureOrigin, ModelStatus } from "@/lib/types";

export type ExposureTier = "core" | "fundamental" | "returns";

export function normalizeExposureOrigin(
  origin?: ExposureOrigin | null,
  modelStatus?: ModelStatus | null,
): ExposureOrigin {
  const raw = String(origin || "").trim();
  if (raw === "projected") {
    return "projected_returns";
  }
  if (modelStatus === "projected_only" && (raw === "" || raw === "native")) {
    return "projected_fundamental";
  }
  if (raw === "projected_fundamental" || raw === "projected_returns" || raw === "native") {
    return raw;
  }
  if (modelStatus === "projected_only") {
    return "projected_fundamental";
  }
  return "native";
}

export function exposureTier(
  origin?: ExposureOrigin | null,
  modelStatus?: ModelStatus | null,
): ExposureTier {
  const normalized = normalizeExposureOrigin(origin, modelStatus);
  if (normalized === "projected_returns") return "returns";
  if (normalized === "projected_fundamental") return "fundamental";
  return "core";
}

export function exposureMethodLabel(
  origin?: ExposureOrigin | null,
  modelStatus?: ModelStatus | null,
): string {
  if (modelStatus === "core_estimated") return "Core";
  if (modelStatus === "ineligible" && !origin) return "Ineligible";
  const tier = exposureTier(origin, modelStatus);
  if (tier === "fundamental") return "Fundamental Projection";
  if (tier === "returns") return "Returns Projection";
  return modelStatus === "ineligible" ? "Ineligible" : "Core";
}
