import { ApiError } from "@/lib/apiTransport";
import type { AppAuthContextPayload } from "@/app/api/auth/_context";
import type { AppSessionPayload } from "@/lib/authSessionBootstrap";

export type UiAccountType = "personal" | "shared" | "system" | "unknown";
export type UiErrorKind =
  | "session_required"
  | "account_provisioning"
  | "account_context"
  | "account_access"
  | "admin_required"
  | "invalid_request"
  | "not_found"
  | "not_ready"
  | "rate_limited"
  | "timeout"
  | "service_unavailable"
  | "unexpected";

export interface UiErrorContext {
  surface?: string;
  operation?: "read" | "write" | "recalculate";
  accountType?: UiAccountType | null;
  accountName?: string | null;
  authProvider?: AppSessionPayload["authProvider"] | null;
  isAdmin?: boolean;
}

export interface UiErrorDescription {
  kind: UiErrorKind;
  title: string;
  message: string;
  action: "sign_in" | "retry" | "none";
  diagnostic: string | null;
}

type ErrorDetail = {
  code?: unknown;
  error?: unknown;
  message?: unknown;
  build_profile?: unknown;
};

interface ErrorMeta {
  status: number | null;
  code: string;
  rawMessage: string;
  buildProfile: string;
  timedOut: boolean;
  networkFailure: boolean;
}

const ERROR_KIND_BY_CODE: Partial<Record<string, UiErrorKind>> = {
  account_bootstrap_disabled: "account_provisioning",
  account_provisioning_required: "account_provisioning",
  account_context_unavailable: "account_context",
  admin_required: "admin_required",
  session_expired: "session_required",
  session_required: "session_required",
  account_access_denied: "account_access",
  cache_not_ready: "not_ready",
  cpar_not_ready: "not_ready",
  cpar_authority_unavailable: "service_unavailable",
  authority_unavailable: "service_unavailable",
  upstream_unavailable: "service_unavailable",
};

function cleanText(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function readErrorMeta(error: unknown): ErrorMeta {
  if (error instanceof ApiError) {
    const detail = error.detail;
    const objectDetail = detail && typeof detail === "object" ? detail as ErrorDetail : null;
    return {
      status: error.status,
      code: cleanText(objectDetail?.code || objectDetail?.error).toLowerCase(),
      rawMessage: cleanText(typeof detail === "string" ? detail : objectDetail?.message) || cleanText(error.message),
      buildProfile: cleanText(objectDetail?.build_profile),
      timedOut: false,
      networkFailure: false,
    };
  }
  if (error instanceof Error) {
    const message = cleanText(error.message);
    return {
      status: null,
      code: "",
      rawMessage: message,
      buildProfile: "",
      timedOut: error.name === "AbortError" || /timed? out|timeout|aborted/i.test(message),
      networkFailure: error instanceof TypeError,
    };
  }
  if (error && typeof error === "object") {
    const value = error as ErrorDetail & { status?: unknown };
    const status = typeof value.status === "number" ? value.status : null;
    const rawMessage = cleanText(value.message);
    return {
      status,
      code: cleanText(value.code || value.error).toLowerCase(),
      rawMessage,
      buildProfile: cleanText(value.build_profile),
      timedOut: /timed? out|timeout|aborted/i.test(rawMessage),
      networkFailure: false,
    };
  }
  return { status: null, code: "", rawMessage: "", buildProfile: "", timedOut: false, networkFailure: false };
}

function inferKind(meta: ErrorMeta): UiErrorKind {
  // Structured account-state codes win over status. In particular, middleware may
  // return 401 for a valid but unprovisioned session whose cookies remain intact.
  const codedKind = ERROR_KIND_BY_CODE[meta.code];
  if (codedKind) return codedKind;
  if (meta.status === 401) return "session_required";
  if (meta.status === 403) return "account_access";
  if (meta.status === 404) return "not_found";
  if (meta.status === 400 || meta.status === 409 || meta.status === 422) return "invalid_request";
  if (meta.status === 429) return "rate_limited";
  if (meta.timedOut || meta.status === 408 || meta.status === 504) return "timeout";
  if (meta.networkFailure || meta.status === 502 || meta.status === 503) {
    return "service_unavailable";
  }
  return "unexpected";
}

function normalizedSurface(value: string | undefined): string {
  const clean = cleanText(value);
  return clean || "this view";
}

function accountLabel(context: UiErrorContext): string {
  const named = cleanText(context.accountName);
  if (named) return named;
  switch (context.accountType) {
    case "personal":
      return "your personal portfolio";
    case "shared":
      return "this shared account";
    case "system":
      return "this system account";
    default:
      return "this account";
  }
}

function sanitizeDiagnostic(value: string): string {
  return value
    .replace(/(postgres(?:ql)?:\/\/)[^@\s]+@/gi, "$1[redacted]@")
    .replace(/(authorization\s*[:=]\s*)(?:bearer\s+)?[^\s,;]+/gi, "$1[redacted]")
    .replace(/(bearer\s+)[A-Za-z0-9._~-]{10,}/gi, "$1[redacted]")
    .replace(/((?:authorization|token|password|secret|api[_-]?key)\s*[:=]\s*)[^\s,;]+/gi, "$1[redacted]")
    .replace(/\beyJ[A-Za-z0-9_-]{20,}(?:\.[A-Za-z0-9_-]{10,}){1,2}\b/g, "[redacted token]");
}

function adminDiagnostic(meta: ErrorMeta, isAdmin: boolean): string | null {
  if (!isAdmin) return null;
  const parts: string[] = [];
  if (meta.status) parts.push(`HTTP ${meta.status}`);
  if (meta.code) parts.push(meta.code);
  if (meta.buildProfile) parts.push(`build profile: ${meta.buildProfile}`);
  if (meta.rawMessage) parts.push(sanitizeDiagnostic(meta.rawMessage.replace(/\s+/g, " ")).slice(0, 320));
  return parts.length ? parts.join(" · ") : null;
}

export function accountTypeFromSession(
  session: AppSessionPayload | null | undefined,
  context: AppAuthContextPayload | null | undefined,
): UiAccountType {
  if (session?.authProvider === "shared" || context?.auth_provider === "shared") return "shared";
  if (session?.authProvider === "neon" || context?.auth_provider === "neon") return "personal";
  return "unknown";
}

export function accountTypeLabel(accountType: UiAccountType | null | undefined): string {
  switch (accountType) {
    case "personal":
      return "Personal";
    case "shared":
      return "Shared";
    case "system":
      return "System";
    default:
      return "Account";
  }
}

export function describeUiError(error: unknown, context: UiErrorContext = {}): UiErrorDescription {
  const meta = readErrorMeta(error);
  const kind = inferKind(meta);
  const surface = normalizedSurface(context.surface);
  const account = accountLabel(context);
  const operation = context.operation ?? "read";
  const diagnostic = adminDiagnostic(meta, Boolean(context.isAdmin));

  switch (kind) {
    case "session_required":
      return {
        kind,
        title: "Sign in again",
        message: `Your Ceiora session ended before ${surface} could ${operation === "read" ? "load" : "finish"}. Sign in again to continue; no account access was changed.`,
        action: "sign_in",
        diagnostic,
      };
    case "account_provisioning": {
      const bootstrapDisabled = meta.code === "account_bootstrap_disabled";
      return {
        kind,
        title: bootstrapDisabled
          ? "Personal workspace setup paused"
          : context.authProvider === "neon" ? "Personal workspace not ready" : "Account setup incomplete",
        message: bootstrapDisabled
          ? "Your identity is valid, but automatic personal portfolio creation is currently disabled. Ask the operator to finish account setup."
          : context.authProvider === "neon"
          ? `Your identity is valid, but Ceiora has not finished creating your personal portfolio. Try again shortly${context.isAdmin ? " or review account provisioning" : ""}.`
          : `Your sign-in is valid, but no portfolio is assigned to this account yet. Ask the account owner or operator to finish setup.`,
        action: bootstrapDisabled ? "none" : "retry",
        diagnostic,
      };
    }
    case "account_context":
      return {
        kind,
        title: "Account context unavailable",
        message: `Ceiora could not confirm which portfolio belongs to this session, so ${surface} was withheld. Your holdings were not changed.`,
        action: "retry",
        diagnostic,
      };
    case "admin_required":
      return {
        kind,
        title: "Admin access required",
        message: `${surface} is a maintenance surface and is not available to personal or shared accounts. Your portfolio access is unaffected.`,
        action: "none",
        diagnostic,
      };
    case "account_access": {
      const accountType = context.accountType ?? "unknown";
      const message = context.isAdmin
        ? `${account} is outside the memberships assigned to this admin session. Admin status does not bypass portfolio scope.`
        : accountType === "shared"
          ? `${surface} requested an account that is not included in this shared session. Ask the account owner to grant access or choose an available account.`
          : accountType === "system"
            ? `${surface} cannot change a system account from this screen. Use the approved maintenance workflow.`
            : `${surface} requested a portfolio outside your personal workspace. Choose one of your available accounts or sign in with the identity that owns it.`;
      return {
        kind,
        title: "Account access unavailable",
        message,
        action: "none",
        diagnostic,
      };
    }
    case "invalid_request":
      return {
        kind,
        title: "Check this request",
        message: `Ceiora could not use the submitted ${surface} request. Review the selected account and entered values, then try again.`,
        action: "none",
        diagnostic,
      };
    case "not_found":
      return {
        kind,
        title: "Requested data not found",
        message: `${surface} is not available for ${account}. It may not be included in the active model package or may no longer exist.`,
        action: "none",
        diagnostic,
      };
    case "not_ready":
      return {
        kind,
        title: "Model data not ready",
        message: `${surface} has not been published for ${account} yet. Existing holdings are unchanged${context.isAdmin ? "; publish the required model package, then retry" : "; try again after the next model update"}.`,
        action: "retry",
        diagnostic,
      };
    case "rate_limited":
      return {
        kind,
        title: "Too many requests",
        message: `${surface} is temporarily paused. Wait a moment before trying again.`,
        action: "retry",
        diagnostic,
      };
    case "timeout":
      return {
        kind,
        title: operation === "write" ? "Change not confirmed" : "Request timed out",
        message: operation === "write"
          ? `Ceiora did not receive confirmation that the ${surface} change finished. Review current holdings before retrying so the change is not applied twice.`
          : `${surface} took too long to respond. No account data was changed; try again when the connection is stable.`,
        action: "retry",
        diagnostic,
      };
    case "service_unavailable":
      return {
        kind,
        title: operation === "write" ? "Change not confirmed" : `${surface.charAt(0).toUpperCase()}${surface.slice(1)} unavailable`,
        message: operation === "write"
          ? `Ceiora could not confirm that the change to ${account} finished. Review current holdings before retrying; do not assume the change was saved.`
          : `Ceiora could not load ${surface} for ${account}. Your holdings are unchanged; try again when the data service is available.`,
        action: "retry",
        diagnostic,
      };
    default:
      return {
        kind,
        title: operation === "write" ? "Change not confirmed" : "Something went wrong",
        message: operation === "write"
          ? `Ceiora could not confirm that the ${surface} change finished. Review current holdings before trying again.`
          : `${surface} could not be loaded. Your holdings are unchanged; try again.`,
        action: "retry",
        diagnostic,
      };
  }
}

export function uiErrorMessage(error: unknown, context: UiErrorContext = {}): string {
  return describeUiError(error, context).message;
}

export function rejectionReasonUiMessage(reasonCode: unknown): string {
  switch (cleanText(reasonCode).toLowerCase()) {
    case "invalid_account_id":
      return "Select an available account.";
    case "missing_identifier":
      return "Add a ticker or RIC.";
    case "invalid_quantity":
    case "zero_quantity":
      return "Enter a numeric, non-zero quantity.";
    case "duplicate_row_in_file":
    case "duplicate_resolved_instrument":
      return "Remove the duplicate security row.";
    case "unknown_ric":
    case "unknown_ticker":
      return "Choose a security available in the active registry.";
    case "identifier_mismatch":
      return "Use a ticker and RIC that identify the same security.";
    default:
      return "Review the account, security, and quantity.";
  }
}

export function whatIfApplyUiError(
  response: {
    status?: unknown;
    rejected_rows?: unknown;
    rejected?: Array<{ reason_code?: unknown }>;
  },
  fallback: string,
): string | null {
  const rejectedCount = Number(response.rejected_rows || 0);
  if (cleanText(response.status).toLowerCase() === "ok" && rejectedCount === 0) return null;
  const guidance = rejectionReasonUiMessage(response.rejected?.[0]?.reason_code);
  return response.rejected?.length || rejectedCount > 0 ? `${fallback} ${guidance}` : fallback;
}
