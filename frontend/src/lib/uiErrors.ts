import { ApiError } from "@/lib/apiTransport";
import type { AppAuthContextPayload } from "@/app/api/auth/_context";
import type { AppSessionPayload } from "@/lib/authSessionBootstrap";

export type UiAccountType = "personal" | "shared" | "system" | "admin" | "unknown";
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
  authenticated?: boolean;
  authProvider?: AppSessionPayload["authProvider"] | null;
  isAdmin?: boolean;
}

export interface UiErrorDescription {
  kind: UiErrorKind;
  title: string;
  message: string;
  action: "sign_in" | "retry" | "change_account" | "contact_owner" | "contact_operator" | "none";
  actionLabel: string | null;
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
}

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
    };
  }
  return { status: null, code: "", rawMessage: "", buildProfile: "", timedOut: false };
}

function inferKind(meta: ErrorMeta): UiErrorKind {
  const haystack = `${meta.code} ${meta.rawMessage}`.toLowerCase();
  // Account-state codes must be checked before the bare 401 status. The middleware
  // returns 401 for an authenticated-but-unprovisioned session while deliberately
  // preserving the session cookies, so classifying on status alone would tell the
  // user to sign in again for a session that is still valid.
  if (/account_bootstrap_disabled|account_provisioning_required|no account memberships/.test(haystack)) {
    return "account_provisioning";
  }
  if (/account_context_unavailable|could not load authenticated account context/.test(haystack)) {
    return "account_context";
  }
  if (/admin_required|admin session required/.test(haystack)) return "admin_required";
  if (meta.status === 401 || /session_expired|session_required|sign in required|authenticated app session required/.test(haystack)) {
    return "session_required";
  }
  if (meta.status === 403 || /outside the authenticated scope|not allowlisted|account_access_denied/.test(haystack)) {
    return "account_access";
  }
  if (/cpar_not_ready|not ready|no serving snapshot|no published/.test(haystack)) return "not_ready";
  if (meta.status === 404) return "not_found";
  if (meta.status === 400 || meta.status === 409 || meta.status === 422) return "invalid_request";
  if (meta.status === 429) return "rate_limited";
  if (meta.timedOut || meta.status === 408 || meta.status === 504) return "timeout";
  if (meta.status === 502 || meta.status === 503 || /authority_unavailable|upstream_unavailable|service unavailable|network/.test(haystack)) {
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
    case "admin":
      return "your admin workspace";
    default:
      return "this account";
  }
}

function safeValidationMessage(rawMessage: string): string | null {
  const clean = rawMessage.replace(/\s+/g, " ").trim();
  if (!clean || clean.length > 240) return null;
  if (/traceback|exception|sql|postgres|sqlite|neon not available/i.test(clean)) return null;
  // Never echo identifiers or filesystem/network locations back to the user: the
  // keyword allowlist below is broad enough that an upstream message mentioning an
  // account id or an internal path would otherwise render verbatim.
  if (/(^|[\s"'(])\/\S/.test(clean)) return null;
  // Match generated identifier *values* (acct_7f3a91c2), not the literal field names
  // that legitimately appear in validation copy ("Each row requires account_id.").
  if (/\b(?:acct|acc|account|user|org|sub)_(?=[a-z0-9-]*\d)[a-z0-9-]{6,}/i.test(clean)) return null;
  if (/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i.test(clean)) return null;
  if (/[0-9a-f]{16,}/i.test(clean)) return null;
  if (!/account|quantity|ticker|ric|row|csv|scenario|security|position|select|provide|required|duplicate|maximum|max\b/i.test(clean)) {
    return null;
  }
  return clean;
}

function sanitizeDiagnostic(value: string): string {
  return value
    .replace(/(postgres(?:ql)?:\/\/)[^@\s]+@/gi, "$1[redacted]@")
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
  if (context?.is_admin || session?.isAdmin) return "admin";
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
    case "admin":
      return "Admin";
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
        actionLabel: "Return to login",
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
        action: bootstrapDisabled ? "contact_operator" : "retry",
        actionLabel: bootstrapDisabled ? null : "Try again",
        diagnostic,
      };
    }
    case "account_context":
      return {
        kind,
        title: "Account context unavailable",
        message: `Ceiora could not confirm which portfolio belongs to this session, so ${surface} was withheld. Your holdings were not changed.`,
        action: "retry",
        actionLabel: "Try again",
        diagnostic,
      };
    case "admin_required":
      return {
        kind,
        title: "Admin access required",
        message: `${surface} is a maintenance surface and is not available to personal or shared accounts. Your portfolio access is unaffected.`,
        action: "none",
        actionLabel: null,
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
        action: context.isAdmin ? "change_account" : accountType === "shared" ? "contact_owner" : "change_account",
        actionLabel: null,
        diagnostic,
      };
    }
    case "invalid_request":
      return {
        kind,
        title: "Check this request",
        message: safeValidationMessage(meta.rawMessage)
          || `Ceiora could not use the submitted ${surface} request. Review the selected account and entered values, then try again.`,
        action: "none",
        actionLabel: null,
        diagnostic,
      };
    case "not_found":
      return {
        kind,
        title: "Requested data not found",
        message: `${surface} is not available for ${account}. It may not be included in the active model package or may no longer exist.`,
        action: "change_account",
        actionLabel: null,
        diagnostic,
      };
    case "not_ready":
      return {
        kind,
        title: "Model data not ready",
        message: `${surface} has not been published for ${account} yet. Existing holdings are unchanged${context.isAdmin ? "; publish the required model package, then retry" : "; try again after the next model update"}.`,
        action: "retry",
        actionLabel: "Try again",
        diagnostic,
      };
    case "rate_limited":
      return {
        kind,
        title: "Too many requests",
        message: `${surface} is temporarily paused. Wait a moment before trying again.`,
        action: "retry",
        actionLabel: "Try again",
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
        actionLabel: "Try again",
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
        actionLabel: "Try again",
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
        actionLabel: "Try again",
        diagnostic,
      };
  }
}

export function uiErrorMessage(error: unknown, context: UiErrorContext = {}): string {
  return describeUiError(error, context).message;
}

export function validationUiMessage(value: unknown, fallback: string): string {
  return safeValidationMessage(cleanText(value)) || fallback;
}
