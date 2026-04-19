"use client";

import { usePathname } from "next/navigation";
import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { isProtectedPagePath } from "@/lib/appAccess";
import type { AppAuthProvider } from "@/lib/appAuth";

type AppAuthContextPayload = {
  auth_provider: AppAuthProvider;
  subject: string;
  email: string | null;
  display_name: string | null;
  is_admin: boolean;
  account_enforcement_enabled: boolean;
  default_account_id: string | null;
  account_ids: string[];
  admin_settings_enabled: boolean;
};

type AppSessionPayload = {
  authProvider: AppAuthProvider;
  username: string;
  email?: string;
  displayName?: string;
  defaultAccountId?: string | null;
  isAdmin: boolean;
  primary: boolean;
};

type AuthSessionState = {
  loading: boolean;
  authenticated: boolean;
  session: AppSessionPayload | null;
  context: AppAuthContextPayload | null;
  neonProjectUrl: string;
  error: string | null;
  refresh: () => Promise<void>;
};

const AuthSessionContext = createContext<AuthSessionState>({
  loading: true,
  authenticated: false,
  session: null,
  context: null,
  neonProjectUrl: "",
  error: null,
  refresh: async () => {},
});

async function loadSessionState() {
  const res = await fetch("/api/auth/session", {
    method: "GET",
    credentials: "same-origin",
    cache: "no-store",
  });
  if (!res.ok) {
    const payload = await res.json().catch(() => ({}));
    const detail =
      typeof payload?.detail === "string"
        ? payload.detail
        : typeof payload?.detail?.message === "string"
          ? payload.detail.message
          : "";
    throw new Error(detail || `Session read failed (${res.status})`);
  }
  return (await res.json()) as {
    authenticated: boolean;
    session?: AppSessionPayload;
    context?: AppAuthContextPayload | null;
  };
}

export function AuthSessionProvider({ children, neonProjectUrl = "" }: { children: ReactNode; neonProjectUrl?: string }) {
  const pathname = usePathname();
  const [loading, setLoading] = useState(true);
  const [authenticated, setAuthenticated] = useState(false);
  const [session, setSession] = useState<AppSessionPayload | null>(null);
  const [context, setContext] = useState<AppAuthContextPayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [booted, setBooted] = useState(false);

  const refresh = useCallback(async () => {
    if (!booted) setLoading(true);
    try {
      const payload = await loadSessionState();
      setAuthenticated(Boolean(payload.authenticated));
      setSession(payload.authenticated ? payload.session ?? null : null);
      setContext(payload.authenticated ? payload.context ?? null : null);
      setError(null);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Session service unavailable.";
      setAuthenticated(false);
      setSession(null);
      setContext(null);
      setError(message || "Session service unavailable.");
    } finally {
      setLoading(false);
      setBooted(true);
    }
  }, [booted]);

  useEffect(() => {
    if (!pathname || !isProtectedPagePath(pathname)) {
      setLoading(false);
      setAuthenticated(false);
      setSession(null);
      setContext(null);
      setError(null);
      return;
    }
    const abort = new AbortController();
    void (async () => {
      if (abort.signal.aborted) return;
      await refresh();
    })();
    return () => {
      abort.abort();
    };
  }, [pathname, refresh]);

  const value = useMemo(
    () => ({ loading, authenticated, session, context, neonProjectUrl, error, refresh }),
    [authenticated, context, error, loading, neonProjectUrl, refresh, session],
  );

  return <AuthSessionContext.Provider value={value}>{children}</AuthSessionContext.Provider>;
}

export function useAuthSession() {
  return useContext(AuthSessionContext);
}
