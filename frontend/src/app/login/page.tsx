"use client";

import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { FormEvent, Suspense, useMemo, useState } from "react";
import { DEFAULT_APP_HOME_PATH, normalizeReturnTo } from "@/lib/appAccess";

export default function LoginPage() {
  return (
    <Suspense fallback={<LoginShell status="idle" errorMessage="" configError={false} returnTo={DEFAULT_APP_HOME_PATH} />}>
      <LoginPageInner />
    </Suspense>
  );
}

function LoginPageInner() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [status, setStatus] = useState<"idle" | "submitting" | "failed">("idle");
  const [errorMessage, setErrorMessage] = useState("");
  const returnTo = useMemo(
    () => normalizeReturnTo(searchParams.get("returnTo") || DEFAULT_APP_HOME_PATH),
    [searchParams],
  );
  const configError = searchParams.get("error") === "misconfigured";

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setStatus("submitting");
    setErrorMessage("");
    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ username, password, returnTo }),
      });
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        setStatus("failed");
        setErrorMessage(typeof payload?.detail === "string" ? payload.detail : "Could not sign in.");
        return;
      }
      router.replace(typeof payload?.returnTo === "string" ? payload.returnTo : returnTo);
      router.refresh();
    } catch {
      setStatus("failed");
      setErrorMessage("Could not sign in.");
    }
  }

  return <LoginShell
    username={username}
    password={password}
    status={status}
    errorMessage={errorMessage}
    configError={configError}
    returnTo={returnTo}
    onUsernameChange={setUsername}
    onPasswordChange={setPassword}
    onSubmit={handleSubmit}
  />;
}

type LoginShellProps = {
  username?: string;
  password?: string;
  status: "idle" | "submitting" | "failed";
  errorMessage: string;
  configError: boolean;
  returnTo: string;
  onUsernameChange?: (value: string) => void;
  onPasswordChange?: (value: string) => void;
  onSubmit?: (event: FormEvent<HTMLFormElement>) => void;
};

function LoginShell({
  username = "",
  password = "",
  status,
  errorMessage,
  configError,
  returnTo,
  onUsernameChange,
  onPasswordChange,
  onSubmit,
}: LoginShellProps) {
  return (
    <div className="settings-page">
      <div className="settings-shell chart-card" style={{ maxWidth: 560 }}>
        <div className="settings-header">
          <div className="settings-kicker">Sign in</div>
          <div className="settings-section-desc">
            Shared app login for the protected Ceiora dashboard surfaces.
          </div>
        </div>

        <form onSubmit={onSubmit} className="settings-section">
          <div className="settings-auth-grid">
            <label className="settings-auth-card">
              <span className="settings-option-label">Username</span>
              <input
                type="text"
                autoComplete="username"
                className="settings-auth-input"
                value={username}
                onChange={(event) => onUsernameChange?.(event.target.value)}
                placeholder="Shared account username"
              />
            </label>
            <label className="settings-auth-card">
              <span className="settings-option-label">Password</span>
              <input
                type="password"
                autoComplete="current-password"
                className="settings-auth-input"
                value={password}
                onChange={(event) => onPasswordChange?.(event.target.value)}
                placeholder="Shared account password"
              />
            </label>
          </div>

          {(configError || errorMessage) && (
            <div style={{ marginTop: 14, color: "rgba(204,53,88,0.95)" }}>
              {configError ? "App auth is not configured yet." : errorMessage}
            </div>
          )}

          <div className="settings-auth-footer" style={{ marginTop: 18 }}>
            <div className="settings-option-help">
              After sign-in you will be returned to <code>{returnTo}</code>.
            </div>
            <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
              <Link href="/" className="settings-auth-clear">
                Back
              </Link>
              <button type="submit" className="btn btn-secondary" disabled={status === "submitting"}>
                {status === "submitting" ? "Signing in..." : "Sign in"}
              </button>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
}
