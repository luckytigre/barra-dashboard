"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { FormEvent, Suspense, useMemo, useState } from "react";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import LandingBackgroundLock from "@/components/LandingBackgroundLock";
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

  return (
    <LoginShell
      username={username}
      password={password}
      status={status}
      errorMessage={errorMessage}
      configError={configError}
      returnTo={returnTo}
      onUsernameChange={setUsername}
      onPasswordChange={setPassword}
      onSubmit={handleSubmit}
    />
  );
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
    <>
      <LandingBackgroundLock />
      <div className="public-login-page">
        <div className="public-login-loader-wrap">
          <AnalyticsLoadingViz message={null} />
        </div>

        <form onSubmit={onSubmit} className="public-login-form">
          <label className="public-login-field">
            <input
              type="text"
              autoComplete="username"
              value={username}
              onChange={(event) => onUsernameChange?.(event.target.value)}
              placeholder="Username"
            />
          </label>

          <label className="public-login-field">
            <input
              type="password"
              autoComplete="current-password"
              value={password}
              onChange={(event) => onPasswordChange?.(event.target.value)}
              placeholder="Password"
            />
          </label>

          {(configError || errorMessage) && (
            <div className="public-login-message" aria-live="polite">
              {configError ? "App auth is not configured yet." : errorMessage}
            </div>
          )}

          <div className="public-login-actions">
            <button type="submit" className="public-login-submit" disabled={status === "submitting"}>
              {status === "submitting" ? "Signing in..." : "Sign in"}
            </button>
          </div>
        </form>
      </div>
    </>
  );
}
