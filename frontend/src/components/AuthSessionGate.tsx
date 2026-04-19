"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { isProtectedPagePath } from "@/lib/appAccess";
import { useAuthSession } from "@/components/AuthSessionContext";

export default function AuthSessionGate({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const { loading, authenticated, error, refresh } = useAuthSession();
  const protectedPage = Boolean(pathname && isProtectedPagePath(pathname));

  if (!protectedPage) return <>{children}</>;

  if (loading) {
    return (
      <div className="auth-session-gate">
        <div className="auth-session-gate-shell">
          <span className="auth-session-gate-folio">Checking session</span>
          <h2 className="auth-session-gate-title">Loading your workspace.</h2>
          <p className="auth-session-gate-copy">
            Verifying your session and active account before the dashboard renders.
          </p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="auth-session-gate">
        <div className="auth-session-gate-shell">
          <span className="auth-session-gate-folio">Account context unavailable</span>
          <h2 className="auth-session-gate-title">This session needs attention.</h2>
          <p className="auth-session-gate-copy">{error}</p>
          <div className="auth-session-gate-actions">
            <button
              type="button"
              className="public-inline-action auth-session-gate-button"
              onClick={() => {
                void refresh();
              }}
            >
              Retry <span aria-hidden="true">↗</span>
            </button>
            <Link href="/login" className="public-inline-action">
              Return to login <span aria-hidden="true">↗</span>
            </Link>
          </div>
        </div>
      </div>
    );
  }

  if (!authenticated) {
    return (
      <div className="auth-session-gate">
        <div className="auth-session-gate-shell">
          <span className="auth-session-gate-folio">Sign in required</span>
          <h2 className="auth-session-gate-title">Your session is no longer available.</h2>
          <p className="auth-session-gate-copy">
            Return to login and start a fresh session before opening protected pages.
          </p>
          <div className="auth-session-gate-actions">
            <Link href="/login" className="public-inline-action">
              Return to login <span aria-hidden="true">↗</span>
            </Link>
          </div>
        </div>
      </div>
    );
  }

  return <>{children}</>;
}
