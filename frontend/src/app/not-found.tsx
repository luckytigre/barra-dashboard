import Link from "next/link";

export default function NotFound() {
  return (
    <div className="auth-session-gate">
      <div className="auth-session-gate-shell">
        <span className="auth-session-gate-folio">Page not found</span>
        <h2 className="auth-session-gate-title">This Ceiora page does not exist.</h2>
        <p className="auth-session-gate-copy">
          The address may be outdated or outside the pages available to this account. No portfolio data was changed.
        </p>
        <div className="auth-session-gate-actions">
          <Link href="/home" className="public-inline-action">
            Return home <span aria-hidden="true">↗</span>
          </Link>
        </div>
      </div>
    </div>
  );
}
