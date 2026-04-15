import Link from "next/link";
import { cookies } from "next/headers";
import { readSessionFromCookieStore } from "@/lib/appAuth";

export default async function PublicLandingPage() {
  const cookieStore = await cookies();
  const session = await readSessionFromCookieStore(cookieStore);

  return (
    <div className="settings-page">
      <div className="settings-shell chart-card" style={{ maxWidth: 840 }}>
        <div className="settings-header">
          <div className="settings-kicker">Ceiora</div>
          <h1 style={{ margin: "0 0 10px", fontSize: "clamp(2rem, 4vw, 3.4rem)" }}>
            Public landing page coming next.
          </h1>
          <div className="settings-section-desc" style={{ maxWidth: 620 }}>
            The authenticated app now lives behind sign-in. The current dashboard home is available at <code>/home</code> after login,
            and the future public front page will replace this temporary placeholder.
          </div>
        </div>

        <section className="settings-section">
          <div className="settings-inline-row">
            <div className="settings-inline-copy">
              <div className="settings-option-label">App access</div>
              <div className="settings-option-help">
                Sign in to reach the protected dashboard routes, including cUSE, cPAR, positions, data, and privileged settings.
              </div>
            </div>
            <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
              {session ? (
                <Link href="/home" className="btn btn-secondary">
                  Continue to app
                </Link>
              ) : (
                <Link href="/login?returnTo=/home" className="btn btn-secondary">
                  Log in
                </Link>
              )}
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}
