import { NextRequest, NextResponse } from "next/server";
import { appSessionCookieOptions, APP_SESSION_COOKIE_NAME, authenticateSharedLogin, createSessionToken, isAppAuthConfigured } from "@/lib/appAuth";
import { normalizeReturnTo } from "@/lib/appAccess";

export async function POST(req: NextRequest) {
  if (!isAppAuthConfigured()) {
    return NextResponse.json({ detail: "App auth is not configured." }, { status: 503 });
  }

  let payload: { username?: string; password?: string; returnTo?: string } | null = null;
  try {
    payload = await req.json();
  } catch {
    return NextResponse.json({ detail: "Invalid login payload." }, { status: 400 });
  }

  const session = await authenticateSharedLogin(payload?.username || "", payload?.password || "");
  if (!session) {
    return NextResponse.json({ detail: "Invalid username or password." }, { status: 401 });
  }

  const token = await createSessionToken(session);
  const returnTo = normalizeReturnTo(payload?.returnTo);
  const res = NextResponse.json({
    ok: true,
    returnTo,
    session: {
      username: session.username,
      primary: session.primary,
      expiresAt: session.expiresAt,
    },
  });
  res.cookies.set(APP_SESSION_COOKIE_NAME, token, appSessionCookieOptions(session.expiresAt));
  return res;
}
