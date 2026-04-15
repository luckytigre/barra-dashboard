import { NextResponse } from "next/server";
import { APP_SESSION_COOKIE_NAME, clearedAppSessionCookieOptions } from "@/lib/appAuth";

export async function POST() {
  const res = NextResponse.json({ ok: true });
  res.cookies.set(APP_SESSION_COOKIE_NAME, "", clearedAppSessionCookieOptions());
  return res;
}
