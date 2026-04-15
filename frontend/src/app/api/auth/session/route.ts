import { NextRequest, NextResponse } from "next/server";
import { readSessionFromRequest } from "@/lib/appAuth";

export async function GET(req: NextRequest) {
  const session = await readSessionFromRequest(req);
  if (!session) {
    return NextResponse.json({ authenticated: false });
  }
  return NextResponse.json({
    authenticated: true,
    session: {
      username: session.username,
      primary: session.primary,
      expiresAt: session.expiresAt,
    },
  });
}
