import { NextRequest, NextResponse } from "next/server";
import { controlBackendOrigin, forwardedAuthHeaders } from "@/app/api/_backend";

export const dynamic = "force-dynamic";

export async function GET(req: NextRequest) {
  const upstream = `${controlBackendOrigin()}/api/refresh/status`;
  const res = await fetch(upstream, {
    method: "GET",
    headers: forwardedAuthHeaders(req),
    cache: "no-store",
  });
  const body = await res.text();
  return new NextResponse(body, {
    status: res.status,
    headers: { "content-type": res.headers.get("content-type") || "application/json" },
  });
}
