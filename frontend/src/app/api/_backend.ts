import { NextRequest, NextResponse } from "next/server";

export function backendOrigin(): string {
  return (process.env.BACKEND_API_ORIGIN || "http://127.0.0.1:8000").replace(/\/+$/, "");
}

export function controlBackendOrigin(): string {
  return (process.env.BACKEND_CONTROL_ORIGIN || process.env.BACKEND_API_ORIGIN || "http://127.0.0.1:8000").replace(
    /\/+$/,
    "",
  );
}

const AUTH_HEADER_NAMES = ["authorization", "x-operator-token", "x-editor-token"] as const;

export function forwardedAuthHeaders(req: NextRequest, extra: HeadersInit = {}): Headers {
  const headers = new Headers(extra);
  for (const name of AUTH_HEADER_NAMES) {
    const value = req.headers.get(name);
    if (value) {
      headers.set(name, value);
    }
  }
  return headers;
}

export async function proxyJson(req: NextRequest, upstream: string, options?: { method?: string; headers?: HeadersInit }) {
  const body = req.method === "GET" || req.method === "HEAD" ? undefined : await req.text();
  const res = await fetch(upstream, {
    method: options?.method || req.method,
    headers: forwardedAuthHeaders(req, options?.headers),
    body,
    cache: "no-store",
  });
  const text = await res.text();
  return new NextResponse(text, {
    status: res.status,
    headers: { "content-type": res.headers.get("content-type") || "application/json" },
  });
}
