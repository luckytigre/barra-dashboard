import type { NextRequest } from "next/server";

export const APP_SESSION_COOKIE_NAME = "__session";
const LEGACY_APP_SESSION_COOKIE_NAMES = ["ceiora-app-session", "ceiora.app-session"] as const;
const APP_SESSION_COOKIE_NAMES = [APP_SESSION_COOKIE_NAME, ...LEGACY_APP_SESSION_COOKIE_NAMES] as const;

const APP_SESSION_TTL_SECONDS = 60 * 60 * 24 * 30;

export interface AppSession {
  username: string;
  primary: boolean;
  issuedAt: number;
  expiresAt: number;
}

interface AuthConfig {
  username: string;
  password: string;
  primaryUsername: string;
  secret: string;
}

interface CookieStoreLike {
  get(name: string): { value: string } | undefined;
}

function textEncoder(): TextEncoder {
  return new TextEncoder();
}

function utf8Bytes(value: string): Uint8Array {
  return textEncoder().encode(value);
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}

function base64UrlEncode(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function base64UrlDecode(value: string): Uint8Array {
  const base64 = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = `${base64}${"=".repeat((4 - (base64.length % 4 || 4)) % 4)}`;
  const binary = atob(padded);
  return Uint8Array.from(binary, (char) => char.charCodeAt(0));
}

function authConfig(): AuthConfig {
  const username = String(process.env.CEIORA_SHARED_LOGIN_USERNAME || "").trim();
  const password = String(process.env.CEIORA_SHARED_LOGIN_PASSWORD || "").trim();
  const secret = String(process.env.CEIORA_SESSION_SECRET || "").trim();
  const primaryUsername = String(process.env.CEIORA_PRIMARY_ACCOUNT_USERNAME || username).trim() || username;
  return {
    username,
    password,
    primaryUsername,
    secret,
  };
}

export function authConfigMissingKeys(): string[] {
  const cfg = authConfig();
  const missing: string[] = [];
  if (!cfg.username) missing.push("CEIORA_SHARED_LOGIN_USERNAME");
  if (!cfg.password) missing.push("CEIORA_SHARED_LOGIN_PASSWORD");
  if (!cfg.secret) missing.push("CEIORA_SESSION_SECRET");
  return missing;
}

export function isAppAuthConfigured(): boolean {
  return authConfigMissingKeys().length === 0;
}

export async function authenticateSharedLogin(username: string, password: string): Promise<AppSession | null> {
  const cfg = authConfig();
  if (!cfg.username || !cfg.password || !cfg.secret) return null;

  const cleanUsername = String(username || "").trim();
  const cleanPassword = String(password || "");
  if (cleanUsername !== cfg.username || cleanPassword !== cfg.password) return null;

  const issuedAt = Math.floor(Date.now() / 1000);
  return {
    username: cleanUsername,
    primary: cleanUsername === cfg.primaryUsername,
    issuedAt,
    expiresAt: issuedAt + APP_SESSION_TTL_SECONDS,
  };
}

async function importSigningKey(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    "raw",
    toArrayBuffer(utf8Bytes(secret)),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"],
  );
}

async function signPayload(payload: string, secret: string): Promise<string> {
  const key = await importSigningKey(secret);
  const signature = await crypto.subtle.sign("HMAC", key, toArrayBuffer(utf8Bytes(payload)));
  return base64UrlEncode(new Uint8Array(signature));
}

export async function createSessionToken(session: AppSession): Promise<string> {
  const cfg = authConfig();
  if (!cfg.secret) {
    throw new Error("App auth is not configured.");
  }
  const payload = base64UrlEncode(utf8Bytes(JSON.stringify(session)));
  const signature = await signPayload(payload, cfg.secret);
  return `${payload}.${signature}`;
}

export async function readSessionFromCookieValue(value: string | null | undefined): Promise<AppSession | null> {
  const cfg = authConfig();
  const token = String(value || "").trim();
  if (!cfg.secret || !token) return null;

  const parts = token.split(".");
  if (parts.length !== 2) return null;
  const [payload, signature] = parts;
  const expected = await signPayload(payload, cfg.secret);
  if (expected !== signature) return null;

  try {
    const decoded = JSON.parse(new TextDecoder().decode(base64UrlDecode(payload))) as Partial<AppSession>;
    const username = String(decoded.username || "").trim();
    const issuedAt = Number(decoded.issuedAt || 0);
    const expiresAt = Number(decoded.expiresAt || 0);
    if (!username || !Number.isFinite(issuedAt) || !Number.isFinite(expiresAt)) return null;
    if (expiresAt <= Math.floor(Date.now() / 1000)) return null;
    return {
      username,
      primary: Boolean(decoded.primary),
      issuedAt,
      expiresAt,
    };
  } catch {
    return null;
  }
}

function extractCookieValue(rawCookieHeader: string | null | undefined, cookieName: string): string | null {
  const header = String(rawCookieHeader || "");
  if (!header) return null;
  const prefix = `${cookieName}=`;
  for (const segment of header.split(";")) {
    const trimmed = segment.trim();
    if (trimmed.startsWith(prefix)) {
      return trimmed.slice(prefix.length);
    }
  }
  return null;
}

async function readSessionFromCookieSources(
  cookieStore: CookieStoreLike | null | undefined,
  rawCookieHeader: string | null | undefined,
): Promise<AppSession | null> {
  for (const cookieName of APP_SESSION_COOKIE_NAMES) {
    const storeValue = cookieStore?.get(cookieName)?.value;
    const session = await readSessionFromCookieValue(storeValue);
    if (session) return session;
  }

  for (const cookieName of APP_SESSION_COOKIE_NAMES) {
    const headerValue = extractCookieValue(rawCookieHeader, cookieName);
    const session = await readSessionFromCookieValue(headerValue);
    if (session) return session;
  }

  return null;
}

export async function readSessionFromRequest(req: NextRequest): Promise<AppSession | null> {
  return readSessionFromCookieSources(req.cookies, req.headers.get("cookie"));
}

export async function readSessionFromCookieStore(
  cookieStore: CookieStoreLike | null | undefined,
  rawCookieHeader?: string | null,
): Promise<AppSession | null> {
  return readSessionFromCookieSources(cookieStore, rawCookieHeader);
}

export function appSessionCookieOptions(expiresAt: number) {
  return {
    httpOnly: true,
    sameSite: "lax" as const,
    secure: process.env.NODE_ENV === "production",
    path: "/",
    expires: new Date(expiresAt * 1000),
  };
}

export function clearedAppSessionCookieOptions() {
  return {
    httpOnly: true,
    sameSite: "lax" as const,
    secure: process.env.NODE_ENV === "production",
    path: "/",
    expires: new Date(0),
  };
}
