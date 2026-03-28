const BASE = "";
const REQUEST_TIMEOUT_MS = 30000;

export class ApiError extends Error {
  status: number;
  url: string;
  detail: unknown;

  constructor(status: number, url: string, detail: unknown) {
    const message =
      typeof detail === "string"
        ? detail
        : (detail as { message?: string } | null)?.message || `Request failed (${status}) for ${url}`;
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.url = url;
    this.detail = detail;
  }
}

async function parseErrorDetail(res: Response): Promise<unknown> {
  const text = await res.text();
  if (!text) return null;
  try {
    const payload = JSON.parse(text) as { detail?: unknown };
    return payload?.detail ?? payload;
  } catch {
    return text;
  }
}

export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  const url = `${BASE}${path}`;
  try {
    const res = await fetch(url, { ...init, signal: controller.signal });
    if (!res.ok) {
      const detail = await parseErrorDetail(res);
      throw new ApiError(res.status, url, detail);
    }
    return res.json();
  } finally {
    clearTimeout(timer);
  }
}
