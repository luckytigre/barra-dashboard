export const OPERATOR_TOKEN_STORAGE_KEY = "ceiora.operator-token";
export const EDITOR_TOKEN_STORAGE_KEY = "ceiora.editor-token";

export interface StoredAuthTokens {
  operatorToken: string;
  editorToken: string;
}

function cleanToken(value: string | null | undefined): string {
  return String(value || "").trim();
}

export function readStoredAuthTokens(storage?: Pick<Storage, "getItem"> | null): StoredAuthTokens {
  const source = storage ?? (typeof window !== "undefined" ? window.localStorage : null);
  if (!source) {
    return { operatorToken: "", editorToken: "" };
  }
  return {
    operatorToken: cleanToken(source.getItem(OPERATOR_TOKEN_STORAGE_KEY)),
    editorToken: cleanToken(source.getItem(EDITOR_TOKEN_STORAGE_KEY)),
  };
}

export function writeStoredAuthToken(key: typeof OPERATOR_TOKEN_STORAGE_KEY | typeof EDITOR_TOKEN_STORAGE_KEY, value: string): void {
  if (typeof window === "undefined") return;
  const clean = cleanToken(value);
  if (!clean) {
    window.localStorage.removeItem(key);
    return;
  }
  window.localStorage.setItem(key, clean);
}

export function clearStoredAuthTokens(): void {
  if (typeof window === "undefined") return;
  window.localStorage.removeItem(OPERATOR_TOKEN_STORAGE_KEY);
  window.localStorage.removeItem(EDITOR_TOKEN_STORAGE_KEY);
}
