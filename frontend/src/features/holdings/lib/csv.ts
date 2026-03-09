"use client";

export function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === "\"") {
      if (inQuotes && i + 1 < line.length && line[i + 1] === "\"") {
        current += "\"";
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === "," && !inQuotes) {
      out.push(current.trim());
      current = "";
      continue;
    }
    current += ch;
  }
  out.push(current.trim());
  return out;
}

function normalizeHeader(raw: string): string {
  return raw.trim().toLowerCase().replace(/[\s\-]+/g, "_");
}

function pickField(cells: string[], idx: Record<string, number>, keys: string[]): string {
  for (const key of keys) {
    const i = idx[key];
    if (typeof i === "number" && i >= 0 && i < cells.length) {
      return String(cells[i] ?? "").trim();
    }
  }
  return "";
}

export function parseHoldingsCsv(text: string, defaultSource: string) {
  const lines = text
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  if (lines.length < 2) {
    throw new Error("CSV must include a header row and at least one data row.");
  }
  const headers = parseCsvLine(lines[0]).map(normalizeHeader);
  const idx: Record<string, number> = {};
  headers.forEach((h, i) => {
    idx[h] = i;
  });

  const rows: Array<{ account_id?: string; ric?: string; ticker?: string; quantity: number; source?: string }> = [];
  const rejected: string[] = [];

  for (let r = 1; r < lines.length; r += 1) {
    const cells = parseCsvLine(lines[r]);
    const ric = pickField(cells, idx, ["ric", "ric_code", "lseg_ric"]).toUpperCase();
    const ticker = pickField(cells, idx, ["ticker", "symbol", "security"]).toUpperCase();
    const qtyRaw = pickField(cells, idx, ["quantity", "qty", "shares", "position", "position_qty"]).replaceAll(",", "");
    const source = pickField(cells, idx, ["source", "origin"]) || defaultSource;
    const accountId = pickField(cells, idx, ["account_id", "account", "accountid", "acct"]);

    const qty = Number.parseFloat(qtyRaw);
    if (!Number.isFinite(qty)) {
      rejected.push(`line ${r + 1}: invalid quantity "${qtyRaw}"`);
      continue;
    }
    if (!ric && !ticker) {
      rejected.push(`line ${r + 1}: missing ticker/ric`);
      continue;
    }
    rows.push({
      account_id: accountId || undefined,
      ric: ric || undefined,
      ticker: ticker || undefined,
      quantity: qty,
      source,
    });
  }

  if (rows.length === 0) {
    throw new Error("CSV rows were parsed, but none were usable.");
  }
  return { rows, rejected };
}

export function fmtQty(n: number): string {
  const abs = Math.abs(n);
  const digits = abs >= 1000 ? 0 : abs >= 100 ? 1 : abs >= 10 ? 2 : 4;
  return n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: digits });
}
