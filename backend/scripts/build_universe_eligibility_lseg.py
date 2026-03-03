"""Build a PermID-keyed universe eligibility summary from LSEG constituents."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db.universe_schema import (  # noqa: E402
    UNIVERSE_SNAPSHOT_TABLE,
    UNIVERSE_SUMMARY_TABLE,
    clear_universe_tables,
    ensure_universe_tables,
)
from trading_calendar import is_xnys_session, previous_or_same_xnys_session  # noqa: E402

_DB_RAW = Path(os.getenv("DATA_DB_PATH", "data.db")).expanduser()
DEFAULT_DB = _DB_RAW if _DB_RAW.is_absolute() else (Path(__file__).resolve().parent.parent / _DB_RAW)

DEFAULT_CHAIN_RIC = "0#.SPX,0#.MID,0#.RTY,0#.NDX"
DEFAULT_INDEX_RIC = ".SPX,.MID,.RTY,.NDX"
DEFAULT_HISTORICAL_DATE = "2019-03-02"
DEFAULT_SOURCE = "lseg_workspace"

LSEG_BATCH_SIZE = 500
SQLITE_TIMEOUT_SECONDS = 120
SQLITE_BUSY_TIMEOUT_MS = 120000

_MAJOR_US_EXCHANGE_HINTS = ("NASDAQ", "NEW YORK STOCK EXCHANGE", "NYSE")
_MA_REASON_HINTS = (
    "merger",
    "acquisition",
    "acquired",
    "takeover",
    "combination",
)
_RESTRUCTURED_REASON_HINTS = (
    "bankrupt",
    "bankruptcy",
    "chapter 11",
    "liquidat",
    "restructur",
    "insolven",
    "otc",
    "pink",
)


def _load_rd_module():
    import lseg.data as rd

    return rd


def _connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    return conn


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {str(c).lower(): str(c) for c in df.columns}
    for c in candidates:
        got = cols.get(c.lower())
        if got:
            return got
    return None


def _norm_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "nat", "null", "<na>"}:
        return None
    return s


def _norm_date(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return _norm_text(value)
    return str(ts.date())


def _to_bool(value: Any) -> bool:
    if value is None or pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    return s in {"true", "1", "y", "yes"}


def _to_ticker_from_ric(ric: str | None) -> str | None:
    s = _norm_text(ric)
    if not s:
        return None
    return s.split(".", 1)[0].strip().upper() or None


def _split_identifiers(raw: str | None) -> list[str]:
    s = _norm_text(raw)
    if not s:
        return []
    return [x.strip() for x in str(s).replace(";", ",").split(",") if x.strip()]


def _split_paths(raw: str | None) -> list[Path]:
    return [Path(p).expanduser() for p in _split_identifiers(raw)]


def _canonical_identifier(identifier: str) -> str:
    s = (_norm_text(identifier) or "").strip()
    up = s.upper()
    if up == "0#.RTY":
        return "0#.RUT"
    if up == ".RTY":
        return ".RUT"
    if up == "RTY":
        return ".RUT"
    return s


def _to_index_ric(identifier: str) -> str:
    s = _canonical_identifier(identifier)
    if s.startswith("0#") and len(s) > 2:
        return s[2:]
    return s


def _is_russell_identifier(identifier: str) -> bool:
    idx = _to_index_ric(identifier).upper()
    return idx in {".RUT", ".RTY"}


def _load_rics_from_xlsx(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Russell constituents XLSX not found: {path}")
    df = pd.read_excel(path, sheet_name=0, header=None)
    header_row = None
    for i in range(min(len(df), 20)):
        vals = [str(v).strip().upper() for v in df.iloc[i].tolist() if pd.notna(v)]
        if vals and "RIC" in vals:
            header_row = i
            break
    if header_row is None:
        raise RuntimeError(f"Could not locate a 'RIC' header row in {path}")
    data = pd.read_excel(path, sheet_name=0, header=header_row)
    ric_col = _pick_col(data, ["RIC", "Instrument", "Constituent RIC"])
    if not ric_col:
        raise RuntimeError(f"No RIC column found in {path}")
    out: set[str] = set()
    for v in data[ric_col].tolist():
        ric = _norm_text(v)
        if not ric:
            continue
        ric = ric.upper()
        if "." not in ric:
            continue
        out.add(ric)
    return sorted(out)


def _extract_rics(df: pd.DataFrame, *, universe_hint: str | None = None) -> list[str]:
    if df is None or df.empty:
        return []
    ric_col = _pick_col(df, ["Constituent RIC", "RIC", "Instrument"])
    if not ric_col:
        return []
    out: list[str] = []
    hint = _norm_text(universe_hint)
    for v in df[ric_col].tolist():
        ric = _norm_text(v)
        if not ric:
            continue
        if hint and ric.upper() == hint.upper():
            continue
        # Basic RIC sanity: include chained/historical suffix values.
        if "." not in ric:
            continue
        out.append(ric.upper())
    return sorted(set(out))


def _fetch_constituents_via_get_data(rd, universe: str, *, as_of_date: str | None = None) -> tuple[list[str], str] | None:
    params = {"SDate": as_of_date, "EDate": as_of_date} if as_of_date else None

    # Primary pull: include RIC + name + weight so we can detect entitlement-gated payloads
    # where weights are returned but identifiers are blank.
    try:
        combo = rd.get_data(
            universe=universe,
            fields=[
                "TR.IndexConstituentRIC",
                "TR.IndexConstituentName",
                "TR.IndexConstituentWeightPercent",
            ],
            parameters=params,
        )
        rics = _extract_rics(combo, universe_hint=universe)
        if rics:
            return rics, "get_data:index_combo"
        if combo is not None and not combo.empty:
            ric_col = _pick_col(combo, ["Constituent RIC"])
            wt_col = _pick_col(combo, ["Weight percent"])
            if wt_col and len(combo) > 10:
                nonblank_ric = 0
                if ric_col:
                    vals = combo[ric_col].dropna().astype(str).str.strip()
                    nonblank_ric = int(vals.ne("").sum())
                if nonblank_ric == 0:
                    raise RuntimeError(
                        f"Constituent identifiers unavailable for {universe} "
                        f"(rows={len(combo)}; weights present, RIC/name blank)."
                    )
    except Exception as exc:
        if "Constituent identifiers unavailable" in str(exc):
            raise

    attempts: list[tuple[str, str]] = [
        ("TR.IndexConstituentRIC", "get_data:index_constituent"),
        ("TR.RIC", "get_data:ric"),
    ]
    for field, method in attempts:
        try:
            df = rd.get_data(universe=universe, fields=[field], parameters=params)
        except Exception:
            continue
        rics = _extract_rics(df, universe_hint=universe)
        if rics:
            return rics, method
    return None


def fetch_constituents(
    rd,
    *,
    chain_ric: str | None,
    index_ric: str,
    snapshot_date: str | None,
) -> tuple[list[str], str]:
    methods: list[tuple[str, str | None, str | None]] = []
    if chain_ric and snapshot_date is None:
        methods.append(("get_data", chain_ric, None))
        chain_trim = str(chain_ric).strip()
        if chain_trim.startswith("0#") and len(chain_trim) > 2:
            methods.append(("get_data", chain_trim[2:], None))
    methods.append(("get_data", index_ric, snapshot_date))
    # Preserve order, remove duplicates.
    methods = list(dict.fromkeys(methods))

    errors: list[str] = []
    for mode, identifier, as_of in methods:
        if not identifier:
            continue
        try:
            got = _fetch_constituents_via_get_data(rd, identifier, as_of_date=as_of)
            if got and got[0]:
                return got
            errors.append(f"{mode}:{identifier}:empty")
        except Exception as exc:
            errors.append(f"{mode}:{identifier}:{exc}")

    raise RuntimeError(
        "Unable to fetch constituents from requested LSEG identifiers. "
        f"Tried {methods}. Diagnostics={errors}"
    )


def _fetch_in_chunks(rd, universe: list[str], fields: list[str], parameters: dict[str, str] | None) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    def _is_bad_field_error(message: str) -> bool:
        s = message.lower()
        return "unable to resolve all requested fields" in s

    def _is_retryable_identifier_error(message: str) -> bool:
        s = message.lower()
        return (
            "unable to resolve all requested identifiers" in s
            or "unable to collect data for the field" in s
        )

    def _fetch_batch(batch: list[str]) -> None:
        if not batch:
            return
        try:
            part = rd.get_data(universe=batch, fields=fields, parameters=parameters)
            if part is not None and not part.empty:
                frames.append(part)
            return
        except Exception as exc:
            msg = str(exc or "")
            if _is_bad_field_error(msg):
                raise
            if not _is_retryable_identifier_error(msg):
                raise
            if len(batch) <= 1:
                return
        mid = len(batch) // 2
        _fetch_batch(batch[:mid])
        _fetch_batch(batch[mid:])

    for i in range(0, len(universe), LSEG_BATCH_SIZE):
        _fetch_batch(universe[i : i + LSEG_BATCH_SIZE])

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _load_optional_reason_map(rd, universe: list[str]) -> dict[str, str]:
    candidates = [
        "TR.DelistingReason",
        "TR.DelistReason",
        "TR.InstrumentDelistReason",
    ]
    for field in candidates:
        try:
            df = _fetch_in_chunks(rd, universe, [field], parameters=None)
        except Exception:
            continue
        if df.empty:
            continue
        instrument_col = _pick_col(df, ["Instrument"])
        reason_col = _pick_col(df, ["Delisting Reason", "Delist Reason", field])
        if not instrument_col or not reason_col:
            continue
        out: dict[str, str] = {}
        for _, row in df.iterrows():
            k = _norm_text(row.get(instrument_col))
            v = _norm_text(row.get(reason_col))
            if k and v:
                out[str(k).strip().upper()] = v
        if out:
            return out
    return {}


def _fetch_metadata(
    rd,
    universe: list[str],
    *,
    as_of_date: str | None,
    include_last_quote: bool = True,
) -> pd.DataFrame:
    if not universe:
        return pd.DataFrame()

    params = {"SDate": as_of_date, "EDate": as_of_date} if as_of_date else None
    fields = [
        "TR.OrganizationID",
        "TR.RIC",
        "TR.TickerSymbol",
        "TR.CommonName",
        "TR.InstrumentIsActive",
        "TR.ExchangeName",
    ]
    if include_last_quote:
        fields.append("TR.PriceClose.date")
    df = _fetch_in_chunks(rd, universe, fields, parameters=params)
    if df.empty:
        return df

    instrument_col = _pick_col(df, ["Instrument"])
    if not instrument_col:
        return pd.DataFrame()

    org_col = _pick_col(df, ["Organization PermID"])
    ric_col = _pick_col(df, ["RIC"])
    ticker_col = _pick_col(df, ["Ticker Symbol"])
    name_col = _pick_col(df, ["Company Common Name"])
    active_col = _pick_col(df, ["Instrument Is Active Flag"])
    exch_col = _pick_col(df, ["Exchange Name"])
    last_quote_col = _pick_col(df, ["Date"]) if include_last_quote else None

    out = pd.DataFrame()
    out["instrument"] = df[instrument_col].astype(str).str.strip().str.upper()
    out["permid"] = df[org_col] if org_col else None
    out["ric"] = df[ric_col] if ric_col else None
    out["ticker"] = df[ticker_col] if ticker_col else None
    out["common_name"] = df[name_col] if name_col else None
    out["instrument_is_active"] = df[active_col] if active_col else None
    out["exchange_name"] = df[exch_col] if exch_col else None
    out["last_quote_date"] = df[last_quote_col] if last_quote_col else None
    out["instrument_delisted_date"] = None

    out["permid"] = out["permid"].apply(_norm_text)
    out["ric"] = out["ric"].apply(_norm_text)
    out["ticker"] = out["ticker"].apply(_norm_text)
    out["common_name"] = out["common_name"].apply(_norm_text)
    out["exchange_name"] = out["exchange_name"].apply(_norm_text)
    out["last_quote_date"] = out["last_quote_date"].apply(_norm_date)
    out["instrument_delisted_date"] = out["instrument_delisted_date"].apply(_norm_date)
    out["instrument_is_active"] = out["instrument_is_active"].apply(_to_bool)

    out = out.drop_duplicates(subset=["instrument"], keep="last")
    # Optional field: can be sparse/unavailable for many instruments.
    try:
        ddf = _fetch_in_chunks(rd, universe, ["TR.InstrumentDelistedDate"], parameters=params)
    except Exception:
        ddf = pd.DataFrame()
    if not ddf.empty:
        d_inst_col = _pick_col(ddf, ["Instrument"])
        d_col = _pick_col(ddf, ["Instrumented Delist Date", "Instrument Delisted Date"])
        if d_inst_col and d_col:
            dmap = (
                ddf[[d_inst_col, d_col]]
                .rename(columns={d_inst_col: "instrument", d_col: "instrument_delisted_date"})
                .drop_duplicates(subset=["instrument"], keep="last")
            )
            dmap["instrument"] = dmap["instrument"].astype(str).str.strip().str.upper()
            dmap["instrument_delisted_date"] = dmap["instrument_delisted_date"].apply(_norm_date)
            out = out.merge(dmap, on="instrument", how="left", suffixes=("", "_ovr"))
            out["instrument_delisted_date"] = out["instrument_delisted_date_ovr"].combine_first(
                out["instrument_delisted_date"]
            )
            out = out.drop(columns=["instrument_delisted_date_ovr"], errors="ignore")
    return out


def _fetch_historical_permid_only(rd, universe: list[str], *, as_of_date: str | None) -> pd.DataFrame:
    if not universe:
        return pd.DataFrame()
    params = {"SDate": as_of_date, "EDate": as_of_date} if as_of_date else None
    df = _fetch_in_chunks(rd, universe, ["TR.OrganizationID"], parameters=params)
    if df.empty:
        return pd.DataFrame()
    instrument_col = _pick_col(df, ["Instrument"])
    org_col = _pick_col(df, ["Organization PermID"])
    if not instrument_col:
        return pd.DataFrame()
    out = pd.DataFrame()
    out["instrument"] = df[instrument_col].astype(str).str.strip().str.upper()
    out["permid"] = df[org_col] if org_col else None
    out["permid"] = out["permid"].apply(_norm_text)
    # Minimal columns to match _build_snapshot_rows expectations.
    out["ric"] = out["instrument"]
    out["ticker"] = None
    out["common_name"] = None
    out["instrument_is_active"] = None
    out["exchange_name"] = None
    out["last_quote_date"] = None
    out["instrument_delisted_date"] = None
    out = out.drop_duplicates(subset=["instrument"], keep="last")
    return out


def _resolve_permid_fallback(rd, ric: str, *, as_of_date: str | None) -> str | None:
    candidates = [ric]
    base = _to_ticker_from_ric(ric)
    if base:
        candidates.append(base)
    params = {"SDate": as_of_date, "EDate": as_of_date} if as_of_date else None
    try:
        df = rd.get_data(universe=candidates, fields=["TR.OrganizationID"], parameters=params)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    col = _pick_col(df, ["Organization PermID"])
    if not col:
        return None
    for v in df[col].tolist():
        p = _norm_text(v)
        if p:
            return p
    return None


def _classify_state(*, is_active: bool, exchange_name: str | None, delisting_reason: str | None) -> str:
    reason = (delisting_reason or "").strip().lower()
    exchange = (exchange_name or "").strip().upper()
    major_us = any(h in exchange for h in _MAJOR_US_EXCHANGE_HINTS)

    if is_active and major_us:
        return "ACTIVE"
    if not is_active:
        if reason and any(tok in reason for tok in _MA_REASON_HINTS):
            return "DELISTED_M_A"
        if reason and any(tok in reason for tok in _RESTRUCTURED_REASON_HINTS):
            return "DELISTED_RESTRUCTURED"
        if "OTC" in exchange or "PINK" in exchange:
            return "DELISTED_RESTRUCTURED"
    return "INACTIVE_OTHER"


def _is_major_us_exchange(exchange_name: str | None) -> bool:
    exchange = (exchange_name or "").strip().upper()
    return any(h in exchange for h in _MAJOR_US_EXCHANGE_HINTS)


def _build_snapshot_rows(
    *,
    input_rics: list[str],
    metadata: pd.DataFrame,
    snapshot_label: str,
    snapshot_date: str,
    input_identifier: str,
    retrieval_method: str,
    source: str,
    job_run_id: str,
    updated_at: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    input_df = pd.DataFrame({"input_ric": sorted(set(input_rics))})
    input_df["input_ric"] = input_df["input_ric"].astype(str).str.upper()

    meta = metadata.copy() if metadata is not None else pd.DataFrame()
    if not meta.empty:
        meta = meta.rename(columns={"instrument": "input_ric"})
        meta["input_ric"] = meta["input_ric"].astype(str).str.upper()

    merged = input_df.merge(meta, on="input_ric", how="left") if not input_df.empty else meta

    rows: list[dict[str, Any]] = []
    unresolved = 0
    missing_permid = 0
    for _, row in merged.iterrows():
        permid = _norm_text(row.get("permid"))
        if permid is None:
            missing_permid += 1
        if permid is None and _norm_text(row.get("input_ric")):
            unresolved += 1

        rows.append(
            {
                "snapshot_label": snapshot_label,
                "snapshot_date": snapshot_date,
                "input_identifier": input_identifier,
                "retrieval_method": retrieval_method,
                "input_ric": _norm_text(row.get("input_ric")),
                "resolved_ric": _norm_text(row.get("ric")),
                "permid": permid,
                "ticker": _norm_text(row.get("ticker")) or _to_ticker_from_ric(_norm_text(row.get("ric"))),
                "common_name": _norm_text(row.get("common_name")),
                "exchange_name": _norm_text(row.get("exchange_name")),
                "instrument_is_active": 1 if _to_bool(row.get("instrument_is_active")) else 0,
                "source": source,
                "job_run_id": job_run_id,
                "updated_at": updated_at,
            }
        )

    stats = {
        "snapshot_label": snapshot_label,
        "rows": int(len(rows)),
        "missing_permid": int(missing_permid),
        "unresolved": int(unresolved),
    }
    return rows, stats


def _insert_snapshot_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    payload = [
        (
            r["snapshot_label"],
            r["snapshot_date"],
            r["input_identifier"],
            r["retrieval_method"],
            r["input_ric"],
            r["resolved_ric"],
            r["permid"],
            r["ticker"],
            r["common_name"],
            r["exchange_name"],
            r["instrument_is_active"],
            r["source"],
            r["job_run_id"],
            r["updated_at"],
        )
        for r in rows
        if r.get("input_ric")
    ]
    conn.executemany(
        f"""
        INSERT OR REPLACE INTO {UNIVERSE_SNAPSHOT_TABLE}
        (
            snapshot_label, snapshot_date, input_identifier, retrieval_method,
            input_ric, resolved_ric, permid, ticker, common_name, exchange_name,
            instrument_is_active, source, job_run_id, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)


def _insert_summary_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    payload = [
        (
            r["permid"],
            r["current_ric"],
            r["ticker"],
            r["common_name"],
            r["exchange_name"],
            int(r["instrument_is_active"]),
            r["last_quote_date"],
            r["delisting_reason"],
            r["eligibility_state"],
            r["start_date"],
            r["end_date"],
            int(r["in_current_snapshot"]),
            int(r["in_historical_snapshot"]),
            r["current_snapshot_date"],
            r["historical_snapshot_date"],
            int(r["is_trading_day_active"]),
            int(r["is_eligible"]),
            r["source"],
            r["job_run_id"],
            r["updated_at"],
        )
        for r in rows
        if r.get("permid")
    ]
    conn.executemany(
        f"""
        INSERT OR REPLACE INTO {UNIVERSE_SUMMARY_TABLE}
        (
            permid, current_ric, ticker, common_name, exchange_name,
            instrument_is_active, last_quote_date, delisting_reason, eligibility_state,
            start_date, end_date, in_current_snapshot, in_historical_snapshot,
            current_snapshot_date, historical_snapshot_date, is_trading_day_active,
            is_eligible, source, job_run_id, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)


def build_universe_eligibility(
    *,
    db_path: Path,
    current_chain_ric: str,
    historical_index_ric: str,
    historical_date: str,
    current_date: str,
    russell_xlsx: Path | None = None,
    supplemental_historical_xlsx: list[Path] | None = None,
    reset: bool = True,
    output_csv: Path | None = None,
) -> dict[str, Any]:
    rd = _load_rd_module()

    hist_dt = previous_or_same_xnys_session(historical_date)
    cur_dt = previous_or_same_xnys_session(current_date)

    current_identifiers = [_canonical_identifier(x) for x in _split_identifiers(current_chain_ric)]
    historical_identifiers = [_to_index_ric(x) for x in _split_identifiers(historical_index_ric)]
    if not current_identifiers:
        current_identifiers = [current_chain_ric]
    if not historical_identifiers:
        historical_identifiers = [_to_index_ric(historical_index_ric)]

    rd.open_session()
    try:
        current_rics_union: set[str] = set()
        historical_rics_union: set[str] = set()
        current_methods: list[str] = []
        historical_methods: list[str] = []
        fetch_errors: list[str] = []
        russell_override_rics: list[str] = []
        supplemental_hist_rics: set[str] = set()
        if russell_xlsx is not None:
            russell_override_rics = _load_rics_from_xlsx(russell_xlsx)
            print(
                f"Loaded Russell override from {russell_xlsx}: "
                f"{len(russell_override_rics):,} RICs"
            )
        if supplemental_historical_xlsx:
            for p in supplemental_historical_xlsx:
                rics = _load_rics_from_xlsx(p)
                supplemental_hist_rics.update(rics)
                print(f"Loaded supplemental historical constituents from {p}: {len(rics):,} RICs")

        for ident in current_identifiers:
            print(f"Fetching current constituents from {ident}...")
            try:
                if russell_override_rics and _is_russell_identifier(ident):
                    rics = russell_override_rics
                    method = f"xlsx:russell_override:{russell_xlsx.name}"
                else:
                    rics, method = fetch_constituents(
                        rd,
                        chain_ric=ident,
                        index_ric=_to_index_ric(ident),
                        snapshot_date=None,
                    )
                current_rics_union.update(rics)
                current_methods.append(f"{ident}:{method}:{len(rics)}")
                print(f"  {ident}: {len(rics):,} via {method}")
            except Exception as exc:
                msg = f"current:{ident}:{exc}"
                fetch_errors.append(msg)
                print(f"  WARN {msg}")

        for ident in historical_identifiers:
            print(f"Fetching historical constituents from {ident} as of {hist_dt}...")
            try:
                rics, method = fetch_constituents(
                    rd,
                    chain_ric=None,
                    index_ric=ident,
                    snapshot_date=hist_dt,
                )
                historical_rics_union.update(rics)
                historical_methods.append(f"{ident}:{method}:{len(rics)}")
                print(f"  {ident}: {len(rics):,} via {method}")
            except Exception as exc:
                msg = f"historical:{ident}:{exc}"
                fetch_errors.append(msg)
                print(f"  WARN {msg}")

        if supplemental_hist_rics:
            print(f"  supplemental historical additions: {len(supplemental_hist_rics):,}")

        if not current_rics_union:
            raise RuntimeError(f"No current constituents fetched from requested identifiers. errors={fetch_errors}")
        if not historical_rics_union:
            raise RuntimeError(f"No historical constituents fetched from requested identifiers. errors={fetch_errors}")

        current_inputs = sorted(current_rics_union)
        historical_inputs_core = sorted(historical_rics_union)
        historical_inputs_all = sorted(historical_rics_union.union(supplemental_hist_rics))
        print(f"  current universe inputs (indices): {len(current_inputs):,}")
        print(
            f"  historical universe inputs (core indices + supplemental): "
            f"{len(historical_inputs_core):,} + {len(supplemental_hist_rics):,} = {len(historical_inputs_all):,}"
        )

        print("Fetching snapshot metadata...")
        current_meta = _fetch_metadata(rd, current_inputs, as_of_date=cur_dt, include_last_quote=True)
        # Core historical indices resolve at historical date.
        historical_core_meta = _fetch_historical_permid_only(rd, historical_inputs_core, as_of_date=hist_dt)
        historical_meta = historical_core_meta.copy()
        if not historical_meta.empty:
            historical_meta = historical_meta.drop_duplicates(subset=["instrument"], keep="last")

        # Historical fallback: try to recover missing PermIDs with lightweight probes.
        if not historical_meta.empty:
            missing = historical_meta[historical_meta["permid"].isna() | historical_meta["permid"].eq("")]
            if not missing.empty:
                recovered: list[tuple[str, str]] = []
                for ric in missing["instrument"].astype(str).tolist():
                    permid = _resolve_permid_fallback(rd, ric, as_of_date=hist_dt)
                    if permid:
                        recovered.append((ric, permid))
                if recovered:
                    rec_map = {k: v for k, v in recovered}
                    historical_meta.loc[
                        historical_meta["instrument"].isin(rec_map.keys()), "permid"
                    ] = historical_meta["instrument"].map(rec_map)

        # Snapshot rows persist full retrieval diagnostics.
        job_run_id = f"universe_eligibility_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        updated_at = datetime.now(timezone.utc).isoformat()

        cur_rows, cur_stats = _build_snapshot_rows(
            input_rics=current_inputs,
            metadata=current_meta,
            snapshot_label="current",
            snapshot_date=cur_dt,
            input_identifier=",".join(current_identifiers),
            retrieval_method="multi|" + "|".join(current_methods),
            source=DEFAULT_SOURCE,
            job_run_id=job_run_id,
            updated_at=updated_at,
        )
        hist_rows, hist_stats = _build_snapshot_rows(
            input_rics=historical_inputs_all,
            metadata=historical_meta,
            snapshot_label="historical",
            snapshot_date=hist_dt,
            input_identifier=",".join(historical_identifiers),
            retrieval_method="multi|" + "|".join(historical_methods),
            source=DEFAULT_SOURCE,
            job_run_id=job_run_id,
            updated_at=updated_at,
        )
        if supplemental_hist_rics:
            supplemental_set = {str(x).upper() for x in supplemental_hist_rics}
            for row in hist_rows:
                if row.get("permid"):
                    continue
                ric = _norm_text(row.get("input_ric"))
                if ric and ric.upper() in supplemental_set:
                    # Deterministic synthetic key for stale supplemental rows that cannot be
                    # resolved by LSEG historical lookup.
                    row["permid"] = f"RIC::{ric.upper()}"

        cur_df = pd.DataFrame(cur_rows)
        hist_df = pd.DataFrame(hist_rows)

        # Union by PermID.
        permids = sorted(
            {
                p
                for p in pd.concat(
                    [cur_df.get("permid", pd.Series(dtype="object")), hist_df.get("permid", pd.Series(dtype="object"))],
                    ignore_index=True,
                ).tolist()
                if _norm_text(p)
            }
        )
        real_permids = [p for p in permids if not str(p).upper().startswith("RIC::")]
        print(f"Resolving current metadata for {len(real_permids):,} PermIDs...")
        current_meta_by_permid = _fetch_metadata(rd, real_permids, as_of_date=cur_dt, include_last_quote=True)
        current_meta_by_permid["permid"] = current_meta_by_permid["permid"].apply(_norm_text)
        current_meta_by_permid = current_meta_by_permid.dropna(subset=["permid"]).drop_duplicates(
            subset=["permid"], keep="last"
        )

        reason_map = _load_optional_reason_map(rd, real_permids)

        cur_seen = set(cur_df.get("permid", pd.Series(dtype="object")).dropna().astype(str).tolist())
        hist_seen = set(hist_df.get("permid", pd.Series(dtype="object")).dropna().astype(str).tolist())

        cur_lookup = (
            cur_df.dropna(subset=["permid"]).drop_duplicates(subset=["permid"], keep="last").set_index("permid")
            if not cur_df.empty
            else pd.DataFrame().set_index(pd.Index([], name="permid"))
        )
        hist_lookup = (
            hist_df.dropna(subset=["permid"]).drop_duplicates(subset=["permid"], keep="last").set_index("permid")
            if not hist_df.empty
            else pd.DataFrame().set_index(pd.Index([], name="permid"))
        )
        current_lookup = (
            current_meta_by_permid.set_index("permid")
            if not current_meta_by_permid.empty
            else pd.DataFrame().set_index(pd.Index([], name="permid"))
        )

        trade_day_ok = is_xnys_session(cur_dt)
        summary_rows: list[dict[str, Any]] = []
        for permid in permids:
            cur_row = current_lookup.loc[permid] if permid in current_lookup.index else None
            cur_snap = cur_lookup.loc[permid] if permid in cur_lookup.index else None
            hist_snap = hist_lookup.loc[permid] if permid in hist_lookup.index else None

            current_ric = _norm_text(cur_row.get("ric") if cur_row is not None else None)
            ticker = _norm_text(cur_snap.get("ticker") if cur_snap is not None else None)
            if not ticker:
                ticker = _norm_text(hist_snap.get("ticker") if hist_snap is not None else None)
            if not ticker:
                ticker = _norm_text(cur_row.get("ticker") if cur_row is not None else None)
            if not ticker:
                ticker = _to_ticker_from_ric(current_ric)

            common_name = _norm_text(cur_row.get("common_name") if cur_row is not None else None)
            if not common_name:
                common_name = _norm_text(cur_snap.get("common_name") if cur_snap is not None else None)
            if not common_name:
                common_name = _norm_text(hist_snap.get("common_name") if hist_snap is not None else None)

            exchange_name = _norm_text(cur_row.get("exchange_name") if cur_row is not None else None)
            is_active = _to_bool(cur_row.get("instrument_is_active") if cur_row is not None else False)
            last_quote_date = _norm_date(cur_row.get("last_quote_date") if cur_row is not None else None)
            delisted_date = _norm_date(cur_row.get("instrument_delisted_date") if cur_row is not None else None)
            delisting_reason = _norm_text(reason_map.get(str(permid).upper()))

            state = _classify_state(
                is_active=is_active,
                exchange_name=exchange_name,
                delisting_reason=delisting_reason,
            )

            start_date = hist_dt if permid in hist_seen else cur_dt
            if is_active:
                end_date = "9999-12-31"
            else:
                end_date = last_quote_date or delisted_date or cur_dt

            in_current = int(permid in cur_seen)
            in_historical = int(permid in hist_seen)
            is_trading_day_active = int(trade_day_ok and state == "ACTIVE")
            is_eligible = int(bool(is_trading_day_active and in_current == 1 and _is_major_us_exchange(exchange_name)))

            summary_rows.append(
                {
                    "permid": permid,
                    "current_ric": current_ric,
                    "ticker": ticker,
                    "common_name": common_name,
                    "exchange_name": exchange_name,
                    "instrument_is_active": int(is_active),
                    "last_quote_date": last_quote_date,
                    "delisting_reason": delisting_reason,
                    "eligibility_state": state,
                    "start_date": start_date,
                    "end_date": end_date,
                    "in_current_snapshot": in_current,
                    "in_historical_snapshot": in_historical,
                    "current_snapshot_date": cur_dt if in_current else None,
                    "historical_snapshot_date": hist_dt if in_historical else None,
                    "is_trading_day_active": is_trading_day_active,
                    "is_eligible": is_eligible,
                    "source": DEFAULT_SOURCE,
                    "job_run_id": job_run_id,
                    "updated_at": updated_at,
                }
            )

    finally:
        rd.close_session()

    conn = _connect_db(db_path)
    try:
        ensure_universe_tables(conn)
        if reset:
            clear_universe_tables(conn)
        n_snap = _insert_snapshot_rows(conn, cur_rows + hist_rows)
        n_summary = _insert_summary_rows(conn, summary_rows)
        conn.commit()
    finally:
        conn.close()

    out_df = pd.DataFrame(summary_rows)
    if output_csv is not None:
        out_df.sort_values(["eligibility_state", "ticker", "permid"]).to_csv(output_csv, index=False)

    state_counts = out_df["eligibility_state"].value_counts(dropna=False).to_dict() if not out_df.empty else {}
    out = {
        "status": "ok",
        "db_path": str(db_path),
        "summary_table": UNIVERSE_SUMMARY_TABLE,
        "snapshot_table": UNIVERSE_SNAPSHOT_TABLE,
        "rows_summary": int(n_summary),
        "rows_snapshots": int(n_snap),
        "current_snapshot_stats": cur_stats,
        "historical_snapshot_stats": hist_stats,
        "state_counts": {str(k): int(v) for k, v in state_counts.items()},
        "historical_date": hist_dt,
        "current_date": cur_dt,
        "current_chain_ric": ",".join(current_identifiers),
        "historical_index_ric": ",".join(historical_identifiers),
        "fetch_errors": fetch_errors,
        "russell_xlsx_override": str(russell_xlsx) if russell_xlsx is not None else None,
        "supplemental_historical_xlsx": [str(p) for p in (supplemental_historical_xlsx or [])],
        "supplemental_historical_count": int(len(supplemental_hist_rics)),
        "csv": str(output_csv) if output_csv else None,
    }
    print(out)
    return out


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build universe_eligibility_summary from LSEG snapshots.")
    p.add_argument("--db-path", default=str(DEFAULT_DB), help="Path to target SQLite DB")
    p.add_argument(
        "--current-chain-ric",
        default=DEFAULT_CHAIN_RIC,
        help="Current constituent chain RIC(s), comma-separated",
    )
    p.add_argument(
        "--historical-index-ric",
        default=DEFAULT_INDEX_RIC,
        help="Historical index RIC(s), comma-separated (0# prefixes also accepted)",
    )
    p.add_argument("--historical-date", default=DEFAULT_HISTORICAL_DATE, help="Historical snapshot date (YYYY-MM-DD)")
    p.add_argument("--current-date", default=str(date.today()), help="Current snapshot date (YYYY-MM-DD)")
    p.add_argument("--russell-xlsx", default=None, help="Optional XLSX path to use as Russell 2000 constituent source")
    p.add_argument(
        "--supplemental-historical-xlsx",
        default=None,
        help="Optional comma-separated XLSX paths with additional historical constituents",
    )
    p.add_argument("--no-reset", action="store_true", help="Append/update without clearing existing universe tables")
    p.add_argument("--output-csv", default=None, help="Optional CSV path for summary output")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    csv_path = Path(args.output_csv).expanduser() if args.output_csv else None
    russell_xlsx = Path(args.russell_xlsx).expanduser() if args.russell_xlsx else None
    supplemental_hist = _split_paths(args.supplemental_historical_xlsx) if args.supplemental_historical_xlsx else None
    build_universe_eligibility(
        db_path=Path(args.db_path).expanduser(),
        current_chain_ric=str(args.current_chain_ric),
        historical_index_ric=str(args.historical_index_ric),
        historical_date=str(args.historical_date),
        current_date=str(args.current_date),
        russell_xlsx=russell_xlsx,
        supplemental_historical_xlsx=supplemental_hist,
        reset=not bool(args.no_reset),
        output_csv=csv_path,
    )
