# Security Master Alias Cleanup

Historical one-time protocol for the retired physical `security_master` alias cleanup workflow.

This protocol is retained only for historical reference. The active entrypoint
`backend/scripts/cleanup_security_master_second_pass_aliases.py` is retired and
no longer performs runtime cleanup.

## Historical scope

Auto-delete only when all were true:
- same `ticker`
- same `isin`
- exactly two rows in the duplicate group
- one row was a clear primary identity
- the other row was a clear secondary venue/consolidated alias
- no current holdings referenced the alias being removed

Anything else stayed in manual review.

## Historical primary keep rules

- keep `.N` when exchange metadata clearly said `New York Stock Exchange`
- keep `.N` when exchange metadata clearly said `NASDAQ Stock Exchange ...` and the competing row was a consolidated Nasdaq alias
- keep `.OQ` when exchange metadata clearly said `Nasdaq ...` and was not a consolidated alias
- keep `.A` when exchange metadata clearly said `American Stock Exchange` and the competing row was an `AMEX Consolidated` alias

## Historical secondary alias rules

These were delete candidates only when paired against a clear primary row:
- `.K` with `New York Consolidated` or `BATS Consolidated`
- `.K` with `AMEX Consolidated`
- `.K` with `Consolidated Issue listed on NASDAQ ...`
- `.P` with `NYSE Arca`
- `.PH` with `PSX`
- `.B` with `Boston`
- `.TH` with `Third Market`
- `.C` with `National SE when trading ...`
- `.DG` with `Direct Edge - EDGX ...`
- base/no suffix with `AMEX Consolidated`

Additional reviewed-safe rules:
- keep `.N` over `.A` when `.N` was `New York Stock Exchange` and `.A` was `American Stock Exchange`
- keep `.N` over `.C` when `.C` was a National/Cincinnati-style routed alias

## Historical script entrypoint

The retired entrypoint used to be:

```bash
./backend/.venv/bin/python -m backend.scripts.cleanup_security_master_second_pass_aliases --json
./backend/.venv/bin/python -m backend.scripts.cleanup_security_master_second_pass_aliases --apply --json
```

## Historical outputs

The old workflow wrote timestamped report directories under:
- `/tmp/ceiora-security-master-backups/second-pass/`

Artifacts included:
- `delete_candidates.csv`
- `manual_review.csv`
- `holdings_alias_hits.json`
- `summary.json`
