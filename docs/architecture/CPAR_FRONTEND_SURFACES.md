# cPAR Frontend Surfaces

Date: 2026-03-18
Status: Active slice-5 frontend notes
Owner: Codex

This document describes the first cPAR frontend slice.

Related cPAR docs:
- [CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md)
- [CPAR_OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/operations/CPAR_OPERATIONS_PLAYBOOK.md)

## Purpose

This slice adds the first UI for the read-only cPAR backend surfaces.

It does not add:
- a standalone `/cpar/hedge` page
- portfolio or holdings overlays
- cUSE4 vs cPAR comparison views
- route-triggered build behavior
- any shared cUSE4/cPAR truth layer

## Page Structure

`/cpar`
- lightweight landing page
- shows the active cPAR package summary
- shows the fixed cPAR1 factor registry summary
- shows the warning/status legend
- provides the search entry point into explore

`/cpar/explore`
- primary v1 cPAR page
- owns search, ticker selection, detail rendering, hedge mode toggle, hedge preview, and post-hedge display
- uses the active package only

## Backend Contracts Used By The Frontend

`GET /api/cpar/meta`
- package metadata plus ordered factor registry

`GET /api/cpar/search`
- active-package search hits only

`GET /api/cpar/ticker/{ticker}`
- one active-package persisted fit row
- returns `409` when ticker is ambiguous and `ric` is required

`GET /api/cpar/ticker/{ticker}/hedge`
- persisted hedge preview only
- no request-time refit

## Status And Warning Rendering

Fit status:
- `ok`: green success badge
- `limited_history`: warning badge, but loadings and hedge stay visible
- `insufficient_history`: error badge, identity stays visible, loadings and hedge are blocked

Warnings:
- `continuity_gap`: non-blocking warning badge
- `ex_us_caution`: non-blocking warning badge

Read failures:
- cPAR-specific `503 not_ready` is rendered as a package-not-ready state
- cPAR-specific `503 unavailable` is rendered as an authority-unavailable state
- ticker ambiguity is rendered as a UI instruction to choose a specific RIC from search results
- search hits without a ticker render as non-navigable rows because the current detail route is ticker-keyed
- a direct `/cpar/explore?ric=...` visit without `ticker=` must render an explanatory warning rather than silently failing or synthesizing a detail request

## Deferred After This Slice

- dedicated `/cpar/hedge`
- frontend operator surfaces
- cPAR portfolio integration
- cPAR what-if integration
- any shared cUSE4/cPAR comparison UI

## Shared App Chrome

The global brand and background menu remain shared with the rest of the app.

The cUSE4 operator-status signal and `serve-refresh` control are intentionally suppressed on `/cpar*` routes so the first cPAR slice does not imply operator coupling that has not been implemented.
