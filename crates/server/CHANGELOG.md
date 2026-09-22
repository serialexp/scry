# Changelog

## 0.24.0 (2026-09-22)

### Features

- add issue details and memory pressure recovery
- add durable scalar evaluation slice

## 0.23.4 (2026-09-18)

### Bug Fixes

- track meta.json sizes in catalog for correct admission budgets

## 0.23.3 (2026-09-16)

### Bug Fixes

- widen input meta budget and surface compaction failures

## 0.23.0 (2026-09-15)

### Features

- establish the logs v2 reader contract
- add negotiated logs v2 ingestion
- add durable occurrence foundation
- serve issue list over the query wire protocol

### Bug Fixes

- make WAL recovery bounded and retry-safe

## 0.22.0 (2026-09-05)

### Features

- bound compactor memory use
- preserve structured metric points
- stream structured points over live tail

### Bug Fixes

- harden bounded resource handling
- complete resource envelope hardening
- keep control-plane requests available

