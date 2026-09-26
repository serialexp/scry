# Changelog

## 0.24.1 (2026-09-26)

### Bug Fixes

- close object-store consistency gaps

## 0.23.4 (2026-09-18)

### Bug Fixes

- track meta.json sizes in catalog for correct admission budgets

## 0.23.1 (2026-09-15)

### Bug Fixes

- skip duplicate metric descriptors during WAL replay

## 0.23.0 (2026-09-15)

### Features

- establish the logs v2 reader contract
- add negotiated logs v2 ingestion
- add durable occurrence foundation

### Bug Fixes

- make WAL recovery bounded and retry-safe

## 0.22.0 (2026-09-05)

### Features

- bound compactor memory use
- preserve structured metric points
- expose structured points in SQL
- stream structured points over live tail

### Bug Fixes

- harden bounded resource handling
- complete resource envelope hardening

