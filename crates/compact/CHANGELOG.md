# Changelog

## 0.24.1 (2026-09-26)

### Bug Fixes

- close object-store consistency gaps
- read the merged metrics block on one partition

## 0.23.4 (2026-09-18)

### Bug Fixes

- remove input meta budget — it was never in the estimate
- track meta.json sizes in catalog for correct admission budgets

## 0.23.3 (2026-09-16)

### Bug Fixes

- widen input meta budget and surface compaction failures

## 0.23.0 (2026-09-15)

### Features

- require conditional S3 semantics
- establish the logs v2 reader contract
- add durable occurrence foundation

## 0.22.0 (2026-09-05)

### Features

- bound compactor memory use
- preserve structured metric points

### Bug Fixes

- harden bounded resource handling
- complete resource envelope hardening

