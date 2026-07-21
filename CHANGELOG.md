# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

Rust crate versions (`goosefs-sdk`) and Python package versions (`goosefs`) are
kept aligned. Python-specific notes also appear in
[`bindings/python/CHANGELOG.md`](bindings/python/CHANGELOG.md).

## [Unreleased]

## [0.1.8] — 2026-07-21

### Changed

- Default `worker_connection_pool_size` bumped from `1` to `min(cores, 4)`
  (capped via `available_parallelism`); restore legacy behaviour with
  `.with_worker_connection_pool_size(1)` or
  `goosefs.client.worker.connection.pool.size=1`.
- Open-source scrub: public contribution docs, scrubbed internal paths / registry
  instructions, and Docker fixture image override via `GOOSEFS_IMAGE`.
- Version bump: `goosefs-sdk` / `goosefs` `0.1.7` → `0.1.8`.
