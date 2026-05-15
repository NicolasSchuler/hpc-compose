# Migration to Spec v2

This page is reserved for the first breaking hpc-compose spec release. Current hpc-compose builds support spec version `1`; use `version: "1"` or omit the field for v1 specs.

Known v2 migration hint:

- `steps` was renamed to `services` in v2. Rename top-level `steps:` to `services:` before validating with a v2-aware hpc-compose build.
