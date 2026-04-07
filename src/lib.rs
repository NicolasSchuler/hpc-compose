//! Contributor-facing library surface for `hpc-compose`.
//!
//! The binary entrypoint in `main.rs` composes this crate's modules into a
//! pipeline:
//!
//! 1. [`spec`] parses and validates the Compose-like input file.
//! 2. [`planner`] normalizes that spec into a concrete runtime plan.
//! 3. [`preflight`] checks the login-node environment.
//! 4. [`prepare`] imports or rebuilds Enroot artifacts when needed.
//! 5. [`render`] generates the batch script executed by Slurm.
//! 6. [`job`] tracks submitted jobs, logs, metrics, and exported artifacts.
//! 7. [`cache`] records reusable image artifacts.
//! 8. [`init`] exposes the shipped starter templates.
//!
//! This crate is primarily an implementation detail of the CLI. Public items
//! are exposed so the binary and integration tests can share the same logic;
//! they are documented for contributors, not as a semver-stable general
//! purpose API.
#![warn(missing_docs)]

pub mod cache;
pub mod cli;
pub mod init;
pub mod job;
pub mod manpages;
pub mod planner;
pub mod preflight;
pub mod prepare;
pub mod render;
pub mod spec;
