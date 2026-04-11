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
//!
//! ```no_run
//! use std::path::Path;
//!
//! use hpc_compose::planner::build_plan;
//! use hpc_compose::prepare::build_runtime_plan;
//! use hpc_compose::render::render_script;
//! use hpc_compose::spec::ComposeSpec;
//!
//! let compose_path = Path::new("compose.yaml");
//! let spec = ComposeSpec::load(compose_path)?;
//! let plan = build_plan(compose_path, spec)?;
//! let runtime_plan = build_runtime_plan(&plan);
//! let script = render_script(&runtime_plan)?;
//! assert!(script.contains("#SBATCH"));
//! # Ok::<(), anyhow::Error>(())
//! ```
#![warn(missing_docs)]

extern crate self as hpc_compose;

pub mod cache;
pub mod cli;
/// CLI command orchestration used by the binary entrypoint.
pub mod commands;
pub mod context;
pub mod init;
pub mod job;
pub mod manpages;
pub(crate) mod output;
pub(crate) mod path_util;
pub mod planner;
pub mod preflight;
pub mod prepare;
pub(crate) mod readiness_util;
pub mod render;
pub mod schema;
pub mod spec;
pub(crate) mod tracked_paths;
pub(crate) mod watch_ui;
