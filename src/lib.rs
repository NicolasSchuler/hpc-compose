//! Contributor-facing library surface for `hpc-compose`.
//!
//! The binary entrypoint in `main.rs` composes this crate's modules into a
//! pipeline:
//!
//! 1. [`spec`] parses and validates the Compose-like input file.
//! 2. [`planner`] normalizes that spec into a concrete runtime plan.
//! 3. [`preflight`] checks the login-node environment.
//! 4. [`prepare`] imports or rebuilds runtime artifacts when needed.
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
pub(crate) mod cluster;
/// CLI command orchestration used by the binary entrypoint.
pub mod commands;
pub mod context;
pub(crate) mod diagnostics;
pub(crate) mod domain;
pub mod evolve;
pub mod examples;
/// Process exit-code propagation for direct-execution commands.
pub mod exit;
pub mod init;
pub mod job;
pub(crate) mod lint;
pub(crate) mod lint_fix;
pub mod manpages;
pub(crate) mod mpi_util;
pub(crate) mod output;
pub(crate) mod path_util;
pub mod planner;
pub(crate) mod platform;
pub(crate) mod preflight;
pub mod prepare;
pub(crate) mod progress;
pub(crate) mod readiness_util;
pub(crate) mod redaction;
pub mod render;
pub mod rendezvous;
pub(crate) mod schema;
pub(crate) mod secure_io;
pub(crate) mod shell_quote;
pub mod spec;
pub(crate) mod spec_error;
pub(crate) mod suggest;
pub(crate) mod term;
pub(crate) mod terminal_state;
pub(crate) mod time_util;
pub(crate) mod tracked_paths;
pub(crate) mod watch_ui;
pub(crate) mod weather;
pub(crate) mod when;

/// Converts a CLI failure into a rendered diagnostic report while preserving
/// structured spec diagnostics when they are present inside an `anyhow::Error`.
pub fn cli_error_report(error: anyhow::Error) -> miette::Report {
    spec_error::cli_error_report(error)
}

/// Test-only shared synchronization primitives.
///
/// Declared last so it does not trip `clippy::items-after-test-module`.
#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::{Mutex, OnceLock};

    /// Process-wide lock serializing tests that mutate global process state
    /// (environment variables, `set_current_dir`, etc.).
    ///
    /// All in-crate unit tests share this single mutex so that env mutation is
    /// serialized across modules within the one lib-test binary — per-module
    /// mutexes cannot do this because each guards a distinct critical section.
    /// Call sites acquire with `env_lock().lock().expect(..)`, matching the
    /// prior per-module pattern (a poisoned lock surfaces as a test panic).
    pub(crate) fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }
}
