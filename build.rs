//! Best-effort build metadata so `up --remote` can warn when it would delegate
//! an *unreleased* local build to a login node running the published release of
//! the same version (the version-collision trap: both print `--version` X.Y.Z,
//! but the remote lacks the local tree's unreleased spec fields/flags).
//!
//! Emitted as compile-time env vars consumed via `option_env!`:
//!   - `HPC_COMPOSE_BUILD_DIRTY` = "1" when the tracked working tree has
//!     uncommitted changes at build time, else "0".
//!   - `HPC_COMPOSE_BUILD_REV`   = short git rev, or "" when git is unavailable.
//!
//! Everything is best-effort: a release built from a source tarball with no
//! `.git` simply reports clean (`"0"`/`""`), so released users never see the
//! warning. No `rerun-if-changed` is emitted on purpose — that keeps Cargo's
//! default "re-run when any package file changes" behavior, so the dirty flag
//! stays roughly fresh as sources are edited.

use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn main() {
    let rev = git(&["rev-parse", "--short", "HEAD"]).unwrap_or_default();
    // `--untracked-files=no`: only tracked modifications/staged changes count as
    // "unreleased", so a stray scratch file does not trip the warning.
    let dirty = git(&["status", "--porcelain", "--untracked-files=no"])
        .map(|s| !s.is_empty())
        .unwrap_or(false);

    println!("cargo:rustc-env=HPC_COMPOSE_BUILD_REV={rev}");
    println!(
        "cargo:rustc-env=HPC_COMPOSE_BUILD_DIRTY={}",
        if dirty { "1" } else { "0" }
    );
}
