//! Host-platform awareness helpers.
//!
//! `hpc-compose` supports authoring (`new`/`plan`/`validate`/`render`) on
//! developer machines including macOS, while real submission requires a Linux
//! Slurm host. [`is_macos`] lets commands tailor guidance (e.g. explain that
//! missing Slurm tooling is *expected* on macOS rather than a misconfiguration)
//! instead of emitting raw "install Slurm" failures on the authoring platform.

/// Returns `true` when the current host is macOS, which `hpc-compose` supports
/// for authoring only (not for runtime submission).
#[must_use]
pub fn is_macos() -> bool {
    cfg!(target_os = "macos")
}
