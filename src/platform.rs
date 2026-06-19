//! Host-platform awareness helpers.
//!
//! `hpc-compose` supports authoring (`new`/`plan`/`validate`/`render`) on
//! developer machines including macOS, while real submission requires a Linux
//! Slurm host. These helpers let commands tailor guidance (e.g. explain that
//! missing Slurm tooling is *expected* on macOS rather than a misconfiguration)
//! and stamp job records with the cluster they were submitted against, so state
//! created on one cluster is not silently acted upon from another.

use std::path::Path;
use std::process::Command;

/// Returns `true` when the current host is macOS, which `hpc-compose` supports
/// for authoring only (not for runtime submission).
#[must_use]
pub fn is_macos() -> bool {
    cfg!(target_os = "macos")
}

/// Best-effort identifier for the Slurm cluster the current host submits to.
///
/// Resolution order: the `SLURM_CLUSTER_NAME` environment variable, then
/// `scontrol show config` (when a `scontrol` path is provided and runnable),
/// then the system hostname. Returns `None` only when none can be determined.
/// This is intentionally cheap and tolerant: it is used to *warn* about
/// cross-cluster state reuse, never to block on a perfect match.
#[must_use]
pub fn cluster_identity(scontrol: Option<&Path>) -> Option<String> {
    if let Ok(name) = std::env::var("SLURM_CLUSTER_NAME") {
        let name = name.trim();
        if !name.is_empty() {
            return Some(name.to_string());
        }
    }
    if let Some(scontrol) = scontrol
        && let Some(name) = scontrol_cluster_name(scontrol)
    {
        return Some(name);
    }
    hostname()
}

fn scontrol_cluster_name(scontrol: &Path) -> Option<String> {
    let output = Command::new(scontrol)
        .arg("show")
        .arg("config")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        // scontrol prints "ClusterName             = mycluster".
        if line.trim_start().starts_with("ClusterName")
            && let Some((_, value)) = line.split_once('=')
        {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

fn hostname() -> Option<String> {
    if let Ok(h) = std::env::var("HOSTNAME") {
        let h = h.trim();
        if !h.is_empty() {
            return Some(h.to_string());
        }
    }
    let output = Command::new("hostname").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let name = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if name.is_empty() { None } else { Some(name) }
}
