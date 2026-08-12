use std::path::PathBuf;

use serde::Serialize;

/// `workspace status` JSON output (`--format json`).
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct WorkspaceStatusOutput {
    pub(crate) schema_version: u32,
    /// Selected settings profile; `None` when running without a profile.
    pub(crate) profile: Option<String>,
    /// Configured workspace name.
    pub(crate) name: String,
    /// Whether `ws_find` located the workspace.
    pub(crate) exists: bool,
    /// Workspace path from `ws_find`; `None` when it does not exist.
    pub(crate) path: Option<PathBuf>,
    /// Absolute expiry time (unix seconds) computed from `ws_list`'s
    /// remaining time; `None` when unavailable.
    pub(crate) expiry_epoch: Option<u64>,
    /// Raw remaining-time string from `ws_list`.
    pub(crate) remaining_display: Option<String>,
    /// Raw expiration-date string from `ws_list` (display fallback when the
    /// remaining time could not be parsed).
    pub(crate) expiry_display: Option<String>,
    /// Extensions still available per `ws_list`.
    pub(crate) extensions_remaining: Option<u32>,
    /// Persisted workspace state file refreshed by this command.
    pub(crate) state_path: PathBuf,
}

/// `workspace allocate` JSON output (`--format json`).
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct WorkspaceAllocateOutput {
    pub(crate) schema_version: u32,
    pub(crate) profile: Option<String>,
    pub(crate) name: String,
    /// True when the workspace already existed and `ws_allocate` was skipped.
    pub(crate) already_allocated: bool,
    /// Days passed to `ws_allocate`; `None` when it already existed.
    pub(crate) duration_days: Option<u32>,
    pub(crate) path: PathBuf,
    pub(crate) expiry_epoch: Option<u64>,
    pub(crate) remaining_display: Option<String>,
    pub(crate) expiry_display: Option<String>,
    pub(crate) extensions_remaining: Option<u32>,
    pub(crate) state_path: PathBuf,
}

/// `workspace extend` JSON output (`--format json`).
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct WorkspaceExtendOutput {
    pub(crate) schema_version: u32,
    pub(crate) profile: Option<String>,
    pub(crate) name: String,
    /// Days passed to `ws_extend`.
    pub(crate) days: u32,
    pub(crate) path: PathBuf,
    pub(crate) expiry_epoch: Option<u64>,
    pub(crate) remaining_display: Option<String>,
    pub(crate) expiry_display: Option<String>,
    pub(crate) extensions_remaining: Option<u32>,
    pub(crate) state_path: PathBuf,
}

/// `workspace release` JSON output (`--format json`).
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct WorkspaceReleaseOutput {
    pub(crate) schema_version: u32,
    pub(crate) profile: Option<String>,
    pub(crate) name: String,
    /// True when `ws_release` ran; false when there was nothing to release.
    pub(crate) released: bool,
    /// Path the workspace had before release; `None` when it did not exist.
    pub(crate) path: Option<PathBuf>,
    pub(crate) state_path: PathBuf,
}
