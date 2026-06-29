//! Shared diagnostic findings model used by both [`crate::preflight`] and
//! [`crate::cluster`].
//!
//! These types are the common vocabulary for "a list of severity-tagged
//! findings with optional remediation". Hosting them here (rather than in
//! `preflight`) lets `cluster` build reports without depending on `preflight`,
//! breaking the former `cluster` <-> `preflight` import cycle. `preflight`
//! re-exports them so existing `preflight::{Item, Level, Report}` paths keep
//! working.

use serde::Serialize;

/// Severity level for one diagnostic finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    /// The check passed.
    Ok,
    /// The check found a non-fatal issue worth surfacing.
    Warn,
    /// The check found a blocking issue.
    Error,
}

/// One diagnostic finding.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct Item {
    pub level: Level,
    pub message: String,
    pub remediation: Option<String>,
}

/// A flat diagnostic report before items are grouped for display.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub items: Vec<Item>,
}
