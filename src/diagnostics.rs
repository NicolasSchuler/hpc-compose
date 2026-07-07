//! Shared diagnostic findings model used by both [`crate::preflight`] and
//! [`crate::cluster`].
//!
//! These types are the common vocabulary for "a list of severity-tagged
//! findings with optional remediation". Hosting them here (rather than in
//! `preflight`) lets `cluster` build reports without depending on `preflight`,
//! breaking the former `cluster` <-> `preflight` import cycle. `preflight`
//! re-exports them so existing `preflight::{Item, Level, Report}` paths keep
//! working.

use std::cell::Cell;
use std::io::{self, Write};

use serde::Serialize;
use tracing_subscriber::EnvFilter;

/// Severity level for one diagnostic finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, schemars::JsonSchema)]
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
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct Item {
    pub level: Level,
    pub message: String,
    pub remediation: Option<String>,
}

/// A flat diagnostic report before items are grouped for display.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct Report {
    pub items: Vec<Item>,
}

/// How user-facing notices should be written to stderr.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeFormat {
    /// Human-readable text such as `warning: ...`.
    Text,
    /// One JSON object per line for commands whose stdout is machine-readable.
    Json,
}

thread_local! {
    static NOTICE_FORMAT: Cell<NoticeFormat> = const { Cell::new(NoticeFormat::Text) };
}

/// One user-facing notice emitted on stderr.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct Notice {
    pub schema_version: u32,
    pub level: &'static str,
    pub code: Option<&'static str>,
    pub message: String,
    pub remediation: Option<String>,
}

impl Notice {
    /// Output contract version for JSON-line diagnostic notices.
    pub const SCHEMA_VERSION: u32 = 1;

    /// Builds a warning notice.
    #[must_use]
    pub fn warning(message: impl Into<String>) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            level: "warning",
            code: None,
            message: message.into(),
            remediation: None,
        }
    }

    /// Builds an informational notice.
    #[must_use]
    pub fn informational(message: impl Into<String>) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            level: "notice",
            code: None,
            message: message.into(),
            remediation: None,
        }
    }

    /// Adds a stable code to the notice.
    #[must_use]
    pub fn with_code(mut self, code: &'static str) -> Self {
        self.code = Some(code);
        self
    }
}

/// Initializes tracing. `RUST_LOG` wins; otherwise `--debug` and repeatable
/// `--verbose` choose a conservative default filter.
pub fn init_logging(verbose: u8, debug: bool) {
    let default_filter = if debug || verbose > 1 {
        "hpc_compose=debug"
    } else if verbose > 0 {
        "hpc_compose=info"
    } else {
        "warn"
    };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .without_time()
        .try_init();
}

/// Sets how subsequent user-facing notices are emitted by this thread.
pub fn set_notice_format(format: NoticeFormat) {
    NOTICE_FORMAT.with(|cell| cell.set(format));
}

/// Emits a warning notice to stderr using the active notice format.
pub fn warn(message: impl Into<String>) {
    emit(Notice::warning(message));
}

/// Emits a warning notice with a stable machine-readable code.
pub fn warn_with_code(code: &'static str, message: impl Into<String>) {
    emit(Notice::warning(message).with_code(code));
}

/// Emits an informational notice to stderr using the active notice format.
pub fn notice(message: impl Into<String>) {
    emit(Notice::informational(message));
}

/// Emits a full notice to stderr using the active notice format.
pub fn emit(notice: Notice) {
    NOTICE_FORMAT.with(|cell| match cell.get() {
        NoticeFormat::Text => {
            let _ = writeln!(io::stderr(), "{}: {}", notice.level, notice.message);
            if let Some(remediation) = &notice.remediation {
                let _ = writeln!(io::stderr(), "  help: {remediation}");
            }
        }
        NoticeFormat::Json => match serde_json::to_string(&notice) {
            Ok(line) => {
                let _ = writeln!(io::stderr(), "{line}");
            }
            Err(_) => {
                let _ = writeln!(io::stderr(), "{}: {}", notice.level, notice.message);
            }
        },
    });
}
