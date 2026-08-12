//! Shared diagnostic findings model used by both [`crate::preflight`] and
//! [`crate::cluster`].
//!
//! These types are the common vocabulary for "a list of severity-tagged
//! findings with optional remediation". Hosting them here (rather than in
//! `preflight`) lets `cluster` build reports without depending on `preflight`,
//! breaking the former `cluster` <-> `preflight` import cycle. Grouping and
//! contextual-warning classification live with the model, while a private
//! presentation child owns terminal rendering and notice emission. `preflight`
//! re-exports the report types so existing paths keep working.

mod presentation;

use serde::Serialize;

pub(crate) const CONTEXTUAL_TASK_PROLOG_PREFIX: &str =
    "neither /etc/slurm/task_prolog.hk nor /etc/slurm/task_prolog exists";
pub(crate) const CONTEXTUAL_PYXIS_HELPER_PREFIX: &str = "site Pyxis helper path is";
pub(crate) const CONTEXTUAL_METRICS_COLLECTOR_PREFIX: &str = "metrics collector";

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

/// Count summary for a grouped preflight report.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct ReportSummary {
    pub blockers: usize,
    pub actionable_warnings: usize,
    pub contextual_warnings: usize,
    pub passed_checks: usize,
}

/// Preflight report grouped into blockers, warnings, and passes.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct GroupedReport {
    pub summary: ReportSummary,
    pub blockers: Vec<Item>,
    pub actionable_warnings: Vec<Item>,
    pub contextual_warnings: Vec<Item>,
    pub passed_checks: Vec<Item>,
}

impl Report {
    /// Returns `true` when the report contains at least one blocking error.
    pub fn has_errors(&self) -> bool {
        self.items.iter().any(|item| item.level == Level::Error)
    }

    /// Returns `true` when the report contains at least one warning.
    pub fn has_warnings(&self) -> bool {
        self.items.iter().any(|item| item.level == Level::Warn)
    }

    /// Renders the report in the default grouped text format.
    pub fn render(&self) -> String {
        presentation::render_report(self, false)
    }

    /// Renders the report with passed checks included.
    pub fn render_verbose(&self) -> String {
        presentation::render_report(self, true)
    }

    /// Returns a grouped representation used by CLI and JSON output.
    pub fn grouped(&self) -> GroupedReport {
        let mut blockers = Vec::new();
        let mut actionable_warnings = Vec::new();
        let mut contextual_warnings = Vec::new();
        let mut passed_checks = Vec::new();

        for item in &self.items {
            match item.level {
                Level::Error => blockers.push(item.clone()),
                Level::Warn if is_contextual_warning(item) => {
                    contextual_warnings.push(item.clone())
                }
                Level::Warn => actionable_warnings.push(item.clone()),
                Level::Ok => passed_checks.push(item.clone()),
            }
        }

        GroupedReport {
            summary: ReportSummary {
                blockers: blockers.len(),
                actionable_warnings: actionable_warnings.len(),
                contextual_warnings: contextual_warnings.len(),
                passed_checks: passed_checks.len(),
            },
            blockers,
            actionable_warnings,
            contextual_warnings,
            passed_checks,
        }
    }
}

fn is_contextual_warning(item: &Item) -> bool {
    matches!(item.level, Level::Warn)
        && (item.message.starts_with(CONTEXTUAL_TASK_PROLOG_PREFIX)
            || item.message.starts_with(CONTEXTUAL_PYXIS_HELPER_PREFIX)
            || item
                .message
                .starts_with(CONTEXTUAL_METRICS_COLLECTOR_PREFIX))
}

/// How user-facing notices should be written to stderr.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeFormat {
    /// Human-readable text such as `warning: ...`.
    Text,
    /// One JSON object per line for commands whose stdout is machine-readable.
    Json,
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
    presentation::init_logging(verbose, debug);
}

/// Sets how subsequent user-facing notices are emitted by this thread.
pub fn set_notice_format(format: NoticeFormat) {
    presentation::set_notice_format(format);
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
    presentation::emit_notice(notice);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contextual_warning_classifier_preserves_exact_prefix_boundaries() {
        let warn = |message: &str| Item {
            level: Level::Warn,
            message: message.to_string(),
            remediation: None,
        };
        assert!(is_contextual_warning(&warn(
            "neither /etc/slurm/task_prolog.hk nor /etc/slurm/task_prolog exists on this node"
        )));
        assert!(is_contextual_warning(&warn(
            "site Pyxis helper path is /opt/missing"
        )));
        assert!(is_contextual_warning(&warn(
            "metrics collector nvidia-smi not found on host"
        )));
        assert!(is_contextual_warning(&warn(
            "metrics collector any future suffix remains contextual"
        )));
        assert!(!is_contextual_warning(&Item {
            level: Level::Error,
            message: "metrics collector missing".to_string(),
            remediation: None,
        }));
        for message in [
            "neither /etc/slurm/task_prolog.hk nor /etc/slurm/task_prolog exists",
            "site Pyxis helper path is present: /scratch",
            "metrics collector 'gpu' can query nvidia-smi",
        ] {
            assert!(!is_contextual_warning(&Item {
                level: Level::Ok,
                message: message.to_string(),
                remediation: None,
            }));
        }
        assert!(!is_contextual_warning(&warn(
            " Metrics collector nvidia-smi not found on host"
        )));
        assert!(!is_contextual_warning(&warn(
            "Metrics collector nvidia-smi not found on host"
        )));
        assert!(!is_contextual_warning(&warn("something else entirely")));
    }

    #[test]
    fn grouped_report_serialization_preserves_warning_families_and_item_fields() {
        let report = Report {
            items: vec![
                Item {
                    level: Level::Error,
                    message: "blocking check".into(),
                    remediation: Some("repair it".into()),
                },
                Item {
                    level: Level::Warn,
                    message: "actionable check".into(),
                    remediation: Some("act on it".into()),
                },
                Item {
                    level: Level::Warn,
                    message: "neither /etc/slurm/task_prolog.hk nor /etc/slurm/task_prolog exists"
                        .into(),
                    remediation: None,
                },
                Item {
                    level: Level::Warn,
                    message: "site Pyxis helper path is /opt/site/pyxis".into(),
                    remediation: Some("check the site helper".into()),
                },
                Item {
                    level: Level::Warn,
                    message: "metrics collector nvidia-smi not found on host".into(),
                    remediation: None,
                },
                Item {
                    level: Level::Ok,
                    message: "passing check".into(),
                    remediation: None,
                },
            ],
        };

        let serialized = serde_json::to_string_pretty(&report.grouped()).expect("grouped JSON");
        assert_eq!(
            serialized,
            r#"{
  "summary": {
    "blockers": 1,
    "actionable_warnings": 1,
    "contextual_warnings": 3,
    "passed_checks": 1
  },
  "blockers": [
    {
      "level": "error",
      "message": "blocking check",
      "remediation": "repair it"
    }
  ],
  "actionable_warnings": [
    {
      "level": "warn",
      "message": "actionable check",
      "remediation": "act on it"
    }
  ],
  "contextual_warnings": [
    {
      "level": "warn",
      "message": "neither /etc/slurm/task_prolog.hk nor /etc/slurm/task_prolog exists",
      "remediation": null
    },
    {
      "level": "warn",
      "message": "site Pyxis helper path is /opt/site/pyxis",
      "remediation": "check the site helper"
    },
    {
      "level": "warn",
      "message": "metrics collector nvidia-smi not found on host",
      "remediation": null
    }
  ],
  "passed_checks": [
    {
      "level": "ok",
      "message": "passing check",
      "remediation": null
    }
  ]
}"#
        );
    }

    #[test]
    fn notice_serialization_preserves_exact_compact_contract() {
        let warning = Notice::warning("cache unavailable");
        assert_eq!(
            serde_json::to_string(&warning).expect("warning JSON"),
            r#"{"schema_version":1,"level":"warning","code":null,"message":"cache unavailable","remediation":null}"#
        );

        let mut coded = Notice::informational("cache restored").with_code("cache_ready");
        coded.remediation = Some("retry the command".into());
        assert_eq!(
            serde_json::to_string(&coded).expect("coded notice JSON"),
            r#"{"schema_version":1,"level":"notice","code":"cache_ready","message":"cache restored","remediation":"retry the command"}"#
        );
    }
}
