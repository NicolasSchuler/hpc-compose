use std::cell::Cell;
use std::io::{self, Write};

use tracing_subscriber::EnvFilter;

use super::{Item, Notice, NoticeFormat, Report};
use crate::term;

thread_local! {
    static NOTICE_FORMAT: Cell<NoticeFormat> = const { Cell::new(NoticeFormat::Text) };
}

pub(super) fn render_report(report: &Report, verbose: bool) -> String {
    if report.items.is_empty() {
        return String::new();
    }

    let grouped = report.grouped();
    let blocker_label = if grouped.summary.blockers > 0 {
        term::styled_error(&grouped.summary.blockers.to_string())
    } else {
        grouped.summary.blockers.to_string()
    };
    let warn_label = if grouped.summary.actionable_warnings > 0 {
        term::styled_warning(&grouped.summary.actionable_warnings.to_string())
    } else {
        grouped.summary.actionable_warnings.to_string()
    };
    let ctx_label = if grouped.summary.contextual_warnings > 0 {
        term::styled_warning(&grouped.summary.contextual_warnings.to_string())
    } else {
        grouped.summary.contextual_warnings.to_string()
    };
    let passed_label = term::styled_success(&grouped.summary.passed_checks.to_string());
    let mut lines = vec![format!(
        "Summary: {} blocker(s), {} actionable warning(s), {} contextual warning(s), {} passed checks",
        blocker_label, warn_label, ctx_label, passed_label
    )];

    if crate::platform::is_macos() && grouped.summary.blockers > 0 {
        lines.push(
            "note: macOS is an authoring-only platform; missing Slurm/Enroot runtime tools are expected here. Run from a Linux Slurm login node.".to_string(),
        );
    }

    render_section(
        &mut lines,
        "Blockers",
        &grouped.blockers,
        term::styled_error,
    );
    render_section(
        &mut lines,
        "Actionable warnings",
        &grouped.actionable_warnings,
        term::styled_warning,
    );
    render_section(
        &mut lines,
        "Contextual warnings",
        &grouped.contextual_warnings,
        term::styled_warning,
    );

    if verbose {
        render_section(
            &mut lines,
            "Passed checks",
            &grouped.passed_checks,
            term::styled_success,
        );
    } else {
        lines.push(format!(
            "Passed checks: {}",
            term::styled_success(&grouped.summary.passed_checks.to_string())
        ));
    }

    lines.join("\n")
}

fn render_section(
    lines: &mut Vec<String>,
    title: &str,
    items: &[Item],
    style_fn: fn(&str) -> String,
) {
    if items.is_empty() {
        return;
    }

    lines.push(format!("{}:", term::styled_section_header(title)));
    for item in items {
        lines.push(format!("- {}", style_fn(&item.message)));
        if let Some(remediation) = &item.remediation {
            lines.push(format!(
                "  {}: {remediation}",
                term::styled_note("remediation")
            ));
        }
    }
}

pub(super) fn init_logging(verbose: u8, debug: bool) {
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

pub(super) fn set_notice_format(format: NoticeFormat) {
    NOTICE_FORMAT.with(|cell| cell.set(format));
}

pub(super) fn emit_notice(notice: Notice) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::Level;

    #[test]
    fn report_rendering_preserves_exact_plain_and_ansi_contracts() {
        let report = Report {
            items: vec![
                Item {
                    level: Level::Ok,
                    message: "fine".into(),
                    remediation: None,
                },
                Item {
                    level: Level::Warn,
                    message: "warn".into(),
                    remediation: Some("fix".into()),
                },
                Item {
                    level: Level::Warn,
                    message: "metrics collector 'gpu' requires nvidia-smi".into(),
                    remediation: Some("Install nvidia-smi on compute nodes".into()),
                },
                Item {
                    level: Level::Error,
                    message: "boom".into(),
                    remediation: Some("repair".into()),
                },
            ],
        };
        let macos_note = if crate::platform::is_macos() {
            "\nnote: macOS is an authoring-only platform; missing Slurm/Enroot runtime tools are expected here. Run from a Linux Slurm login node."
        } else {
            ""
        };

        crate::term::with_test_color_policy(crate::term::ColorPolicy::Never, || {
            assert_eq!(
                report.render(),
                format!(
                    "Summary: 1 blocker(s), 1 actionable warning(s), 1 contextual warning(s), 1 passed checks{macos_note}\nBlockers:\n- boom\n  remediation: repair\nActionable warnings:\n- warn\n  remediation: fix\nContextual warnings:\n- metrics collector 'gpu' requires nvidia-smi\n  remediation: Install nvidia-smi on compute nodes\nPassed checks: 1"
                )
            );
            assert_eq!(
                report.render_verbose(),
                format!(
                    "Summary: 1 blocker(s), 1 actionable warning(s), 1 contextual warning(s), 1 passed checks{macos_note}\nBlockers:\n- boom\n  remediation: repair\nActionable warnings:\n- warn\n  remediation: fix\nContextual warnings:\n- metrics collector 'gpu' requires nvidia-smi\n  remediation: Install nvidia-smi on compute nodes\nPassed checks:\n- fine"
                )
            );
            assert!(!report.render().ends_with('\n'));
            assert_eq!(Report { items: Vec::new() }.render(), "");
        });

        crate::term::with_test_color_policy(crate::term::ColorPolicy::Always, || {
            assert_eq!(
                report.render(),
                format!(
                    "Summary: \u{1b}[31m1\u{1b}[39m blocker(s), \u{1b}[33m1\u{1b}[39m actionable warning(s), \u{1b}[33m1\u{1b}[39m contextual warning(s), \u{1b}[32m1\u{1b}[39m passed checks{macos_note}\n\u{1b}[1mBlockers\u{1b}[0m:\n- \u{1b}[31mboom\u{1b}[39m\n  \u{1b}[2mremediation\u{1b}[0m: repair\n\u{1b}[1mActionable warnings\u{1b}[0m:\n- \u{1b}[33mwarn\u{1b}[39m\n  \u{1b}[2mremediation\u{1b}[0m: fix\n\u{1b}[1mContextual warnings\u{1b}[0m:\n- \u{1b}[33mmetrics collector 'gpu' requires nvidia-smi\u{1b}[39m\n  \u{1b}[2mremediation\u{1b}[0m: Install nvidia-smi on compute nodes\nPassed checks: \u{1b}[32m1\u{1b}[39m"
                )
            );
            assert_eq!(
                report.render_verbose(),
                format!(
                    "Summary: \u{1b}[31m1\u{1b}[39m blocker(s), \u{1b}[33m1\u{1b}[39m actionable warning(s), \u{1b}[33m1\u{1b}[39m contextual warning(s), \u{1b}[32m1\u{1b}[39m passed checks{macos_note}\n\u{1b}[1mBlockers\u{1b}[0m:\n- \u{1b}[31mboom\u{1b}[39m\n  \u{1b}[2mremediation\u{1b}[0m: repair\n\u{1b}[1mActionable warnings\u{1b}[0m:\n- \u{1b}[33mwarn\u{1b}[39m\n  \u{1b}[2mremediation\u{1b}[0m: fix\n\u{1b}[1mContextual warnings\u{1b}[0m:\n- \u{1b}[33mmetrics collector 'gpu' requires nvidia-smi\u{1b}[39m\n  \u{1b}[2mremediation\u{1b}[0m: Install nvidia-smi on compute nodes\n\u{1b}[1mPassed checks\u{1b}[0m:\n- \u{1b}[32mfine\u{1b}[39m"
                )
            );
        });
    }

    #[test]
    fn notice_format_is_thread_local_and_defaults_to_text() {
        std::thread::spawn(|| {
            assert_eq!(
                NOTICE_FORMAT.with(Cell::get),
                NoticeFormat::Text,
                "a new thread must start in text mode"
            );
            set_notice_format(NoticeFormat::Json);
            assert_eq!(NOTICE_FORMAT.with(Cell::get), NoticeFormat::Json);

            let nested = std::thread::spawn(|| NOTICE_FORMAT.with(Cell::get))
                .join()
                .expect("nested notice-format thread");
            assert_eq!(
                nested,
                NoticeFormat::Text,
                "notice format must not leak into another thread"
            );
        })
        .join()
        .expect("notice-format thread");
    }
}
