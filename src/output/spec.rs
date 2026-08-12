use std::path::PathBuf;

use hpc_compose::context::{ResolvedValue, ValueSource};
use hpc_compose::lint::LintFinding;
use hpc_compose::lint_fix::AppliedFix;
use serde::Serialize;

pub(crate) use super::{
    InterpolationVarsOutput, RenderOutput, build_validate_output, print_interpolation_vars,
    print_plan_inspect, print_plan_inspect_tree, print_plan_inspect_verbose,
    print_plan_inspect_verbose_with_profile, print_prepare_summary, print_report,
    print_rightsize_report,
};

/// Renders the proposed lint rewrite as a minimal unified diff.
///
/// Uses an LCS-over-lines pass. Unchanged ("equal") lines are emitted
/// verbatim (no leading space) so the surrounding context of each change is
/// readable without the noise of a full unified-diff context window.
pub(crate) fn unified_diff(original: &str, updated: &str) -> String {
    let a: Vec<&str> = original.split_inclusive('\n').collect();
    let b: Vec<&str> = updated.split_inclusive('\n').collect();
    let hunks = diff_hunks(&a, &b);
    let mut out = String::new();
    out.push_str("--- lint --fix (proposed)\n+++ lint --fix (proposed)\n");
    for (tag, line) in hunks {
        match tag {
            DiffTag::Equal => out.push_str(line),
            DiffTag::Delete => {
                out.push('-');
                out.push_str(line);
            }
            DiffTag::Insert => {
                out.push('+');
                out.push_str(line);
            }
        }
    }
    out
}

#[derive(Clone, Copy)]
enum DiffTag {
    Equal,
    Delete,
    Insert,
}

fn diff_hunks<'a>(a: &[&'a str], b: &[&'a str]) -> Vec<(DiffTag, &'a str)> {
    let lcs = lcs_table(a, b);
    let mut i = a.len();
    let mut j = b.len();
    let mut out = Vec::new();
    while i > 0 || j > 0 {
        if i > 0 && j > 0 && a[i - 1] == b[j - 1] {
            out.push((DiffTag::Equal, a[i - 1]));
            i -= 1;
            j -= 1;
        } else if j > 0 && (i == 0 || lcs[i][j - 1] >= lcs[i - 1][j]) {
            out.push((DiffTag::Insert, b[j - 1]));
            j -= 1;
        } else {
            out.push((DiffTag::Delete, a[i - 1]));
            i -= 1;
        }
    }
    out.reverse();
    out
}

fn lcs_table(a: &[&str], b: &[&str]) -> Vec<Vec<usize>> {
    let mut dp = vec![vec![0_usize; b.len() + 1]; a.len() + 1];
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            dp[i][j] = if a[i - 1] == b[j - 1] {
                dp[i - 1][j - 1] + 1
            } else {
                dp[i - 1][j].max(dp[i][j - 1])
            };
        }
    }
    dp
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct LintOutput {
    pub(crate) schema_version: u32,
    pub(crate) passed: bool,
    pub(crate) compose_file: PathBuf,
    pub(crate) warning_count: usize,
    pub(crate) error_count: usize,
    pub(crate) fixable_count: usize,
    pub(crate) findings: Vec<LintFinding>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) applied_fixes: Vec<AppliedFix>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct PlanOutput {
    pub(crate) schema_version: u32,
    pub(crate) valid: bool,
    pub(crate) compose_file: PathBuf,
    pub(crate) runtime_plan: hpc_compose::runtime_plan::RuntimePlan,
    pub(crate) cluster_warnings: Vec<String>,
    pub(crate) explanations: Vec<PlanHint>,
    pub(crate) script: Option<String>,
}

#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub(crate) struct PlanHint {
    pub(crate) level: &'static str,
    pub(crate) message: String,
}

/// `explain --format json` output: the provenance entries selected by the
/// query (or the full map when no query is given).
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExplainOutput {
    pub(crate) schema_version: u32,
    pub(crate) compose_file: PathBuf,
    pub(crate) entries: Vec<ExplainEntry>,
}

/// One provenance span: a spec field and the preview-script line range it
/// produced.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExplainEntry {
    /// Spec path that produced the lines, e.g. `x-slurm.mem` or
    /// `services.app.readiness.tcp`.
    pub(crate) source: String,
    /// Feature-block section name for banner-level entries, e.g.
    /// `artifact helpers`.
    pub(crate) section: Option<String>,
    /// First script line of the span (1-based, inclusive).
    pub(crate) start_line: usize,
    /// Last script line of the span (1-based, inclusive).
    pub(crate) end_line: usize,
    /// The matching script lines, secret-redacted. Empty in full-map mode,
    /// which reports line ranges without echoing contents.
    pub(crate) lines: Vec<String>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ContextRuntimePaths {
    pub(crate) compose_dir: PathBuf,
    pub(crate) current_submit_dir: PathBuf,
    pub(crate) default_script_path: PathBuf,
    pub(crate) runtime_job_root_pattern: String,
    pub(crate) cache_dir: Option<ResolvedValue<PathBuf>>,
    /// Resolved enroot prepare-time temporary scratch directory
    /// (`ENROOT_TEMP_PATH`).
    pub(crate) enroot_temp_dir: ResolvedValue<PathBuf>,
    pub(crate) resume_dir: Option<ResolvedValue<PathBuf>>,
    pub(crate) artifact_export_dir: Option<ResolvedValue<String>>,
    pub(crate) metadata_root: ResolvedValue<PathBuf>,
    pub(crate) jobs_dir: ResolvedValue<PathBuf>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ContextOutput {
    pub(crate) schema_version: u32,
    pub(crate) cwd: PathBuf,
    pub(crate) settings_path: Option<PathBuf>,
    pub(crate) settings_base_dir: Option<PathBuf>,
    pub(crate) selected_profile: Option<String>,
    pub(crate) compose_file: ResolvedValue<PathBuf>,
    pub(crate) binaries: hpc_compose::context::ResolvedBinaries,
    pub(crate) interpolation_vars: std::collections::BTreeMap<String, String>,
    pub(crate) interpolation_var_sources: std::collections::BTreeMap<String, ValueSource>,
    pub(crate) compose_load_error: Option<String>,
    pub(crate) runtime_paths: ContextRuntimePaths,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unified_diff_preserves_headers_equal_lines_and_edits() {
        assert_eq!(
            unified_diff("a\nb\nc\n", "a\nx\nc\n"),
            "--- lint --fix (proposed)\n+++ lint --fix (proposed)\na\n-b\n+x\nc\n"
        );
    }

    #[test]
    fn unified_diff_preserves_unchanged_input_verbatim() {
        assert_eq!(
            unified_diff("a\nb\n", "a\nb\n"),
            "--- lint --fix (proposed)\n+++ lint --fix (proposed)\na\nb\n"
        );
    }

    #[test]
    fn unified_diff_preserves_trailing_newline_behavior() {
        assert_eq!(
            unified_diff("a\nold", "a\nnew"),
            "--- lint --fix (proposed)\n+++ lint --fix (proposed)\na\n-old+new"
        );
        assert_eq!(
            unified_diff("a\n", "a"),
            "--- lint --fix (proposed)\n+++ lint --fix (proposed)\n-a\n+a"
        );
    }

    #[test]
    fn unified_diff_preserves_lcs_tie_breaking() {
        assert_eq!(
            unified_diff("a\nb\n", "b\na\n"),
            "--- lint --fix (proposed)\n+++ lint --fix (proposed)\n-a\nb\n+a\n"
        );
    }
}
