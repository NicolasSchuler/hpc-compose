use std::env;
use std::path::{Path, PathBuf};

use crate::runtime_plan::RuntimePlan;

use super::model::SubmissionBackend;

pub(super) fn batch_log_path_for_backend(
    plan: &RuntimePlan,
    submit_dir: &Path,
    job_id: &str,
    backend: SubmissionBackend,
) -> PathBuf {
    // When the user pins x-slurm.output, honor it verbatim (relative -> submit_dir).
    if let Some(raw) = plan.slurm.output.clone() {
        let rendered = expand_slurm_filename_pattern(
            &raw,
            job_id,
            &plan.name,
            current_user_name().as_deref(),
            false,
        );
        let candidate = PathBuf::from(rendered);
        return if candidate.is_absolute() {
            candidate
        } else {
            submit_dir.join(candidate)
        };
    }
    // Default: a hidden, job-id-free parent under the resolved runtime root that
    // can be pre-created host-side before sbatch (Slurm opens --output before the
    // script body runs). %j is client-expanded here so the persisted record
    // points at the concrete file Slurm will write. Keep the basename independent
    // of the raw Slurm job name because names can contain path separators.
    let _ = backend; // both backends share the same default shape
    let runtime_root =
        crate::tracked_paths::resolve_runtime_root(submit_dir, plan.slurm.runtime_root.as_deref());
    let file = expand_slurm_filename_pattern(
        crate::tracked_paths::DEFAULT_BATCH_LOG_FILE_PATTERN,
        job_id,
        &plan.name,
        current_user_name().as_deref(),
        false,
    );
    runtime_root
        .join(crate::tracked_paths::LOGS_DIR_NAME)
        .join(file)
}

#[cfg(test)]
pub(super) fn batch_log_path_for(plan: &RuntimePlan, submit_dir: &Path, job_id: &str) -> PathBuf {
    batch_log_path_for_backend(plan, submit_dir, job_id, SubmissionBackend::Slurm)
}

/// Returns a filesystem glob (with `%t`/`%N`/`%n`/`%s`/`%a` -> `*`) matching the
/// batch log(s) Slurm wrote for a job. `%j`/`%A`/`%x`/`%u` are still expanded
/// from submit-time values. For read-back/discovery only; callers that stat a
/// single concrete path must use [`batch_log_path_for_backend`].
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn batch_log_glob_for_backend(
    plan: &RuntimePlan,
    submit_dir: &Path,
    job_id: &str,
    _backend: SubmissionBackend,
) -> PathBuf {
    if let Some(raw) = plan.slurm.output.clone() {
        let rendered = expand_slurm_filename_pattern(
            &raw,
            job_id,
            &plan.name,
            current_user_name().as_deref(),
            true,
        );
        let candidate = PathBuf::from(rendered);
        return if candidate.is_absolute() {
            candidate
        } else {
            submit_dir.join(candidate)
        };
    }
    let runtime_root =
        crate::tracked_paths::resolve_runtime_root(submit_dir, plan.slurm.runtime_root.as_deref());
    let file = expand_slurm_filename_pattern(
        crate::tracked_paths::DEFAULT_BATCH_LOG_FILE_PATTERN,
        job_id,
        &plan.name,
        current_user_name().as_deref(),
        true,
    );
    runtime_root
        .join(crate::tracked_paths::LOGS_DIR_NAME)
        .join(file)
}

pub(super) fn expand_slurm_filename_pattern(
    pattern: &str,
    job_id: &str,
    job_name: &str,
    user_name: Option<&str>,
    glob_per_task: bool,
) -> String {
    let mut rendered = String::new();
    let mut chars = pattern.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch != '%' {
            rendered.push(ch);
            continue;
        }

        if matches!(chars.peek(), Some('%')) {
            chars.next();
            rendered.push('%');
            continue;
        }

        let mut width = String::new();
        while let Some(peek) = chars.peek() {
            if peek.is_ascii_digit() {
                width.push(*peek);
                chars.next();
            } else {
                break;
            }
        }

        let Some(specifier) = chars.next() else {
            rendered.push('%');
            rendered.push_str(&width);
            break;
        };
        let padded_width = width.parse::<usize>().ok().map(|value| value.min(10));

        match specifier {
            'j' | 'A' => rendered.push_str(&zero_pad(job_id, padded_width)),
            'x' => rendered.push_str(job_name),
            'u' => {
                if let Some(user_name) = user_name {
                    rendered.push_str(user_name);
                } else {
                    rendered.push('%');
                    rendered.push_str(&width);
                    rendered.push(specifier);
                }
            }
            // Per-task / per-node specifiers are only known to Slurm at runtime.
            // For read-back matching, collapse each to a single glob `*`; for the
            // record path (glob_per_task = false) leave them literal so the path
            // stays stable (callers that stat directly must not glob).
            't' | 'N' | 'n' | 's' | 'a' if glob_per_task => rendered.push('*'),
            _ => {
                rendered.push('%');
                rendered.push_str(&width);
                rendered.push(specifier);
            }
        }
    }

    rendered
}

fn zero_pad(value: &str, width: Option<usize>) -> String {
    if let Some(width) = width {
        format!("{value:0>width$}")
    } else {
        value.to_string()
    }
}

fn current_user_name() -> Option<String> {
    env::var("USER").ok().or_else(|| env::var("LOGNAME").ok())
}
