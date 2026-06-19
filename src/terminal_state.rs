//! Interpretation of terminal Slurm job states into actionable remediation.
//!
//! When a job ends in a terminal *failure* state, surfacing the raw Slurm
//! string (e.g. `OUT_OF_MEMORY`) is necessary but not sufficient — the whole
//! value proposition of the tool is helping users move from an opaque failure
//! to the next action. [`interpret`] maps the common terminal-failure states to
//! a one-line explanation and a hint about whether right-sizing is relevant.

/// Actionable guidance for a terminal Slurm job state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalRemediation {
    /// One-line, user-facing explanation of what the state means and what to do.
    pub message: &'static str,
    /// Whether suggesting `inspect --rightsize` is relevant (memory/time issues).
    pub suggest_rightsize: bool,
}

/// Returns remediation guidance for a terminal Slurm state string, or `None`
/// for success / cancellation / unrecognized states (e.g. `COMPLETED`,
/// `CANCELLED`). Matching is case-insensitive and ignores any trailing detail
/// Slurm appends (such as `CANCELLED by 1234`).
#[must_use]
pub fn interpret(state: &str) -> Option<TerminalRemediation> {
    let normalized = state.trim().to_ascii_uppercase();
    let key = normalized.split_whitespace().next().unwrap_or("");
    let (message, suggest_rightsize) = match key {
        "OUT_OF_MEMORY" | "OOM" => (
            "the job exceeded its memory allocation; raise x-slurm.mem (or the service's mem) or run `hpc-compose inspect --rightsize` for a data-driven suggestion",
            true,
        ),
        "TIMEOUT" => (
            "the job exceeded its walltime; raise x-slurm.time or run `hpc-compose inspect --rightsize`",
            true,
        ),
        "NODE_FAIL" | "BOOT_FAIL" => (
            "a node failed (infrastructure, not your job); this is usually safe to resubmit with `hpc-compose up`",
            false,
        ),
        "PREEMPTED" => (
            "the job was preempted by higher-priority work; resubmit, or use a higher-priority QoS/partition if available",
            false,
        ),
        "DEADLINE" => (
            "the job hit its scheduling deadline before completing; adjust the deadline or resource request and resubmit",
            false,
        ),
        "LAUNCH_FAILED" | "RECONFIG_FAIL" => (
            "Slurm could not launch the job step; check the batch log and run `hpc-compose debug --preflight` for environment issues",
            false,
        ),
        _ => return None,
    };
    Some(TerminalRemediation {
        message,
        suggest_rightsize,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_failure_states() {
        assert!(interpret("OUT_OF_MEMORY").unwrap().suggest_rightsize);
        assert!(interpret("timeout").unwrap().suggest_rightsize);
        assert!(!interpret("NODE_FAIL").unwrap().suggest_rightsize);
        assert!(interpret("PREEMPTED").is_some());
    }

    #[test]
    fn ignores_trailing_detail_and_success_states() {
        assert!(interpret("CANCELLED by 1234").is_none());
        assert!(interpret("COMPLETED").is_none());
        assert!(interpret("").is_none());
        // Trailing detail on a failure state is still recognized by prefix.
        assert!(interpret("OUT_OF_MEMORY (step 0)").is_some());
    }
}
