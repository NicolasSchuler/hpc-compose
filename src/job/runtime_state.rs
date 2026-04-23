use super::*;

#[derive(Debug, Clone, Deserialize)]
pub(super) struct ServiceRuntimeStateFile {
    #[allow(dead_code)]
    #[serde(default)]
    pub(super) backend: Option<SubmissionBackend>,
    #[serde(default)]
    pub(super) job_status: Option<String>,
    #[serde(default)]
    pub(super) job_exit_code: Option<i32>,
    #[serde(default)]
    pub(super) supervisor_pid: Option<u32>,
    #[serde(default)]
    pub(super) attempt: Option<u32>,
    #[serde(default)]
    pub(super) is_resume: Option<bool>,
    #[serde(default)]
    pub(super) resume_dir: Option<PathBuf>,
    #[serde(default)]
    pub(super) services: Vec<ServiceRuntimeStateEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct ServiceRuntimeStateEntry {
    pub(super) service_name: String,
    #[serde(default)]
    pub(super) step_name: Option<String>,
    #[serde(default)]
    pub(super) log_path: Option<PathBuf>,
    #[serde(default)]
    pub(super) launch_index: Option<u32>,
    #[serde(default)]
    pub(super) launcher_pid: Option<u32>,
    #[serde(default)]
    pub(super) healthy: Option<bool>,
    #[serde(default)]
    pub(super) completed_successfully: Option<bool>,
    #[serde(default)]
    pub(super) readiness_configured: Option<bool>,
    #[serde(default)]
    pub(super) failure_policy_mode: Option<String>,
    #[serde(default)]
    pub(super) restart_count: Option<u32>,
    #[serde(default)]
    pub(super) max_restarts: Option<u32>,
    #[serde(default)]
    pub(super) window_seconds: Option<u64>,
    #[serde(default)]
    pub(super) max_restarts_in_window: Option<u32>,
    #[serde(default)]
    pub(super) restart_failures_in_window: Option<u32>,
    #[serde(default)]
    pub(super) restart_failure_timestamps: Option<Vec<u64>>,
    #[serde(default)]
    pub(super) last_exit_code: Option<i32>,
    #[serde(default)]
    pub(super) placement_mode: Option<String>,
    #[serde(default)]
    pub(super) nodes: Option<u32>,
    #[serde(default)]
    pub(super) ntasks: Option<u32>,
    #[serde(default)]
    pub(super) ntasks_per_node: Option<u32>,
    #[serde(default)]
    pub(super) nodelist: Option<String>,
}

pub(super) fn load_runtime_state(record: &SubmissionRecord) -> Option<ServiceRuntimeStateFile> {
    let state_path = tracked_paths::latest_state_path(&tracked_paths::runtime_job_root(
        &record.submit_dir,
        &record.job_id,
    ));
    read_json::<ServiceRuntimeStateFile>(&state_path).ok()
}

pub(super) fn active_restart_failures_in_window(
    state: &ServiceRuntimeStateEntry,
    now: u64,
) -> Option<u32> {
    let timestamps = state.restart_failure_timestamps.as_ref()?;
    let window_seconds = state.window_seconds?;
    Some(
        timestamps
            .iter()
            .filter(|&&timestamp| now.saturating_sub(timestamp) < window_seconds)
            .count() as u32,
    )
}

pub(super) fn runtime_state_by_service(
    state: &ServiceRuntimeStateFile,
) -> BTreeMap<String, ServiceRuntimeStateEntry> {
    let mut by_service = BTreeMap::new();
    for service in &state.services {
        by_service.insert(service.service_name.clone(), service.clone());
    }
    by_service
}
