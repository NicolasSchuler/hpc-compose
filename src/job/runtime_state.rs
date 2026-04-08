use super::*;

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
