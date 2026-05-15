use super::runtime_state::{ServiceRuntimeStateEntry, ServiceRuntimeStateFile};
use super::scheduler::{build_log_status, build_scheduler_status};
use super::*;

const SERVICE_EXITS_DIR_NAME: &str = "service-exits";
const REPLAY_FIDELITY: &str = "best-effort";

/// Reconstructed best-effort replay timeline for one tracked job.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct ReplayReport {
    pub job_id: String,
    pub record: SubmissionRecord,
    pub fidelity: String,
    pub notes: Vec<String>,
    pub artifacts: ReplayArtifactPaths,
    pub events: Vec<ReplayEvent>,
    pub frames: Vec<ReplayFrame>,
    pub timeline_start_unix: Option<u64>,
    pub timeline_end_unix: Option<u64>,
}

/// Runtime artifact paths consulted by replay.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Serialize)]
pub struct ReplayArtifactPaths {
    pub runtime_roots: Vec<PathBuf>,
    pub state_paths: Vec<PathBuf>,
    pub service_exit_dirs: Vec<PathBuf>,
    pub metrics_dirs: Vec<PathBuf>,
    pub log_dirs: Vec<PathBuf>,
}

/// One point on the reconstructed replay timeline.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplayEvent {
    pub at_unix: u64,
    pub attempt: Option<u32>,
    pub kind: ReplayEventKind,
    pub service: Option<String>,
    pub exit_code: Option<i32>,
    pub detail: Option<String>,
}

/// Timeline event categories used for stable ordering.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ReplayEventKind {
    AttemptStart,
    ServiceStart,
    MetricsSample,
    ServiceExit,
    FinalSnapshot,
}

/// One renderable watch-style frame at a replay event boundary.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct ReplayFrame {
    pub cursor_unix: u64,
    pub event_index: usize,
    pub event: ReplayEvent,
    pub services: Vec<ReplayServiceFrame>,
    pub metrics_line: Option<String>,
    pub fidelity_note: Option<String>,
    #[serde(skip_serializing)]
    pub snapshot: PsSnapshot,
}

/// Reconstructed service state for one replay frame.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplayServiceFrame {
    pub service_name: String,
    pub status: String,
    pub started_at: Option<u64>,
    pub finished_at: Option<u64>,
    pub last_exit_code: Option<i32>,
    pub restart_count: Option<u32>,
}

#[derive(Debug, Clone)]
struct ReplayRoot {
    attempt: Option<u32>,
    root: PathBuf,
    state_path: PathBuf,
    service_exit_dir: PathBuf,
    metrics_dir: PathBuf,
    log_dir: PathBuf,
    state: Option<ServiceRuntimeStateFile>,
}

#[derive(Debug, Clone, Deserialize)]
struct ServiceExitMarkerRow {
    service: String,
    exit_code: i32,
    at_unix: u64,
    #[serde(default)]
    node: Option<String>,
    #[serde(default)]
    rank: Option<String>,
    #[serde(default)]
    nodelist: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ReplayGpuRow {
    sampled_at: String,
    #[serde(default)]
    utilization_gpu: Option<String>,
    #[serde(default)]
    memory_used_mib: Option<String>,
    #[serde(default)]
    memory_total_mib: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ReplaySlurmRow {
    sampled_at: String,
    #[serde(default)]
    step_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ReplayMetricAggregate {
    at_unix: u64,
    gpu_count: usize,
    gpu_util_sum: u64,
    gpu_util_count: usize,
    memory_used_mib: Option<u64>,
    memory_total_mib: Option<u64>,
    slurm_steps: usize,
}

#[derive(Debug, Clone)]
struct ReplayMetricSample {
    at_unix: u64,
    attempt: Option<u32>,
    line: String,
}

#[derive(Debug, Clone)]
struct ReplayExit {
    at_unix: u64,
    attempt: Option<u32>,
    service: String,
    exit_code: i32,
    detail: Option<String>,
    from_marker: bool,
}

/// Builds a best-effort replay report from existing tracked artifacts.
pub fn build_replay_report(
    record: &SubmissionRecord,
    service_filter: Option<&str>,
) -> Result<ReplayReport> {
    if let Some(service) = service_filter
        && !record.service_logs.contains_key(service)
    {
        bail!(
            "service '{}' does not exist in tracked job {}",
            service,
            record.job_id
        );
    }

    let mut notes = vec![
        "replay is best-effort because state.json is a final snapshot and service logs do not contain per-line timestamps".to_string(),
    ];
    let roots = discover_replay_roots(record, &mut notes)?;
    let mut artifacts = ReplayArtifactPaths::default();
    let mut final_state_by_service = BTreeMap::new();
    let mut root_states = Vec::new();
    let mut exits = Vec::new();
    let mut metrics = Vec::new();

    for root in &roots {
        artifacts.runtime_roots.push(root.root.clone());
        artifacts.state_paths.push(root.state_path.clone());
        artifacts
            .service_exit_dirs
            .push(root.service_exit_dir.clone());
        artifacts.metrics_dirs.push(root.metrics_dir.clone());
        artifacts.log_dirs.push(root.log_dir.clone());

        if let Some(state) = &root.state {
            for service in &state.services {
                final_state_by_service.insert(service.service_name.clone(), service.clone());
            }
            root_states.push((root.attempt.or(state.attempt), state.clone()));
        }

        exits.extend(load_service_exit_markers(root, &mut notes));
        metrics.extend(load_replay_metrics(root, &mut notes));
    }

    metrics.sort_by(|left, right| {
        left.at_unix
            .cmp(&right.at_unix)
            .then_with(|| left.line.cmp(&right.line))
    });
    metrics.dedup_by(|left, right| left.at_unix == right.at_unix && left.line == right.line);

    let service_names = replay_service_names(record, service_filter);
    add_fallback_exits(
        &root_states,
        &mut exits,
        &service_names,
        service_filter,
        &mut notes,
    );
    exits.sort_by(|left, right| {
        left.at_unix
            .cmp(&right.at_unix)
            .then_with(|| left.service.cmp(&right.service))
            .then_with(|| left.exit_code.cmp(&right.exit_code))
    });

    let events = build_replay_events(
        record,
        &roots,
        &root_states,
        &service_names,
        &exits,
        &metrics,
        &final_state_by_service,
    );
    if events.is_empty() {
        notes.push(
            "no replay events could be reconstructed; using submission time as a single frame"
                .to_string(),
        );
    }
    if metrics.is_empty() {
        notes.push("no historical metrics JSONL samples were found for this job".to_string());
    } else if let Some(first_event) = events.first()
        && metrics
            .first()
            .is_some_and(|metric| metric.at_unix > first_event.at_unix)
    {
        notes.push(
            "some early replay frames occur before the first metrics sample; replay never shows future metrics as current"
                .to_string(),
        );
    }

    let events = if events.is_empty() {
        vec![ReplayEvent {
            at_unix: record.submitted_at,
            attempt: None,
            kind: ReplayEventKind::FinalSnapshot,
            service: None,
            exit_code: None,
            detail: Some("fallback final snapshot".to_string()),
        }]
    } else {
        events
    };
    let frames = build_replay_frames(
        record,
        &service_names,
        &final_state_by_service,
        &events,
        &exits,
        &metrics,
    );
    let timeline_start_unix = events.first().map(|event| event.at_unix);
    let timeline_end_unix = events.last().map(|event| event.at_unix);

    Ok(ReplayReport {
        job_id: record.job_id.clone(),
        record: record.clone(),
        fidelity: REPLAY_FIDELITY.to_string(),
        notes,
        artifacts,
        events,
        frames,
        timeline_start_unix,
        timeline_end_unix,
    })
}

impl ReplayReport {
    /// Returns the frame index nearest to, but not after, a cursor timestamp.
    pub fn frame_index_at_or_before(&self, cursor_unix: u64) -> usize {
        let mut selected = 0;
        for (index, frame) in self.frames.iter().enumerate() {
            if frame.cursor_unix > cursor_unix {
                break;
            }
            selected = index;
        }
        selected
    }

    /// Clamps a cursor timestamp to the reconstructed replay bounds.
    pub fn clamp_cursor(&self, cursor_unix: u64) -> u64 {
        match (self.timeline_start_unix, self.timeline_end_unix) {
            (Some(start), Some(end)) => cursor_unix.clamp(start, end),
            (Some(start), None) | (None, Some(start)) => start,
            (None, None) => cursor_unix,
        }
    }
}

fn discover_replay_roots(
    record: &SubmissionRecord,
    notes: &mut Vec<String>,
) -> Result<Vec<ReplayRoot>> {
    let job_root = runtime_job_root_for_record(record);
    let attempts_dir = tracked_paths::attempts_dir(&job_root);
    let mut roots = Vec::new();

    if attempts_dir.is_dir() {
        let mut attempts = Vec::new();
        for entry in fs::read_dir(&attempts_dir)
            .with_context(|| format!("failed to read {}", attempts_dir.display()))?
        {
            let entry = entry?;
            let file_type = entry
                .file_type()
                .with_context(|| format!("failed to stat {}", entry.path().display()))?;
            if !file_type.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            match name.parse::<u32>() {
                Ok(attempt) => attempts.push((attempt, entry.path())),
                Err(_) => notes.push(format!(
                    "ignoring non-numeric replay attempt directory {}",
                    entry.path().display()
                )),
            }
        }
        attempts.sort_by_key(|(attempt, _)| *attempt);
        for (attempt, root) in attempts {
            roots.push(build_replay_root(Some(attempt), root, notes));
        }
    }

    if roots.is_empty() {
        roots.push(build_replay_root(None, job_root, notes));
    } else {
        notes.push(
            "using attempt-specific runtime roots and ignoring top-level latest links to avoid duplicate replay events"
                .to_string(),
        );
    }

    Ok(roots)
}

fn build_replay_root(attempt: Option<u32>, root: PathBuf, notes: &mut Vec<String>) -> ReplayRoot {
    let state_path = tracked_paths::latest_state_path(&root);
    let state = match read_json::<ServiceRuntimeStateFile>(&state_path) {
        Ok(state) => Some(state),
        Err(err) => {
            notes.push(format!(
                "could not read replay state at {}: {err}",
                state_path.display()
            ));
            None
        }
    };
    ReplayRoot {
        attempt,
        service_exit_dir: root.join(SERVICE_EXITS_DIR_NAME),
        metrics_dir: tracked_paths::latest_metrics_dir(&root),
        log_dir: tracked_paths::latest_logs_dir(&root),
        state_path,
        root,
        state,
    }
}

fn replay_service_names(record: &SubmissionRecord, service_filter: Option<&str>) -> Vec<String> {
    match service_filter {
        Some(service) => vec![service.to_string()],
        None => record.service_logs.keys().cloned().collect(),
    }
}

fn load_service_exit_markers(root: &ReplayRoot, notes: &mut Vec<String>) -> Vec<ReplayExit> {
    if !root.service_exit_dir.is_dir() {
        notes.push(format!(
            "no service-exits directory found at {}",
            root.service_exit_dir.display()
        ));
        return Vec::new();
    }

    let mut exits = Vec::new();
    let entries = match fs::read_dir(&root.service_exit_dir) {
        Ok(entries) => entries,
        Err(err) => {
            notes.push(format!(
                "could not read service-exits directory {}: {err}",
                root.service_exit_dir.display()
            ));
            return exits;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
            continue;
        }
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(err) => {
                notes.push(format!("could not read {}: {err}", path.display()));
                continue;
            }
        };
        for (index, raw_line) in raw.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() {
                continue;
            }
            let row: ServiceExitMarkerRow = match serde_json::from_str(line) {
                Ok(row) => row,
                Err(err) => {
                    notes.push(format!(
                        "could not parse {} line {}: {err}",
                        path.display(),
                        index + 1
                    ));
                    continue;
                }
            };
            let detail = replay_exit_detail(&row);
            exits.push(ReplayExit {
                at_unix: row.at_unix,
                attempt: root.attempt,
                service: row.service,
                exit_code: row.exit_code,
                detail,
                from_marker: true,
            });
        }
    }

    exits
}

fn replay_exit_detail(row: &ServiceExitMarkerRow) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(node) = row.node.as_ref().filter(|value| !value.is_empty()) {
        parts.push(format!("node={node}"));
    }
    if let Some(rank) = row.rank.as_ref().filter(|value| !value.is_empty()) {
        parts.push(format!("rank={rank}"));
    }
    if let Some(nodelist) = row.nodelist.as_ref().filter(|value| !value.is_empty()) {
        parts.push(format!("nodelist={nodelist}"));
    }
    (!parts.is_empty()).then(|| parts.join(" "))
}

fn add_fallback_exits(
    root_states: &[(Option<u32>, ServiceRuntimeStateFile)],
    exits: &mut Vec<ReplayExit>,
    service_names: &[String],
    service_filter: Option<&str>,
    notes: &mut Vec<String>,
) {
    for (attempt, state) in root_states {
        for service in &state.services {
            if !service_names.contains(&service.service_name) {
                continue;
            }
            if service_filter.is_some_and(|filter| filter != service.service_name) {
                continue;
            }
            let Some(finished_at) = service.finished_at else {
                continue;
            };
            let Some(exit_code) = service.last_exit_code else {
                continue;
            };
            let has_marker = exits.iter().any(|exit| {
                exit.service == service.service_name
                    && exit.at_unix == finished_at
                    && exit.exit_code == exit_code
            });
            if has_marker {
                continue;
            }
            notes.push(format!(
                "using state.json fallback exit for service '{}' at {} because no matching service-exit marker was found",
                service.service_name, finished_at
            ));
            exits.push(ReplayExit {
                at_unix: finished_at,
                attempt: *attempt,
                service: service.service_name.clone(),
                exit_code,
                detail: Some("from final state.json".to_string()),
                from_marker: false,
            });
        }
    }
}

fn load_replay_metrics(root: &ReplayRoot, notes: &mut Vec<String>) -> Vec<ReplayMetricSample> {
    if !root.metrics_dir.is_dir() {
        notes.push(format!(
            "no metrics directory found at {}",
            root.metrics_dir.display()
        ));
        return Vec::new();
    }

    let mut by_timestamp: BTreeMap<String, ReplayMetricAggregate> = BTreeMap::new();
    load_gpu_metric_rows(
        &root.metrics_dir.join("gpu.jsonl"),
        &mut by_timestamp,
        notes,
    );
    load_slurm_metric_rows(
        &root.metrics_dir.join("slurm.jsonl"),
        &mut by_timestamp,
        notes,
    );

    by_timestamp
        .into_values()
        .filter_map(|aggregate| {
            let line = format_metric_aggregate(&aggregate)?;
            Some(ReplayMetricSample {
                at_unix: aggregate.at_unix,
                attempt: root.attempt,
                line,
            })
        })
        .collect()
}

fn load_gpu_metric_rows(
    path: &Path,
    by_timestamp: &mut BTreeMap<String, ReplayMetricAggregate>,
    notes: &mut Vec<String>,
) {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return,
        Err(err) => {
            notes.push(format!("could not read {}: {err}", path.display()));
            return;
        }
    };
    for (index, raw_line) in raw.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let row: ReplayGpuRow = match serde_json::from_str(line) {
            Ok(row) => row,
            Err(err) => {
                notes.push(format!(
                    "could not parse {} line {}: {err}",
                    path.display(),
                    index + 1
                ));
                continue;
            }
        };
        let Some(at_unix) = parse_scheduler_timestamp(&row.sampled_at) else {
            notes.push(format!(
                "could not parse metrics timestamp '{}' in {} line {}",
                row.sampled_at,
                path.display(),
                index + 1
            ));
            continue;
        };
        let aggregate =
            by_timestamp
                .entry(row.sampled_at)
                .or_insert_with(|| ReplayMetricAggregate {
                    at_unix,
                    ..ReplayMetricAggregate::default()
                });
        aggregate.gpu_count += 1;
        if let Some(util) = parse_u64_metric(row.utilization_gpu.as_deref()) {
            aggregate.gpu_util_sum += util;
            aggregate.gpu_util_count += 1;
        }
        aggregate.memory_used_mib = sum_optional_metric(
            aggregate.memory_used_mib,
            parse_u64_metric(row.memory_used_mib.as_deref()),
        );
        aggregate.memory_total_mib = sum_optional_metric(
            aggregate.memory_total_mib,
            parse_u64_metric(row.memory_total_mib.as_deref()),
        );
    }
}

fn load_slurm_metric_rows(
    path: &Path,
    by_timestamp: &mut BTreeMap<String, ReplayMetricAggregate>,
    notes: &mut Vec<String>,
) {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return,
        Err(err) => {
            notes.push(format!("could not read {}: {err}", path.display()));
            return;
        }
    };
    for (index, raw_line) in raw.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let row: ReplaySlurmRow = match serde_json::from_str(line) {
            Ok(row) => row,
            Err(err) => {
                notes.push(format!(
                    "could not parse {} line {}: {err}",
                    path.display(),
                    index + 1
                ));
                continue;
            }
        };
        let Some(at_unix) = parse_scheduler_timestamp(&row.sampled_at) else {
            notes.push(format!(
                "could not parse metrics timestamp '{}' in {} line {}",
                row.sampled_at,
                path.display(),
                index + 1
            ));
            continue;
        };
        let aggregate =
            by_timestamp
                .entry(row.sampled_at)
                .or_insert_with(|| ReplayMetricAggregate {
                    at_unix,
                    ..ReplayMetricAggregate::default()
                });
        if row.step_id.as_deref().is_some_and(|step| !step.is_empty()) {
            aggregate.slurm_steps += 1;
        }
    }
}

fn parse_u64_metric(value: Option<&str>) -> Option<u64> {
    value?.trim().parse::<u64>().ok()
}

fn sum_optional_metric(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left + right),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn format_metric_aggregate(aggregate: &ReplayMetricAggregate) -> Option<String> {
    let mut parts = Vec::new();
    if aggregate.gpu_count > 0 {
        let util = if aggregate.gpu_util_count > 0 {
            format!(
                "{:.0}%",
                aggregate.gpu_util_sum as f64 / aggregate.gpu_util_count as f64
            )
        } else {
            "-".to_string()
        };
        let mem = match (aggregate.memory_used_mib, aggregate.memory_total_mib) {
            (Some(used), Some(total)) => format!("{used}/{total} MiB"),
            _ => "-".to_string(),
        };
        parts.push(format!(
            "gpu: {} util={} mem={}",
            aggregate.gpu_count, util, mem
        ));
    }
    if aggregate.slurm_steps > 0 {
        parts.push(format!("stats: sampler ({} steps)", aggregate.slurm_steps));
    }
    (!parts.is_empty()).then(|| parts.join(" | "))
}

fn build_replay_events(
    record: &SubmissionRecord,
    roots: &[ReplayRoot],
    root_states: &[(Option<u32>, ServiceRuntimeStateFile)],
    service_names: &[String],
    exits: &[ReplayExit],
    metrics: &[ReplayMetricSample],
    final_state_by_service: &BTreeMap<String, ServiceRuntimeStateEntry>,
) -> Vec<ReplayEvent> {
    let mut events = Vec::new();

    for root in roots {
        let attempt = root
            .attempt
            .or_else(|| root.state.as_ref().and_then(|state| state.attempt));
        let at_unix = attempt_start_time(root, exits, metrics, record.submitted_at);
        events.push(ReplayEvent {
            at_unix,
            attempt,
            kind: ReplayEventKind::AttemptStart,
            service: None,
            exit_code: None,
            detail: Some(format!(
                "attempt {}",
                attempt
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "latest".to_string())
            )),
        });
    }

    for (attempt, state) in root_states {
        for service in &state.services {
            if !service_names.contains(&service.service_name) {
                continue;
            }
            if let Some(started_at) = service.started_at {
                events.push(ReplayEvent {
                    at_unix: started_at,
                    attempt: *attempt,
                    kind: ReplayEventKind::ServiceStart,
                    service: Some(service.service_name.clone()),
                    exit_code: None,
                    detail: Some("started_at from final state.json".to_string()),
                });
            }
        }
    }

    for service in service_names {
        if let Some(state) = final_state_by_service.get(service)
            && state.started_at.is_none()
            && !exits.iter().any(|exit| &exit.service == service)
        {
            events.push(ReplayEvent {
                at_unix: record.submitted_at,
                attempt: None,
                kind: ReplayEventKind::ServiceStart,
                service: Some(service.clone()),
                exit_code: None,
                detail: Some("service start time unavailable; using submission time".to_string()),
            });
        }
    }

    for metric in metrics {
        events.push(ReplayEvent {
            at_unix: metric.at_unix,
            attempt: metric.attempt,
            kind: ReplayEventKind::MetricsSample,
            service: None,
            exit_code: None,
            detail: Some(metric.line.clone()),
        });
    }

    for exit in exits {
        if !service_names.contains(&exit.service) {
            continue;
        }
        events.push(ReplayEvent {
            at_unix: exit.at_unix,
            attempt: exit.attempt,
            kind: ReplayEventKind::ServiceExit,
            service: Some(exit.service.clone()),
            exit_code: Some(exit.exit_code),
            detail: exit
                .detail
                .clone()
                .or_else(|| (!exit.from_marker).then(|| "from final state.json".to_string())),
        });
    }

    let final_at = events
        .iter()
        .map(|event| event.at_unix)
        .max()
        .unwrap_or(record.submitted_at);
    events.push(ReplayEvent {
        at_unix: final_at,
        attempt: root_states.last().and_then(|(_, state)| state.attempt),
        kind: ReplayEventKind::FinalSnapshot,
        service: None,
        exit_code: None,
        detail: Some("final tracked snapshot".to_string()),
    });

    sort_and_dedup_events(events)
}

fn attempt_start_time(
    root: &ReplayRoot,
    exits: &[ReplayExit],
    metrics: &[ReplayMetricSample],
    fallback: u64,
) -> u64 {
    let mut values = Vec::new();
    if let Some(state) = &root.state {
        values.extend(
            state
                .services
                .iter()
                .filter_map(|service| service.started_at),
        );
    }
    values.extend(
        exits
            .iter()
            .filter(|exit| exit.attempt == root.attempt)
            .map(|exit| exit.at_unix),
    );
    values.extend(
        metrics
            .iter()
            .filter(|metric| metric.attempt == root.attempt)
            .map(|metric| metric.at_unix),
    );
    values.into_iter().min().unwrap_or(fallback)
}

fn sort_and_dedup_events(mut events: Vec<ReplayEvent>) -> Vec<ReplayEvent> {
    events.sort_by(|left, right| {
        left.at_unix
            .cmp(&right.at_unix)
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.service.cmp(&right.service))
            .then_with(|| left.exit_code.cmp(&right.exit_code))
            .then_with(|| left.detail.cmp(&right.detail))
    });
    events.dedup();
    events
}

fn build_replay_frames(
    record: &SubmissionRecord,
    service_names: &[String],
    final_state_by_service: &BTreeMap<String, ServiceRuntimeStateEntry>,
    events: &[ReplayEvent],
    exits: &[ReplayExit],
    metrics: &[ReplayMetricSample],
) -> Vec<ReplayFrame> {
    events
        .iter()
        .enumerate()
        .map(|(event_index, event)| {
            let services = service_names
                .iter()
                .map(|service| {
                    build_service_frame(service, event, final_state_by_service.get(service), exits)
                })
                .collect::<Vec<_>>();
            let metrics_line = metrics_line_at(metrics, event.at_unix);
            let fidelity_note = Some("best-effort replay from existing tracked artifacts".into());
            let snapshot = build_frame_snapshot(
                record,
                final_state_by_service,
                event,
                &services,
                fidelity_note.clone(),
            );
            ReplayFrame {
                cursor_unix: event.at_unix,
                event_index,
                event: event.clone(),
                services,
                metrics_line,
                fidelity_note,
                snapshot,
            }
        })
        .collect()
}

fn build_service_frame(
    service: &str,
    event: &ReplayEvent,
    final_state: Option<&ServiceRuntimeStateEntry>,
    exits: &[ReplayExit],
) -> ReplayServiceFrame {
    let started_at = final_state.and_then(|state| state.started_at);
    let service_exits = exits
        .iter()
        .filter(|exit| exit.service == service && exit.at_unix <= event.at_unix)
        .collect::<Vec<_>>();
    let latest_exit = service_exits.last().copied();
    let future_exit_exists = latest_exit.is_some_and(|latest| {
        exits
            .iter()
            .any(|exit| exit.service == service && exit.at_unix > latest.at_unix)
    });
    let current_event_is_exit_for_service =
        event.kind == ReplayEventKind::ServiceExit && event.service.as_deref() == Some(service);
    let status = match latest_exit {
        Some(exit)
            if future_exit_exists && !current_event_is_exit_for_service && exit.exit_code != 0 =>
        {
            "running"
        }
        Some(exit) if exit.exit_code == 0 => "exited",
        Some(_) => "failed",
        None if started_at.is_some_and(|started| started <= event.at_unix) => "running",
        None => "unknown",
    }
    .to_string();
    let finished_at = latest_exit
        .filter(|_| !future_exit_exists || current_event_is_exit_for_service)
        .map(|exit| exit.at_unix)
        .or_else(|| {
            final_state
                .and_then(|state| state.finished_at)
                .filter(|finished| *finished <= event.at_unix)
        });
    let last_exit_code = latest_exit
        .map(|exit| exit.exit_code)
        .or_else(|| final_state.and_then(|state| state.last_exit_code));
    let restart_count = Some(
        service_exits
            .iter()
            .filter(|exit| exit.exit_code != 0)
            .filter(|exit| {
                exits
                    .iter()
                    .any(|future| future.service == service && future.at_unix > exit.at_unix)
            })
            .count() as u32,
    );

    ReplayServiceFrame {
        service_name: service.to_string(),
        status,
        started_at,
        finished_at,
        last_exit_code,
        restart_count,
    }
}

fn build_frame_snapshot(
    record: &SubmissionRecord,
    final_state_by_service: &BTreeMap<String, ServiceRuntimeStateEntry>,
    event: &ReplayEvent,
    services: &[ReplayServiceFrame],
    fidelity_note: Option<String>,
) -> PsSnapshot {
    let now = event.at_unix;
    let rows = services
        .iter()
        .map(|service| {
            let final_state = final_state_by_service.get(&service.service_name);
            let path = record
                .service_logs
                .get(&service.service_name)
                .cloned()
                .or_else(|| final_state.and_then(|state| state.log_path.clone()))
                .unwrap_or_else(|| {
                    log_dir_for_record(record)
                        .join(log_file_name_for_service(&service.service_name))
                });
            let log_status = build_log_status(&path, now);
            PsServiceRow {
                service_name: service.service_name.clone(),
                path: path.clone(),
                present: log_status.present,
                updated_at: log_status.updated_at,
                updated_age_seconds: log_status.updated_age_seconds,
                log_path: final_state
                    .and_then(|state| state.log_path.clone())
                    .or(Some(path)),
                step_name: final_state.and_then(|state| state.step_name.clone()),
                launch_index: final_state.and_then(|state| state.launch_index),
                launcher_pid: None,
                healthy: None,
                completed_successfully: Some(service.status == "exited"),
                readiness_configured: final_state.and_then(|state| state.readiness_configured),
                status: Some(service.status.clone()),
                failure_policy_mode: final_state
                    .and_then(|state| state.failure_policy_mode.clone()),
                restart_count: service.restart_count,
                max_restarts: final_state.and_then(|state| state.max_restarts),
                window_seconds: final_state.and_then(|state| state.window_seconds),
                max_restarts_in_window: final_state.and_then(|state| state.max_restarts_in_window),
                restart_failures_in_window: service.restart_count,
                last_exit_code: service.last_exit_code,
                started_at: service.started_at,
                finished_at: service.finished_at,
                duration_seconds: service
                    .started_at
                    .zip(service.finished_at)
                    .map(|(started, finished)| finished.saturating_sub(started)),
                assertions: None,
                placement_mode: final_state.and_then(|state| state.placement_mode.clone()),
                nodes: final_state.and_then(|state| state.nodes),
                ntasks: final_state.and_then(|state| state.ntasks),
                ntasks_per_node: final_state.and_then(|state| state.ntasks_per_node),
                nodelist: final_state.and_then(|state| state.nodelist.clone()),
            }
        })
        .collect::<Vec<_>>();
    let scheduler = replay_scheduler_status(&rows, fidelity_note);
    PsSnapshot {
        record: record.clone(),
        scheduler,
        queue_diagnostics: None,
        log_dir: log_dir_for_record(record),
        services: rows,
        attempt: event.attempt,
        is_resume: None,
        resume_dir: record.resume_dir.clone(),
    }
}

fn replay_scheduler_status(services: &[PsServiceRow], detail: Option<String>) -> SchedulerStatus {
    let any_running = services
        .iter()
        .any(|service| service.status.as_deref() == Some("running"));
    let any_failed = services
        .iter()
        .any(|service| service.status.as_deref() == Some("failed"));
    let any_known = services
        .iter()
        .any(|service| service.status.as_deref() != Some("unknown"));
    let mut status = if any_running {
        build_scheduler_status("RUNNING".to_string(), SchedulerSource::LocalOnly)
    } else if any_failed {
        build_scheduler_status("FAILED".to_string(), SchedulerSource::LocalOnly)
    } else if any_known {
        build_scheduler_status("COMPLETED".to_string(), SchedulerSource::LocalOnly)
    } else {
        SchedulerStatus {
            state: "UNKNOWN".to_string(),
            source: SchedulerSource::LocalOnly,
            terminal: false,
            failed: false,
            detail: None,
        }
    };
    status.detail = detail;
    status
}

fn metrics_line_at(metrics: &[ReplayMetricSample], cursor_unix: u64) -> Option<String> {
    metrics
        .iter()
        .take_while(|metric| metric.at_unix <= cursor_unix)
        .last()
        .map(|metric| metric.line.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record(root: &Path) -> SubmissionRecord {
        let mut service_logs = BTreeMap::new();
        service_logs.insert(
            "app".to_string(),
            root.join(".hpc-compose/12345/logs/app.log"),
        );
        SubmissionRecord {
            schema_version: SUBMISSION_SCHEMA_VERSION,
            backend: SubmissionBackend::Slurm,
            kind: SubmissionKind::Main,
            job_id: "12345".to_string(),
            submitted_at: 100,
            compose_file: root.join("compose.yaml"),
            submit_dir: root.to_path_buf(),
            script_path: root.join("job.sbatch"),
            cache_dir: root.join("cache"),
            batch_log: root.join("slurm-12345.out"),
            service_logs,
            artifact_export_dir: None,
            resume_dir: None,
            service_name: None,
            command_override: None,
            requested_walltime: None,
            slurm_array: None,
            sweep: None,
            config_snapshot_yaml: None,
            cached_artifacts: Vec::new(),
        }
    }

    fn write_record(_root: &Path, record: &SubmissionRecord) {
        fs::write(
            &record.compose_file,
            "services:\n  app:\n    image: app.sqsh\n",
        )
        .expect("compose");
        write_submission_record(record).expect("record");
    }

    #[test]
    fn replay_parses_exit_markers_and_preserves_malformed_notes() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let record = sample_record(tmpdir.path());
        write_record(tmpdir.path(), &record);
        let exit_dir = runtime_job_root_for_record(&record).join(SERVICE_EXITS_DIR_NAME);
        fs::create_dir_all(&exit_dir).expect("exit dir");
        fs::write(
            exit_dir.join("app.jsonl"),
            "\n{\"service\":\"app\",\"exit_code\":41,\"at_unix\":110}\nnot json\n{\"service\":\"app\",\"exit_code\":0,\"at_unix\":120}\n",
        )
        .expect("exit markers");

        let report = build_replay_report(&record, None).expect("replay");
        let exits = report
            .events
            .iter()
            .filter(|event| event.kind == ReplayEventKind::ServiceExit)
            .collect::<Vec<_>>();
        assert_eq!(exits.len(), 2);
        assert_eq!(exits[0].exit_code, Some(41));
        assert_eq!(exits[1].exit_code, Some(0));
        assert!(
            report
                .notes
                .iter()
                .any(|note| note.contains("could not parse"))
        );
    }

    #[test]
    fn replay_reconstructs_single_service_failure_from_final_state() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let record = sample_record(tmpdir.path());
        write_record(tmpdir.path(), &record);
        let job_root = runtime_job_root_for_record(&record);
        fs::create_dir_all(&job_root).expect("job root");
        fs::write(
            job_root.join("state.json"),
            r#"{"services":[{"service_name":"app","started_at":101,"finished_at":120,"last_exit_code":7}]}"#,
        )
        .expect("state");

        let report = build_replay_report(&record, None).expect("replay");
        let final_frame = report.frames.last().expect("final frame");
        assert_eq!(final_frame.services[0].status, "failed");
        assert_eq!(final_frame.services[0].last_exit_code, Some(7));
        assert!(
            report
                .notes
                .iter()
                .any(|note| note.contains("state.json fallback exit"))
        );
    }

    #[test]
    fn replay_reconstructs_restart_then_success() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let record = sample_record(tmpdir.path());
        write_record(tmpdir.path(), &record);
        let job_root = runtime_job_root_for_record(&record);
        let exit_dir = job_root.join(SERVICE_EXITS_DIR_NAME);
        fs::create_dir_all(&exit_dir).expect("exit dir");
        fs::write(
            job_root.join("state.json"),
            r#"{"services":[{"service_name":"app","started_at":101,"finished_at":130,"last_exit_code":0,"failure_policy_mode":"restart_on_failure"}]}"#,
        )
        .expect("state");
        fs::write(
            exit_dir.join("app.jsonl"),
            "{\"service\":\"app\",\"exit_code\":41,\"at_unix\":110}\n{\"service\":\"app\",\"exit_code\":0,\"at_unix\":130}\n",
        )
        .expect("markers");

        let report = build_replay_report(&record, None).expect("replay");
        let failed_frame = report
            .frames
            .iter()
            .find(|frame| {
                frame.event.kind == ReplayEventKind::ServiceExit
                    && frame.event.exit_code == Some(41)
            })
            .expect("failed frame");
        assert_eq!(failed_frame.services[0].status, "failed");
        let final_frame = report.frames.last().expect("final frame");
        assert_eq!(final_frame.services[0].status, "exited");
        assert_eq!(final_frame.services[0].restart_count, Some(1));
    }

    #[test]
    fn replay_prefers_numeric_attempt_roots() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let record = sample_record(tmpdir.path());
        write_record(tmpdir.path(), &record);
        let job_root = runtime_job_root_for_record(&record);
        fs::create_dir_all(job_root.join("attempts/1")).expect("attempt 1");
        fs::create_dir_all(job_root.join("attempts/2")).expect("attempt 2");
        fs::write(
            job_root.join("attempts/2/state.json"),
            r#"{"attempt":2,"services":[{"service_name":"app","started_at":210,"finished_at":220,"last_exit_code":0}]}"#,
        )
        .expect("state 2");
        fs::write(
            job_root.join("state.json"),
            r#"{"services":[{"service_name":"app","started_at":999,"finished_at":1000,"last_exit_code":1}]}"#,
        )
        .expect("latest state");

        let report = build_replay_report(&record, None).expect("replay");
        assert!(
            report
                .events
                .iter()
                .any(|event| event.attempt == Some(2) && event.at_unix == 210)
        );
        assert!(!report.events.iter().any(|event| event.at_unix == 999));
    }

    #[test]
    fn replay_uses_nearest_prior_metrics_only() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let record = sample_record(tmpdir.path());
        write_record(tmpdir.path(), &record);
        let job_root = runtime_job_root_for_record(&record);
        let metrics_dir = job_root.join("metrics");
        fs::create_dir_all(&metrics_dir).expect("metrics");
        fs::write(
            metrics_dir.join("gpu.jsonl"),
            "{\"sampled_at\":\"1970-01-01T00:02:00Z\",\"utilization_gpu\":\"90\",\"memory_used_mib\":\"4\",\"memory_total_mib\":\"8\"}\n",
        )
        .expect("gpu metrics");
        fs::write(
            job_root.join("state.json"),
            r#"{"services":[{"service_name":"app","started_at":100,"finished_at":130,"last_exit_code":0}]}"#,
        )
        .expect("state");

        let report = build_replay_report(&record, None).expect("replay");
        let early = report
            .frames
            .iter()
            .find(|frame| frame.cursor_unix == 100)
            .expect("early frame");
        assert_eq!(early.metrics_line, None);
        let metric_frame = report
            .frames
            .iter()
            .find(|frame| frame.cursor_unix == 120)
            .expect("metric frame");
        assert!(
            metric_frame
                .metrics_line
                .as_deref()
                .is_some_and(|line| line.contains("gpu: 1"))
        );
    }

    #[test]
    fn replay_clamps_cursor_to_bounds() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let record = sample_record(tmpdir.path());
        write_record(tmpdir.path(), &record);
        let job_root = runtime_job_root_for_record(&record);
        fs::create_dir_all(&job_root).expect("job root");
        fs::write(
            job_root.join("state.json"),
            r#"{"services":[{"service_name":"app","started_at":101,"finished_at":120,"last_exit_code":0}]}"#,
        )
        .expect("state");

        let report = build_replay_report(&record, None).expect("replay");
        assert_eq!(report.clamp_cursor(0), report.timeline_start_unix.unwrap());
        assert_eq!(report.clamp_cursor(999), report.timeline_end_unix.unwrap());
    }
}
