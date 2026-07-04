use std::collections::BTreeSet;

use serde::Serialize;
use serde_json::Value;

use super::*;

/// Compact comparison between two tracked job submissions.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct JobDiffReport {
    pub left: JobDiffSide,
    pub right: JobDiffSide,
    pub outcome_changes: Vec<JobDiffChange>,
    pub provenance_changes: Vec<JobDiffChange>,
    pub resource_changes: Vec<JobDiffChange>,
    pub config_changes: Vec<JobDiffChange>,
    pub notes: Vec<String>,
}

/// One side of a tracked job diff.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct JobDiffSide {
    pub job_id: String,
    pub submitted_at: u64,
    pub compose_file: PathBuf,
    pub backend: SubmissionBackend,
    pub kind: SubmissionKind,
    pub scheduler_state: Option<String>,
    pub scheduler_failed: Option<bool>,
    pub first_failure: Option<FirstFailure>,
    pub services: Vec<JobDiffServiceStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<JobProvenance>,
}

/// Service status projected into a diff report.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JobDiffServiceStatus {
    pub service_name: String,
    pub status: Option<String>,
    pub last_exit_code: Option<i32>,
}

/// One value-level difference.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JobDiffChange {
    pub path: String,
    pub left: Option<String>,
    pub right: Option<String>,
}

/// N-way comparison matrix across several tracked job submissions.
///
/// One column per run (positionally aligned to [`Self::runs`]) and one row per
/// field that differs in at least one run; fields identical across every run are
/// collapsed (omitted).
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JobMatrixReport {
    pub runs: Vec<JobMatrixRun>,
    pub rows: Vec<JobMatrixRow>,
    pub notes: Vec<String>,
}

/// Metadata for one column (run) of a [`JobMatrixReport`].
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JobMatrixRun {
    pub job_id: String,
    pub submitted_at: u64,
    pub compose_file: PathBuf,
    pub backend: SubmissionBackend,
    pub kind: SubmissionKind,
    pub scheduler_state: Option<String>,
    pub scheduler_failed: Option<bool>,
}

/// One differing field projected across every run; `values` is positionally
/// aligned to [`JobMatrixReport::runs`] (one cell per run, `None` when absent).
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JobMatrixRow {
    pub section: String,
    pub path: String,
    pub values: Vec<Option<String>>,
}

const RESOURCE_FIELDS: &[&[&str]] = &[
    &["runtime", "backend"],
    &["x-slurm", "resources"],
    &["x-slurm", "partition"],
    &["x-slurm", "account"],
    &["x-slurm", "qos"],
    &["x-slurm", "reservation"],
    &["x-slurm", "licenses"],
    &["x-slurm", "time"],
    &["x-slurm", "nodes"],
    &["x-slurm", "ntasks"],
    &["x-slurm", "ntasks_per_node"],
    &["x-slurm", "cpus_per_task"],
    &["x-slurm", "mem"],
    &["x-slurm", "gres"],
    &["x-slurm", "gpus"],
    &["x-slurm", "gpus_per_node"],
    &["x-slurm", "gpus_per_task"],
    &["x-slurm", "cpus_per_gpu"],
    &["x-slurm", "mem_per_gpu"],
    &["x-slurm", "constraint"],
    &["x-slurm", "requeue"],
    &["x-slurm", "signal"],
];

const SERVICE_RESOURCE_FIELDS: &[&[&str]] = &[
    &["image"],
    &["command"],
    &["x-slurm", "nodes"],
    &["x-slurm", "ntasks"],
    &["x-slurm", "ntasks_per_node"],
    &["x-slurm", "cpus_per_task"],
    &["x-slurm", "mem"],
    &["x-slurm", "gres"],
    &["x-slurm", "gpus"],
    &["x-slurm", "gpus_per_node"],
    &["x-slurm", "gpus_per_task"],
    &["x-slurm", "cpus_per_gpu"],
    &["x-slurm", "mem_per_gpu"],
    &["x-slurm", "placement"],
];

/// Batches the raw scheduler probe over the Slurm-backed records in `records`
/// (one squeue + one gated sacct total). Local records probe no scheduler, so
/// they are excluded; the returned map is keyed by job id.
fn batch_scheduler_probes(
    records: &[&SubmissionRecord],
    options: &SchedulerOptions,
) -> BTreeMap<String, (SchedulerStatus, Option<QueueDiagnostics>)> {
    let job_ids = records
        .iter()
        .filter(|record| record.backend == SubmissionBackend::Slurm)
        .map(|record| record.job_id.as_str())
        .collect::<Vec<_>>();
    probe_scheduler_status_many(&job_ids, options)
}

/// Builds a compact diff report for two tracked jobs.
pub fn build_job_diff_report(
    left: &SubmissionRecord,
    right: &SubmissionRecord,
    options: &SchedulerOptions,
) -> JobDiffReport {
    let mut notes = Vec::new();
    // One squeue + (when needed) one sacct for both jobs instead of a probe pair
    // per side.
    let probes = batch_scheduler_probes(&[left, right], options);
    let left_status = match build_status_snapshot_with_status(
        &left.compose_file,
        Some(&left.job_id),
        options,
        probes.get(&left.job_id).cloned(),
    ) {
        Ok(snapshot) => Some(snapshot),
        Err(err) => {
            notes.push(format!("status unavailable for job {}: {err}", left.job_id));
            None
        }
    };
    let right_status = match build_status_snapshot_with_status(
        &right.compose_file,
        Some(&right.job_id),
        options,
        probes.get(&right.job_id).cloned(),
    ) {
        Ok(snapshot) => Some(snapshot),
        Err(err) => {
            notes.push(format!(
                "status unavailable for job {}: {err}",
                right.job_id
            ));
            None
        }
    };
    let left_side = diff_side(left, left_status.as_ref());
    let right_side = diff_side(right, right_status.as_ref());
    let outcome_changes = outcome_changes(&left_side, &right_side);
    let (left_config, right_config) = parse_config_snapshots(left, right, &mut notes);
    let resource_changes = match (left_config.as_ref(), right_config.as_ref()) {
        (Some(left_config), Some(right_config)) => resource_changes(left_config, right_config),
        _ => Vec::new(),
    };
    let config_changes = match (left_config.as_ref(), right_config.as_ref()) {
        (Some(left_config), Some(right_config)) => {
            let mut changes = Vec::new();
            diff_json_values("", left_config, right_config, &mut changes);
            changes
        }
        _ => Vec::new(),
    };

    let provenance_changes = provenance_changes(left, right);
    if provenance_changes
        .iter()
        .any(|change| change.path == "provenance.git.sha")
    {
        notes.push("jobs were built from different commits".to_string());
    }

    JobDiffReport {
        left: left_side,
        right: right_side,
        outcome_changes,
        provenance_changes,
        resource_changes,
        config_changes,
        notes,
    }
}

/// Builds an N-way comparison matrix across several tracked job submissions.
///
/// This is a pure projection over already-persisted records (the same surface
/// the pairwise [`build_job_diff_report`] reads) and reuses its field-derivation
/// primitives. Rows are emitted only for fields that differ in at least one run;
/// fields identical across every run are collapsed. Cells are positionally
/// aligned to the returned `runs`.
pub fn build_job_matrix_report(
    records: &[SubmissionRecord],
    options: &SchedulerOptions,
) -> JobMatrixReport {
    let mut notes = Vec::new();

    // Project each record into the same per-side view the pairwise diff uses,
    // and parse its config snapshot once. Both reuse the existing primitives.
    // One squeue + (when needed) one sacct across every run instead of a probe
    // pair per record.
    let probes = batch_scheduler_probes(&records.iter().collect::<Vec<_>>(), options);
    let mut sides = Vec::with_capacity(records.len());
    let mut configs = Vec::with_capacity(records.len());
    for record in records {
        let status = match build_status_snapshot_with_status(
            &record.compose_file,
            Some(&record.job_id),
            options,
            probes.get(&record.job_id).cloned(),
        ) {
            Ok(snapshot) => Some(snapshot),
            Err(err) => {
                notes.push(format!(
                    "status unavailable for job {}: {err}",
                    record.job_id
                ));
                None
            }
        };
        sides.push(diff_side(record, status.as_ref()));
        configs.push(parse_config_snapshot(record, &mut notes));
    }

    let mut rows = Vec::new();
    matrix_outcome_rows(&sides, &mut rows);
    matrix_provenance_rows(records, &mut rows);
    matrix_resource_rows(&configs, &mut rows);
    matrix_config_rows(&configs, &mut rows);

    if rows
        .iter()
        .any(|row| row.path == "provenance.git.sha" && row.section == "provenance")
    {
        notes.push("jobs were built from different commits".to_string());
    }

    JobMatrixReport {
        runs: sides.iter().map(matrix_run_from_side).collect(),
        rows,
        notes,
    }
}

fn matrix_run_from_side(side: &JobDiffSide) -> JobMatrixRun {
    JobMatrixRun {
        job_id: side.job_id.clone(),
        submitted_at: side.submitted_at,
        compose_file: side.compose_file.clone(),
        backend: side.backend,
        kind: side.kind,
        scheduler_state: side.scheduler_state.clone(),
        scheduler_failed: side.scheduler_failed,
    }
}

/// Returns `true` when every cell holds the same value (so the row is dropped).
fn collapse_identical(values: &[Option<String>]) -> bool {
    values
        .first()
        .is_none_or(|first| values.iter().all(|value| value == first))
}

/// Appends a row for the field unless all cells are identical.
fn push_matrix_row_if_different(
    rows: &mut Vec<JobMatrixRow>,
    section: &str,
    path: String,
    values: Vec<Option<String>>,
) {
    if collapse_identical(&values) {
        return;
    }
    rows.push(JobMatrixRow {
        section: section.to_string(),
        path,
        values,
    });
}

/// Outcome rows mirror [`outcome_changes`]: scheduler.state/failed,
/// first_failure.service/exit_code, and per-service status/last_exit_code.
fn matrix_outcome_rows(sides: &[JobDiffSide], rows: &mut Vec<JobMatrixRow>) {
    push_matrix_row_if_different(
        rows,
        "outcome",
        "scheduler.state".to_string(),
        sides
            .iter()
            .map(|side| side.scheduler_state.clone())
            .collect(),
    );
    push_matrix_row_if_different(
        rows,
        "outcome",
        "scheduler.failed".to_string(),
        sides
            .iter()
            .map(|side| side.scheduler_failed.map(|value| value.to_string()))
            .collect(),
    );
    push_matrix_row_if_different(
        rows,
        "outcome",
        "first_failure.service".to_string(),
        sides
            .iter()
            .map(|side| {
                side.first_failure
                    .as_ref()
                    .map(|failure| failure.service.clone())
            })
            .collect(),
    );
    push_matrix_row_if_different(
        rows,
        "outcome",
        "first_failure.exit_code".to_string(),
        sides
            .iter()
            .map(|side| {
                side.first_failure
                    .as_ref()
                    .map(|failure| failure.exit_code.to_string())
            })
            .collect(),
    );

    let service_names = sides
        .iter()
        .flat_map(|side| {
            side.services
                .iter()
                .map(|service| service.service_name.clone())
        })
        .collect::<BTreeSet<_>>();
    for service_name in service_names {
        let find = |side: &JobDiffSide| {
            side.services
                .iter()
                .find(|service| service.service_name == service_name)
                .cloned()
        };
        push_matrix_row_if_different(
            rows,
            "outcome",
            format!("services.{service_name}.status"),
            sides
                .iter()
                .map(|side| find(side).and_then(|service| service.status))
                .collect(),
        );
        push_matrix_row_if_different(
            rows,
            "outcome",
            format!("services.{service_name}.last_exit_code"),
            sides
                .iter()
                .map(|side| {
                    find(side)
                        .and_then(|service| service.last_exit_code)
                        .map(|value| value.to_string())
                })
                .collect(),
        );
    }
}

/// Provenance rows mirror [`provenance_changes`]: tool version, git sha/dirty/
/// branch, and per-service image refs.
fn matrix_provenance_rows(records: &[SubmissionRecord], rows: &mut Vec<JobMatrixRow>) {
    let provenances: Vec<Option<&JobProvenance>> = records
        .iter()
        .map(|record| record.provenance.as_ref())
        .collect();
    push_matrix_row_if_different(
        rows,
        "provenance",
        "provenance.tool_version".to_string(),
        provenances
            .iter()
            .map(|prov| prov.map(|prov| prov.tool_version.clone()))
            .collect(),
    );
    push_matrix_row_if_different(
        rows,
        "provenance",
        "provenance.git.sha".to_string(),
        provenances
            .iter()
            .map(|prov| {
                prov.and_then(|prov| prov.git.as_ref())
                    .map(|git| git.sha.clone())
            })
            .collect(),
    );
    push_matrix_row_if_different(
        rows,
        "provenance",
        "provenance.git.dirty".to_string(),
        provenances
            .iter()
            .map(|prov| {
                prov.and_then(|prov| prov.git.as_ref())
                    .map(|git| git.dirty.to_string())
            })
            .collect(),
    );
    push_matrix_row_if_different(
        rows,
        "provenance",
        "provenance.git.branch".to_string(),
        provenances
            .iter()
            .map(|prov| {
                prov.and_then(|prov| prov.git.as_ref())
                    .and_then(|git| git.branch.clone())
            })
            .collect(),
    );
    let services = provenances
        .iter()
        .flatten()
        .flat_map(|prov| prov.image_refs.keys().cloned())
        .collect::<BTreeSet<_>>();
    for service in services {
        push_matrix_row_if_different(
            rows,
            "provenance",
            format!("provenance.image_refs.{service}"),
            provenances
                .iter()
                .map(|prov| prov.and_then(|prov| prov.image_refs.get(&service).cloned()))
                .collect(),
        );
    }
}

/// Resource rows reuse [`RESOURCE_FIELDS`]/[`SERVICE_RESOURCE_FIELDS`] projected
/// across each record's parsed config snapshot. Records whose config snapshot
/// could not be parsed contribute `None` cells.
fn matrix_resource_rows(configs: &[Option<Value>], rows: &mut Vec<JobMatrixRow>) {
    for path in RESOURCE_FIELDS {
        push_matrix_row_if_different(
            rows,
            "resources",
            path.join("."),
            project_path_across(configs, path),
        );
    }
    let service_names = configs
        .iter()
        .flatten()
        .flat_map(service_names)
        .collect::<BTreeSet<_>>();
    for service_name in service_names {
        for field_path in SERVICE_RESOURCE_FIELDS {
            let mut full_path = vec!["services", service_name.as_str()];
            full_path.extend(field_path.iter().copied());
            push_matrix_row_if_different(
                rows,
                "resources",
                full_path.join("."),
                project_path_across(configs, &full_path),
            );
        }
    }
}

/// Config rows enumerate the union of dotted config paths that differ across any
/// pair of records (reusing [`diff_json_values`] for path derivation, matching
/// the pairwise config-change keying), then project each path across all runs.
fn matrix_config_rows(configs: &[Option<Value>], rows: &mut Vec<JobMatrixRow>) {
    let mut candidate_paths: BTreeSet<String> = BTreeSet::new();
    for index in 0..configs.len() {
        for other in (index + 1)..configs.len() {
            if let (Some(left), Some(right)) = (configs[index].as_ref(), configs[other].as_ref()) {
                let mut changes = Vec::new();
                diff_json_values("", left, right, &mut changes);
                candidate_paths.extend(changes.into_iter().map(|change| change.path));
            }
        }
    }
    for path in candidate_paths {
        let segments: Vec<&str> = path.split('.').collect();
        push_matrix_row_if_different(
            rows,
            "config",
            path.clone(),
            project_path_across(configs, &segments),
        );
    }
}

/// Projects a JSON path across each record's optional config, producing one cell
/// per record (`None` when the config is missing or the path is absent).
fn project_path_across(configs: &[Option<Value>], path: &[&str]) -> Vec<Option<String>> {
    configs
        .iter()
        .map(|config| {
            config
                .as_ref()
                .and_then(|config| get_path(config, path))
                .map(value_label)
        })
        .collect()
}

/// Builds value-level differences between the two records' pinned provenance:
/// tool version, git sha/dirty/branch, and per-service image refs.
fn provenance_changes(left: &SubmissionRecord, right: &SubmissionRecord) -> Vec<JobDiffChange> {
    let mut changes = Vec::new();
    let left_prov = left.provenance.as_ref();
    let right_prov = right.provenance.as_ref();
    push_change_if_different(
        &mut changes,
        "provenance.tool_version",
        left_prov.map(|prov| prov.tool_version.clone()),
        right_prov.map(|prov| prov.tool_version.clone()),
    );
    push_change_if_different(
        &mut changes,
        "provenance.git.sha",
        left_prov
            .and_then(|prov| prov.git.as_ref())
            .map(|git| git.sha.clone()),
        right_prov
            .and_then(|prov| prov.git.as_ref())
            .map(|git| git.sha.clone()),
    );
    push_change_if_different(
        &mut changes,
        "provenance.git.dirty",
        left_prov
            .and_then(|prov| prov.git.as_ref())
            .map(|git| git.dirty.to_string()),
        right_prov
            .and_then(|prov| prov.git.as_ref())
            .map(|git| git.dirty.to_string()),
    );
    push_change_if_different(
        &mut changes,
        "provenance.git.branch",
        left_prov
            .and_then(|prov| prov.git.as_ref())
            .and_then(|git| git.branch.clone()),
        right_prov
            .and_then(|prov| prov.git.as_ref())
            .and_then(|git| git.branch.clone()),
    );
    push_change_if_different(
        &mut changes,
        "provenance.source_content_hash",
        left_prov.and_then(|prov| prov.source_content_hash.clone()),
        right_prov.and_then(|prov| prov.source_content_hash.clone()),
    );
    let mut services: BTreeSet<&str> = BTreeSet::new();
    if let Some(prov) = left_prov {
        services.extend(prov.image_refs.keys().map(String::as_str));
    }
    if let Some(prov) = right_prov {
        services.extend(prov.image_refs.keys().map(String::as_str));
    }
    for service in services {
        push_change_if_different(
            &mut changes,
            &format!("provenance.image_refs.{service}"),
            left_prov.and_then(|prov| prov.image_refs.get(service).cloned()),
            right_prov.and_then(|prov| prov.image_refs.get(service).cloned()),
        );
    }
    changes
}

fn diff_side(record: &SubmissionRecord, status: Option<&StatusSnapshot>) -> JobDiffSide {
    let first_failure = status.and_then(|status| first_failure_from_services(&status.services));
    let services = status
        .map(|status| {
            status
                .services
                .iter()
                .map(|service| JobDiffServiceStatus {
                    service_name: service.service_name.clone(),
                    status: service.status.clone(),
                    last_exit_code: service.last_exit_code,
                })
                .collect()
        })
        .unwrap_or_default();
    JobDiffSide {
        job_id: record.job_id.clone(),
        submitted_at: record.submitted_at,
        compose_file: record.compose_file.clone(),
        backend: record.backend,
        kind: record.kind,
        scheduler_state: status.map(|status| status.scheduler.state.clone()),
        scheduler_failed: status.map(|status| status.scheduler.failed),
        first_failure,
        services,
        provenance: record.provenance.clone(),
    }
}

fn first_failure_from_services(services: &[PsServiceRow]) -> Option<FirstFailure> {
    services.iter().find_map(|service| {
        Some(FirstFailure {
            service: service.service_name.clone(),
            exit_code: service.last_exit_code?,
            at_unix: None,
            node: None,
            rank: None,
        })
        .filter(|failure| failure.exit_code != 0)
    })
}

fn outcome_changes(left: &JobDiffSide, right: &JobDiffSide) -> Vec<JobDiffChange> {
    let mut changes = Vec::new();
    push_change_if_different(
        &mut changes,
        "scheduler.state",
        left.scheduler_state.clone(),
        right.scheduler_state.clone(),
    );
    push_change_if_different(
        &mut changes,
        "scheduler.failed",
        left.scheduler_failed.map(|value| value.to_string()),
        right.scheduler_failed.map(|value| value.to_string()),
    );
    push_change_if_different(
        &mut changes,
        "first_failure.service",
        left.first_failure
            .as_ref()
            .map(|failure| failure.service.clone()),
        right
            .first_failure
            .as_ref()
            .map(|failure| failure.service.clone()),
    );
    push_change_if_different(
        &mut changes,
        "first_failure.exit_code",
        left.first_failure
            .as_ref()
            .map(|failure| failure.exit_code.to_string()),
        right
            .first_failure
            .as_ref()
            .map(|failure| failure.exit_code.to_string()),
    );

    let service_names = left
        .services
        .iter()
        .map(|service| service.service_name.clone())
        .chain(
            right
                .services
                .iter()
                .map(|service| service.service_name.clone()),
        )
        .collect::<BTreeSet<_>>();
    for service_name in service_names {
        let left_service = left
            .services
            .iter()
            .find(|service| service.service_name == service_name);
        let right_service = right
            .services
            .iter()
            .find(|service| service.service_name == service_name);
        push_change_if_different(
            &mut changes,
            &format!("services.{service_name}.status"),
            left_service.and_then(|service| service.status.clone()),
            right_service.and_then(|service| service.status.clone()),
        );
        push_change_if_different(
            &mut changes,
            &format!("services.{service_name}.last_exit_code"),
            left_service
                .and_then(|service| service.last_exit_code)
                .map(|value| value.to_string()),
            right_service
                .and_then(|service| service.last_exit_code)
                .map(|value| value.to_string()),
        );
    }
    changes
}

fn parse_config_snapshots(
    left: &SubmissionRecord,
    right: &SubmissionRecord,
    notes: &mut Vec<String>,
) -> (Option<Value>, Option<Value>) {
    let left_config = parse_config_snapshot(left, notes);
    let right_config = parse_config_snapshot(right, notes);
    (left_config, right_config)
}

fn parse_config_snapshot(record: &SubmissionRecord, notes: &mut Vec<String>) -> Option<Value> {
    let Some(raw) = record.config_snapshot_yaml.as_deref() else {
        notes.push(format!(
            "config snapshot unavailable for job {}",
            record.job_id
        ));
        return None;
    };
    match serde_norway::from_str::<Value>(raw) {
        Ok(value) => Some(value),
        Err(err) => {
            notes.push(format!(
                "config snapshot for job {} could not be parsed: {err}",
                record.job_id
            ));
            None
        }
    }
}

fn resource_changes(left: &Value, right: &Value) -> Vec<JobDiffChange> {
    let mut changes = Vec::new();
    for path in RESOURCE_FIELDS {
        push_value_change_if_different(
            &mut changes,
            &path.join("."),
            get_path(left, path),
            get_path(right, path),
        );
    }

    let service_names = service_names(left)
        .into_iter()
        .chain(service_names(right))
        .collect::<BTreeSet<_>>();
    for service_name in service_names {
        for field_path in SERVICE_RESOURCE_FIELDS {
            let mut full_path = vec!["services", service_name.as_str()];
            full_path.extend(field_path.iter().copied());
            push_value_change_if_different(
                &mut changes,
                &full_path.join("."),
                get_path(left, &full_path),
                get_path(right, &full_path),
            );
        }
    }
    changes
}

fn service_names(value: &Value) -> BTreeSet<String> {
    value
        .get("services")
        .and_then(Value::as_object)
        .map(|services| services.keys().cloned().collect())
        .unwrap_or_default()
}

fn diff_json_values(path: &str, left: &Value, right: &Value, changes: &mut Vec<JobDiffChange>) {
    if left == right {
        return;
    }
    match (left, right) {
        (Value::Object(left_map), Value::Object(right_map)) => {
            let keys = left_map
                .keys()
                .chain(right_map.keys())
                .cloned()
                .collect::<BTreeSet<_>>();
            for key in keys {
                let child_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                match (left_map.get(&key), right_map.get(&key)) {
                    (Some(left_child), Some(right_child)) => {
                        diff_json_values(&child_path, left_child, right_child, changes);
                    }
                    (Some(left_child), None) => changes.push(JobDiffChange {
                        path: child_path,
                        left: Some(value_label(left_child)),
                        right: None,
                    }),
                    (None, Some(right_child)) => changes.push(JobDiffChange {
                        path: child_path,
                        left: None,
                        right: Some(value_label(right_child)),
                    }),
                    (None, None) => {}
                }
            }
        }
        _ => changes.push(JobDiffChange {
            path: path.to_string(),
            left: Some(value_label(left)),
            right: Some(value_label(right)),
        }),
    }
}

fn get_path<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn push_value_change_if_different(
    changes: &mut Vec<JobDiffChange>,
    path: &str,
    left: Option<&Value>,
    right: Option<&Value>,
) {
    if left == right {
        return;
    }
    changes.push(JobDiffChange {
        path: path.to_string(),
        left: left.map(value_label),
        right: right.map(value_label),
    });
}

fn push_change_if_different(
    changes: &mut Vec<JobDiffChange>,
    path: &str,
    left: Option<String>,
    right: Option<String>,
) {
    if left == right {
        return;
    }
    changes.push(JobDiffChange {
        path: path.to_string(),
        left,
        right,
    });
}

fn value_label(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(_) | Value::Object(_) => {
            serde_json::to_string(value).unwrap_or_else(|_| "<unprintable>".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local_diff_record(
        root: &Path,
        job_id: &str,
        config_snapshot_yaml: &str,
    ) -> SubmissionRecord {
        let mut service_logs = BTreeMap::new();
        service_logs.insert(
            "app".to_string(),
            root.join(format!(".hpc-compose/{job_id}/logs/app.log")),
        );
        SubmissionRecord {
            schema_version: SUBMISSION_SCHEMA_VERSION,
            backend: SubmissionBackend::Local,
            kind: SubmissionKind::Main,
            job_id: job_id.to_string(),
            submitted_at: 100,
            compose_file: root.join("compose.yaml"),
            submit_dir: root.to_path_buf(),
            script_path: root.join(format!("{job_id}.sbatch")),
            cache_dir: root.join("cache"),
            runtime_root: None,
            batch_log: root.join(format!("job-{job_id}.out")),
            batch_log_managed: false,
            service_logs,
            artifact_export_dir: None,
            resume_dir: None,
            service_name: None,
            command_override: None,
            requested_walltime: None,
            slurm_array: None,
            sweep: None,
            config_snapshot_yaml: Some(config_snapshot_yaml.to_string()),
            cached_artifacts: Vec::new(),
            provenance: None,
        }
    }

    fn write_local_diff_record(record: &SubmissionRecord, state_json: &str) {
        fs::write(
            &record.compose_file,
            "services:\n  app:\n    image: app.sqsh\n",
        )
        .expect("compose");
        write_submission_record(record).expect("record");
        let state_path = state_path_for_record(record);
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent).expect("state dir");
        }
        fs::write(state_path, state_json).expect("state");
    }

    #[test]
    fn provenance_changes_reports_tool_git_and_image_deltas() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut left = local_diff_record(tmp.path(), "1", "name: x");
        let mut right = local_diff_record(tmp.path(), "2", "name: x");
        left.provenance = Some(JobProvenance {
            tool_version: "0.1.0".to_string(),
            git: Some(GitProvenance {
                sha: "aaa".to_string(),
                dirty: false,
                branch: Some("main".to_string()),
            }),
            image_refs: [("app".to_string(), "img:1".to_string())]
                .into_iter()
                .collect(),
            source_content_hash: None,
        });
        right.provenance = Some(JobProvenance {
            tool_version: "0.2.0".to_string(),
            git: Some(GitProvenance {
                sha: "bbb".to_string(),
                dirty: true,
                branch: Some("main".to_string()),
            }),
            image_refs: [("app".to_string(), "img:2".to_string())]
                .into_iter()
                .collect(),
            source_content_hash: None,
        });
        let changes = provenance_changes(&left, &right);
        let paths: Vec<&str> = changes.iter().map(|change| change.path.as_str()).collect();
        assert!(paths.contains(&"provenance.tool_version"));
        assert!(paths.contains(&"provenance.git.sha"));
        assert!(paths.contains(&"provenance.git.dirty"));
        assert!(paths.contains(&"provenance.image_refs.app"));
        // An identical field (branch) is not reported.
        assert!(!paths.contains(&"provenance.git.branch"));

        // Identical provenance yields no changes; one-sided Some/None does.
        right.provenance = left.provenance.clone();
        assert!(provenance_changes(&left, &right).is_empty());
        right.provenance = None;
        assert!(
            provenance_changes(&left, &right)
                .iter()
                .any(|change| change.path == "provenance.tool_version")
        );
    }

    #[test]
    fn diff_notes_malformed_config_snapshots_but_preserves_outcome_changes() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let left = local_diff_record(tmpdir.path(), "111", "x-slurm: [not valid");
        let right = local_diff_record(
            tmpdir.path(),
            "222",
            "x-slurm:\n  time: 00:20:00\nservices:\n  app:\n    image: app.sqsh\n",
        );
        write_local_diff_record(
            &left,
            r#"{"backend":"local","job_status":"COMPLETED","job_exit_code":0,"services":[{"service_name":"app","completed_successfully":true,"last_exit_code":0}]}"#,
        );
        write_local_diff_record(
            &right,
            r#"{"backend":"local","job_status":"FAILED","job_exit_code":7,"services":[{"service_name":"app","last_exit_code":7}]}"#,
        );

        let report = build_job_diff_report(&left, &right, &SchedulerOptions::default());
        assert!(
            report
                .notes
                .iter()
                .any(|note| note.contains("config snapshot for job 111 could not be parsed"))
        );
        assert!(
            report
                .outcome_changes
                .iter()
                .any(|change| change.path == "scheduler.state")
        );
        assert!(
            report
                .outcome_changes
                .iter()
                .any(|change| change.path == "services.app.last_exit_code")
        );
    }

    #[test]
    fn diff_json_values_reports_nested_changes() {
        let left = serde_json::json!({"x-slurm": {"time": "00:10:00"}, "services": {"app": {"image": "a"}}});
        let right = serde_json::json!({"x-slurm": {"time": "00:20:00"}, "services": {"app": {"image": "b"}}});
        let changes = resource_changes(&left, &right);
        assert!(changes.iter().any(|change| change.path == "x-slurm.time"));
        assert!(
            changes
                .iter()
                .any(|change| change.path == "services.app.image")
        );
    }

    #[test]
    fn diff_resource_changes_reports_reservation_and_licenses() {
        let left = serde_json::json!({
            "x-slurm": {"reservation": "maint_2026", "licenses": "ansys:2"}
        });
        let right = serde_json::json!({
            "x-slurm": {"reservation": "maint_2027", "licenses": "ansys:4,comsol:1"}
        });
        let changes = resource_changes(&left, &right);
        assert!(changes.iter().any(|change| {
            change.path == "x-slurm.reservation"
                && change.left.as_deref() == Some("maint_2026")
                && change.right.as_deref() == Some("maint_2027")
        }));
        assert!(changes.iter().any(|change| {
            change.path == "x-slurm.licenses"
                && change.left.as_deref() == Some("ansys:2")
                && change.right.as_deref() == Some("ansys:4,comsol:1")
        }));
    }

    #[test]
    fn diff_resource_changes_reports_requeue_and_signal_updates() {
        let left = serde_json::json!({
            "x-slurm": {"requeue": false, "signal": {"name": "USR1", "at_seconds": 60}}
        });
        let right = serde_json::json!({
            "x-slurm": {"requeue": true, "signal": {"name": "USR1", "at_seconds": 120}}
        });
        let changes = resource_changes(&left, &right);
        assert!(changes.iter().any(|change| {
            change.path == "x-slurm.requeue"
                && change.left.as_deref() == Some("false")
                && change.right.as_deref() == Some("true")
        }));
        assert!(changes.iter().any(|change| change.path == "x-slurm.signal"));
    }

    #[test]
    fn diff_resource_changes_reports_added_and_removed_services() {
        let left = serde_json::json!({
            "services": {
                "app": {"image": "app.sqsh"},
                "sidecar": {"image": "sidecar.sqsh"}
            }
        });
        let right = serde_json::json!({
            "services": {
                "app": {"image": "app.sqsh"},
                "worker": {"image": "worker.sqsh"}
            }
        });

        let changes = resource_changes(&left, &right);
        assert!(changes.iter().any(|change| {
            change.path == "services.worker.image"
                && change.left.is_none()
                && change.right.as_deref() == Some("worker.sqsh")
        }));
        assert!(changes.iter().any(|change| {
            change.path == "services.sidecar.image"
                && change.left.as_deref() == Some("sidecar.sqsh")
                && change.right.is_none()
        }));
    }

    #[test]
    fn diff_status_unavailable_still_reports_snapshot_resource_changes() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let mut left = local_diff_record(
            tmpdir.path(),
            "111",
            "x-slurm:\n  time: 00:10:00\nservices:\n  app:\n    image: app.sqsh\n",
        );
        left.backend = SubmissionBackend::Slurm;
        let mut right = local_diff_record(
            tmpdir.path(),
            "222",
            "x-slurm:\n  time: 00:20:00\nservices:\n  app:\n    image: app.sqsh\n",
        );
        right.backend = SubmissionBackend::Slurm;
        fs::write(
            &left.compose_file,
            "services:\n  app:\n    image: app.sqsh\n",
        )
        .expect("compose");

        let report = build_job_diff_report(
            &left,
            &right,
            &SchedulerOptions {
                squeue_bin: "/definitely/not/squeue".into(),
                sacct_bin: "/definitely/not/sacct".into(),
            },
        );

        assert!(
            report
                .notes
                .iter()
                .any(|note| note.contains("status unavailable for job 111"))
        );
        assert!(
            report
                .notes
                .iter()
                .any(|note| note.contains("status unavailable for job 222"))
        );
        assert!(
            report
                .resource_changes
                .iter()
                .any(|change| change.path == "x-slurm.time")
        );
    }

    #[test]
    fn collapse_identical_detects_uniform_and_mixed_cells() {
        assert!(collapse_identical(&[]));
        assert!(collapse_identical(&[Some("a".to_string())]));
        assert!(collapse_identical(&[
            Some("a".to_string()),
            Some("a".to_string()),
        ]));
        assert!(collapse_identical(&[None, None]));
        assert!(!collapse_identical(&[
            Some("a".to_string()),
            Some("b".to_string()),
        ]));
        assert!(!collapse_identical(&[Some("a".to_string()), None]));
    }

    fn row_for<'a>(report: &'a JobMatrixReport, path: &str) -> Option<&'a JobMatrixRow> {
        report.rows.iter().find(|row| row.path == path)
    }

    #[test]
    fn matrix_keeps_differing_rows_and_collapses_identical_ones() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        // Same partition across all three; differing time and per-service image.
        let records: Vec<SubmissionRecord> = ["1", "2", "3"]
            .into_iter()
            .enumerate()
            .map(|(index, job_id)| {
                let time = format!("00:1{index}:00");
                let image = format!("app-{index}.sqsh");
                let record = local_diff_record(
                    tmpdir.path(),
                    job_id,
                    &format!(
                        "x-slurm:\n  partition: gpu\n  time: {time}\nservices:\n  app:\n    image: {image}\n",
                    ),
                );
                write_local_diff_record(
                    &record,
                    r#"{"backend":"local","job_status":"COMPLETED","job_exit_code":0,"services":[{"service_name":"app","completed_successfully":true,"last_exit_code":0}]}"#,
                );
                record
            })
            .collect();

        let report = build_job_matrix_report(&records, &SchedulerOptions::default());
        assert_eq!(report.runs.len(), 3);
        assert_eq!(
            report
                .runs
                .iter()
                .map(|run| run.job_id.as_str())
                .collect::<Vec<_>>(),
            ["1", "2", "3"]
        );

        // Differing fields are kept, one positionally-aligned cell per run.
        let time_row = row_for(&report, "x-slurm.time").expect("time row present");
        assert_eq!(time_row.section, "resources");
        assert_eq!(
            time_row.values,
            vec![
                Some("00:10:00".to_string()),
                Some("00:11:00".to_string()),
                Some("00:12:00".to_string()),
            ]
        );
        let image_row = row_for(&report, "services.app.image").expect("image row present");
        assert_eq!(
            image_row.values,
            vec![
                Some("app-0.sqsh".to_string()),
                Some("app-1.sqsh".to_string()),
                Some("app-2.sqsh".to_string()),
            ]
        );

        // An identical field (partition) is collapsed (no row).
        assert!(row_for(&report, "x-slurm.partition").is_none());
    }

    #[test]
    fn matrix_two_records_matches_pairwise_resource_and_config_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let left = local_diff_record(
            tmpdir.path(),
            "111",
            "x-slurm:\n  time: 00:10:00\nservices:\n  app:\n    image: app.sqsh\n    command: train\n",
        );
        let right = local_diff_record(
            tmpdir.path(),
            "222",
            "x-slurm:\n  time: 00:20:00\nservices:\n  app:\n    image: app.sqsh\n    command: eval\n",
        );
        write_local_diff_record(
            &left,
            r#"{"backend":"local","job_status":"COMPLETED","job_exit_code":0,"services":[{"service_name":"app","completed_successfully":true,"last_exit_code":0}]}"#,
        );
        write_local_diff_record(
            &right,
            r#"{"backend":"local","job_status":"COMPLETED","job_exit_code":0,"services":[{"service_name":"app","completed_successfully":true,"last_exit_code":0}]}"#,
        );

        let pairwise = build_job_diff_report(&left, &right, &SchedulerOptions::default());
        let matrix =
            build_job_matrix_report(&[left.clone(), right.clone()], &SchedulerOptions::default());

        // The resource paths the matrix surfaces match the pairwise diff exactly.
        let pairwise_resource: BTreeSet<&str> = pairwise
            .resource_changes
            .iter()
            .map(|change| change.path.as_str())
            .collect();
        let matrix_resource: BTreeSet<&str> = matrix
            .rows
            .iter()
            .filter(|row| row.section == "resources")
            .map(|row| row.path.as_str())
            .collect();
        assert_eq!(pairwise_resource, matrix_resource);

        // The config-section paths match the pairwise config_changes exactly.
        let pairwise_config: BTreeSet<&str> = pairwise
            .config_changes
            .iter()
            .map(|change| change.path.as_str())
            .collect();
        let matrix_config: BTreeSet<&str> = matrix
            .rows
            .iter()
            .filter(|row| row.section == "config")
            .map(|row| row.path.as_str())
            .collect();
        assert_eq!(pairwise_config, matrix_config);
    }

    #[test]
    fn matrix_surfaces_provenance_rows_when_git_sha_differs() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let mut first = local_diff_record(tmpdir.path(), "1", "name: x");
        let mut second = local_diff_record(tmpdir.path(), "2", "name: x");
        let mut third = local_diff_record(tmpdir.path(), "3", "name: x");
        let provenance = |sha: &str, tool: &str| {
            Some(JobProvenance {
                tool_version: tool.to_string(),
                git: Some(GitProvenance {
                    sha: sha.to_string(),
                    dirty: false,
                    branch: Some("main".to_string()),
                }),
                image_refs: [("app".to_string(), "img:1".to_string())]
                    .into_iter()
                    .collect(),
                source_content_hash: None,
            })
        };
        first.provenance = provenance("aaa", "0.1.0");
        second.provenance = provenance("bbb", "0.1.0");
        third.provenance = provenance("ccc", "0.1.0");
        for record in [&first, &second, &third] {
            write_local_diff_record(
                record,
                r#"{"backend":"local","job_status":"COMPLETED","job_exit_code":0,"services":[{"service_name":"app","completed_successfully":true,"last_exit_code":0}]}"#,
            );
        }

        let report = build_job_matrix_report(&[first, second, third], &SchedulerOptions::default());
        let sha_row = row_for(&report, "provenance.git.sha").expect("git sha row");
        assert_eq!(sha_row.section, "provenance");
        assert_eq!(
            sha_row.values,
            vec![
                Some("aaa".to_string()),
                Some("bbb".to_string()),
                Some("ccc".to_string()),
            ]
        );
        // tool_version is identical across all runs -> collapsed.
        assert!(row_for(&report, "provenance.tool_version").is_none());
        // git.branch identical -> collapsed; image refs identical -> collapsed.
        assert!(row_for(&report, "provenance.git.branch").is_none());
        assert!(row_for(&report, "provenance.image_refs.app").is_none());
        // The differing-commit note is surfaced.
        assert!(
            report
                .notes
                .iter()
                .any(|note| note == "jobs were built from different commits")
        );
    }
}
