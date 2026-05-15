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

const RESOURCE_FIELDS: &[&[&str]] = &[
    &["runtime", "backend"],
    &["x-slurm", "resources"],
    &["x-slurm", "partition"],
    &["x-slurm", "account"],
    &["x-slurm", "qos"],
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

/// Builds a compact diff report for two tracked jobs.
pub fn build_job_diff_report(
    left: &SubmissionRecord,
    right: &SubmissionRecord,
    options: &SchedulerOptions,
) -> JobDiffReport {
    let mut notes = Vec::new();
    let left_status = match build_status_snapshot(&left.compose_file, Some(&left.job_id), options) {
        Ok(snapshot) => Some(snapshot),
        Err(err) => {
            notes.push(format!("status unavailable for job {}: {err}", left.job_id));
            None
        }
    };
    let right_status =
        match build_status_snapshot(&right.compose_file, Some(&right.job_id), options) {
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

    JobDiffReport {
        left: left_side,
        right: right_side,
        outcome_changes,
        resource_changes,
        config_changes,
        notes,
    }
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
}
