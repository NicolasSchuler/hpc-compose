//! Opinionated static lint checks for hpc-compose specs.

use serde::{Deserialize, Serialize};

use crate::cluster::ClusterProfile;
use crate::domain::{MountParts, split_mount_parts};
use crate::planner::{ImageSource, Plan, cache_path_policy_issue};
use crate::prepare::RuntimePlan;
use crate::spec::{DependencyCondition, ScratchScope, ServiceFailureMode, parse_memory_bytes};

const LOW_MEMORY_PER_CPU_BYTES: u64 = 512 * 1_024 * 1_024;
const HIGH_MEMORY_PER_CPU_BYTES: u64 = 512 * 1_024 * 1_024 * 1_024;

/// Severity for a lint finding.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LintLevel {
    /// Advisory finding that is worth reviewing.
    Warning,
    /// Finding severe enough to reject a spec.
    Error,
}

/// A machine-readable remediation that `lint --fix` can apply to the source
/// compose file.
///
/// Only findings whose fix is deterministic and semantics-preserving carry a
/// [`SuggestedFix`]; everything else stays advisory.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SuggestedFix {
    /// Rewrite a `depends_on` edge so its condition is explicit instead of
    /// relying on the implicit `service_started` default.
    DependsOnCondition {
        /// Service that owns the `depends_on` block.
        service: String,
        /// Dependency edge whose condition should be made explicit.
        dependency: String,
        /// Condition to write into the source file.
        condition: String,
    },
}

/// One stable lint finding emitted by `hpc-compose lint`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LintFinding {
    /// Finding severity.
    pub level: LintLevel,
    /// Stable lint rule identifier.
    pub code: String,
    /// Human-readable finding message.
    pub message: String,
    /// Service associated with this finding, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    /// Spec field associated with this finding, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// Suggested remediation, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommendation: Option<String>,
    /// Machine-readable fix `lint --fix` can apply, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix: Option<SuggestedFix>,
}

impl LintFinding {
    fn warning(
        code: &str,
        message: impl Into<String>,
        service: Option<String>,
        field: Option<String>,
        recommendation: impl Into<String>,
    ) -> Self {
        Self {
            level: LintLevel::Warning,
            code: code.to_string(),
            message: message.into(),
            service,
            field,
            recommendation: Some(recommendation.into()),
            fix: None,
        }
    }

    fn warning_with_fix(
        code: &str,
        message: impl Into<String>,
        service: Option<String>,
        field: Option<String>,
        recommendation: impl Into<String>,
        fix: SuggestedFix,
    ) -> Self {
        Self {
            level: LintLevel::Warning,
            code: code.to_string(),
            message: message.into(),
            service,
            field,
            recommendation: Some(recommendation.into()),
            fix: Some(fix),
        }
    }
}

/// Runs opinionated static lint checks over a validated runtime plan.
#[must_use]
pub fn lint_plan(
    plan: &Plan,
    runtime_plan: &RuntimePlan,
    cluster_profile: Option<&ClusterProfile>,
) -> Vec<LintFinding> {
    let mut findings = Vec::new();
    lint_dependency_readiness(plan, &mut findings);
    lint_implicit_dependency_condition(plan, &mut findings);
    lint_memory_cpu_ratio(plan, &mut findings);
    lint_ignore_shared_writes(plan, runtime_plan, cluster_profile, &mut findings);
    lint_cache_path_policy(runtime_plan, &mut findings);
    lint_node_local_volumes(runtime_plan, &mut findings);
    lint_image_reproducibility(runtime_plan, &mut findings);
    if let Some(profile) = cluster_profile {
        for warning in profile.validate_runtime_plan(runtime_plan) {
            findings.push(LintFinding {
                level: LintLevel::Warning,
                code: "HPC900".to_string(),
                message: warning.message,
                service: None,
                field: None,
                recommendation: warning.remediation,
                fix: None,
            });
        }
    }
    findings
}

fn lint_dependency_readiness(plan: &Plan, findings: &mut Vec<LintFinding>) {
    for service in &plan.ordered_services {
        for dependency in &service.depends_on {
            if dependency.condition != DependencyCondition::ServiceStarted {
                continue;
            }
            let Some(upstream) = plan
                .ordered_services
                .iter()
                .find(|candidate| candidate.name == dependency.name)
            else {
                continue;
            };
            if upstream.readiness.is_some() {
                continue;
            }
            findings.push(LintFinding::warning(
                "HPC001",
                format!(
                    "service '{}' depends on '{}' with service_started, but '{}' has no readiness probe",
                    service.name, dependency.name, dependency.name
                ),
                Some(service.name.clone()),
                Some(format!("services.{}.depends_on.{}", service.name, dependency.name)),
                "Add readiness to the upstream service or use service_completed_successfully for one-shot dependencies.",
            ));
        }
    }
}

/// HPC006: `depends_on` edges whose condition is implicit (list-form or
/// mapping form without an explicit `condition:` key). The fix makes the
/// implicit `service_started` default explicit, which is semantics-preserving.
fn lint_implicit_dependency_condition(plan: &Plan, findings: &mut Vec<LintFinding>) {
    for service in &plan.ordered_services {
        for dependency in &service.depends_on {
            if !dependency.implicit {
                continue;
            }
            let condition = match dependency.condition {
                DependencyCondition::ServiceStarted => "service_started",
                DependencyCondition::ServiceHealthy => "service_healthy",
                DependencyCondition::ServiceCompletedSuccessfully => {
                    "service_completed_successfully"
                }
            };
            findings.push(LintFinding::warning_with_fix(
                "HPC006",
                format!(
                    "service '{}' depends on '{}' without an explicit condition; it currently resolves to '{}'",
                    service.name, dependency.name, condition
                ),
                Some(service.name.clone()),
                Some(format!("services.{}.depends_on.{}", service.name, dependency.name)),
                "Make the condition explicit so author intent is unambiguous. `lint --fix` writes the current default for you.",
                SuggestedFix::DependsOnCondition {
                    service: service.name.clone(),
                    dependency: dependency.name.clone(),
                    condition: condition.to_string(),
                },
            ));
        }
    }
}

/// HPC004: `x-slurm.cache_dir` resolves under a node-local root. Advisory:
/// the right replacement is cluster-specific, so this is not auto-fixable.
fn lint_cache_path_policy(runtime_plan: &RuntimePlan, findings: &mut Vec<LintFinding>) {
    if let Some(issue) = cache_path_policy_issue(&runtime_plan.cache_dir) {
        findings.push(LintFinding::warning(
            "HPC004",
            issue,
            None,
            Some("x-slurm.cache_dir".to_string()),
            "Set x-slurm.cache_dir to a shared workspace or another filesystem visible from both login and compute nodes. This finding is advisory; `lint --fix` will not rewrite paths.",
        ));
    }
}

/// HPC005: a service volume whose host side lives under a node-local root.
/// Advisory: the right replacement is cluster-specific, so this is not
/// auto-fixable.
fn lint_node_local_volumes(runtime_plan: &RuntimePlan, findings: &mut Vec<LintFinding>) {
    for service in &runtime_plan.ordered_services {
        for mount in &service.volumes {
            let Some((host, _container, _mode)) = split_mount(mount) else {
                continue;
            };
            if !crate::path_util::is_node_local_path(host) {
                continue;
            }
            findings.push(LintFinding::warning(
                "HPC005",
                format!(
                    "service '{}' mounts host path '{}' which is typically node-local and not visible from compute nodes",
                    service.name, host
                ),
                Some(service.name.clone()),
                Some(format!("services.{}.volumes", service.name)),
                "Move the host path under shared storage visible from both login and compute nodes, or use job-local scratch. This finding is advisory; `lint --fix` will not rewrite paths.",
            ));
        }
    }
}

/// HPC007: a remote container image referenced by a mutable or imprecise tag
/// (`:latest`, or no tag at all) instead of being pinned to an immutable
/// `@sha256:` digest. Advisory: pinning is reproducibility hygiene, and the
/// correct digest is environment-specific, so this is not auto-fixable.
fn lint_image_reproducibility(runtime_plan: &RuntimePlan, findings: &mut Vec<LintFinding>) {
    for service in &runtime_plan.ordered_services {
        let ImageSource::Remote(reference) = &service.source else {
            // Local `.sqsh`/`.sif` artifacts and host runtime are already
            // file-pinned; only remote registry references can drift.
            continue;
        };
        if image_reference_is_reproducible(reference) {
            continue;
        }
        findings.push(LintFinding::warning(
            "HPC007",
            format!(
                "service '{}' uses image '{}' which relies on a mutable or missing tag",
                service.name, reference
            ),
            Some(service.name.clone()),
            Some(format!("services.{}.image", service.name)),
            "Pin the image by digest (e.g. repo/name@sha256:...), or at least an explicit non-`latest` version tag, for reproducible runs. This finding is advisory; `lint --fix` will not rewrite image references.",
        ));
    }
}

/// Returns `true` when a remote image reference is reproducible enough to skip
/// the HPC007 warning: it is either pinned to an immutable `@sha256:` digest or
/// carries an explicit, non-`latest` tag. A `:latest` tag or a missing tag both
/// count as unpinned.
fn image_reference_is_reproducible(reference: &str) -> bool {
    // Strip the transport scheme (`docker://`, `dockerd://`, `podman://`, …)
    // and any Enroot registry separator so we inspect the image path itself.
    let path = reference
        .split_once("://")
        .map_or(reference, |(_scheme, rest)| rest);
    let path = path.rsplit('#').next().unwrap_or(path);
    // A digest pin is the gold standard and is always reproducible.
    if path.contains("@sha256:") {
        return true;
    }
    // A tag lives after the last ':' that follows the final '/', so a registry
    // port like `host:5000/repo` is not mistaken for a tag.
    let last_segment = path.rsplit('/').next().unwrap_or(path);
    let Some((_name, tag)) = last_segment.rsplit_once(':') else {
        // No tag at all -> implicitly `:latest`, so not reproducible.
        return false;
    };
    !tag.is_empty() && !tag.eq_ignore_ascii_case("latest")
}

fn lint_memory_cpu_ratio(plan: &Plan, findings: &mut Vec<LintFinding>) {
    let Some(mem) = plan.slurm.mem.as_deref() else {
        return;
    };
    let Some(bytes) = parse_memory_bytes(mem) else {
        return;
    };
    let cpus = allocation_cpu_count(plan);
    if cpus == 0 {
        return;
    }
    let bytes_per_cpu = bytes / u64::from(cpus);
    let (message, recommendation) = if bytes_per_cpu < LOW_MEMORY_PER_CPU_BYTES {
        (
            format!(
                "x-slurm.mem='{mem}' gives less than 512 MiB per requested CPU ({})",
                format_bytes(bytes_per_cpu)
            ),
            "Increase x-slurm.mem or reduce CPU/task counts if the job is not intentionally memory-light.",
        )
    } else if bytes_per_cpu > HIGH_MEMORY_PER_CPU_BYTES {
        (
            format!(
                "x-slurm.mem='{mem}' gives more than 512 GiB per requested CPU ({})",
                format_bytes(bytes_per_cpu)
            ),
            "Check x-slurm.mem and CPU counts; very high memory-per-CPU requests can queue poorly or violate site policy.",
        )
    } else {
        return;
    };
    findings.push(LintFinding::warning(
        "HPC002",
        message,
        None,
        Some("x-slurm.mem".to_string()),
        recommendation,
    ));
}

fn allocation_cpu_count(plan: &Plan) -> u32 {
    let cpus_per_task = plan.slurm.cpus_per_task.unwrap_or(1);
    let tasks = plan.slurm.ntasks.unwrap_or_else(|| {
        plan.slurm
            .ntasks_per_node
            .map(|tasks| tasks.saturating_mul(plan.slurm.nodes.unwrap_or(1)))
            .unwrap_or(1)
    });
    tasks.saturating_mul(cpus_per_task).max(1)
}

fn lint_ignore_shared_writes(
    plan: &Plan,
    runtime_plan: &RuntimePlan,
    cluster_profile: Option<&ClusterProfile>,
    findings: &mut Vec<LintFinding>,
) {
    let shared_roots = cluster_profile
        .map(|profile| profile.shared_cache_paths.as_slice())
        .unwrap_or(&[]);
    for service in &runtime_plan.ordered_services {
        if service.failure_policy.mode != ServiceFailureMode::Ignore {
            continue;
        }
        for mount in &service.volumes {
            let Some((host, container, mode)) = split_mount(mount) else {
                continue;
            };
            if mode == Some("ro") || !host_looks_shared(host, shared_roots) {
                continue;
            }
            findings.push(LintFinding::warning(
                "HPC003",
                format!(
                    "service '{}' ignores failures but has a writable mount from '{}' to '{}'",
                    service.name, host, container
                ),
                Some(service.name.clone()),
                Some(format!("services.{}.volumes", service.name)),
                "Use a read-only mount, write to job-local scratch, or avoid mode=ignore for services that can mutate shared state.",
            ));
        }
        if let Some(scratch) = &plan.slurm.scratch
            && scratch.scope == ScratchScope::Shared
            && service_scratch_enabled(service)
        {
            findings.push(LintFinding::warning(
                "HPC003",
                format!(
                    "service '{}' ignores failures while shared scratch is enabled",
                    service.name
                ),
                Some(service.name.clone()),
                Some("x-slurm.scratch".to_string()),
                "Disable service scratch for this sidecar or avoid mode=ignore when it writes to shared scratch.",
            ));
        }
    }
}

fn split_mount(value: &str) -> Option<(&str, &str, Option<&str>)> {
    match split_mount_parts(value) {
        MountParts::HostContainer {
            host,
            container,
            mode,
        } => Some((host.trim(), container.trim(), mode)),
        MountParts::UnsupportedMode(_) | MountParts::InvalidShape => None,
    }
}

fn host_looks_shared(host: &str, shared_roots: &[String]) -> bool {
    if shared_roots.is_empty() {
        return !crate::path_util::is_node_local_path(host);
    }
    shared_roots.iter().any(|root| path_is_under(host, root))
}

fn path_is_under(path: &str, root: &str) -> bool {
    let path = std::path::Path::new(path);
    let root = std::path::Path::new(root);
    path == root || path.starts_with(root)
}

fn service_scratch_enabled(service: &crate::prepare::RuntimeService) -> bool {
    service
        .slurm
        .scratch
        .as_ref()
        .and_then(|scratch| scratch.enabled)
        .unwrap_or(true)
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = UNITS[0];
    for next in UNITS.iter().skip(1) {
        if value < 1024.0 {
            break;
        }
        value /= 1024.0;
        unit = next;
    }
    if unit == "B" {
        format!("{bytes} {unit}")
    } else {
        format!("{value:.1} {unit}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_parser_accepts_simple_slurm_units() {
        assert_eq!(parse_memory_bytes("512M"), Some(512 * 1_024 * 1_024));
        assert_eq!(parse_memory_bytes("1.5G"), Some(1_610_612_736));
        assert_eq!(parse_memory_bytes("2GiB"), Some(2 * 1_024 * 1_024 * 1_024));
        assert_eq!(parse_memory_bytes("4Gc"), None);
    }

    #[test]
    fn hpc007_lint_emits_warning_only_for_mutable_images() {
        use std::path::PathBuf;

        use crate::prepare::{RuntimePlan, RuntimeService};
        use crate::spec::{ServiceFailurePolicy, ServiceSlurmConfig};

        fn service(name: &str, source: ImageSource) -> RuntimeService {
            RuntimeService {
                name: name.into(),
                runtime_image: PathBuf::from("/cache/x.sqsh"),
                execution: crate::planner::ExecutionSpec::ImageDefault,
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                assertions: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: crate::planner::ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: None,
                source,
            }
        }

        let runtime_plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: PathBuf::from("/cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: crate::spec::SlurmConfig::default(),
            ordered_services: vec![
                service("mutable", ImageSource::Remote("docker://foo:latest".into())),
                service(
                    "pinned",
                    ImageSource::Remote(
                        "docker://foo@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                            .into(),
                    ),
                ),
                service("local", ImageSource::LocalSqsh(PathBuf::from("/img.sqsh"))),
            ],
        };

        let mut findings = Vec::new();
        lint_image_reproducibility(&runtime_plan, &mut findings);
        assert_eq!(findings.len(), 1);
        let finding = &findings[0];
        assert_eq!(finding.code, "HPC007");
        assert_eq!(finding.level, LintLevel::Warning);
        assert_eq!(finding.service.as_deref(), Some("mutable"));
    }

    #[test]
    fn hpc007_flags_mutable_or_missing_image_tags() {
        // `:latest` and missing tags are mutable -> HPC007.
        assert!(!image_reference_is_reproducible("docker://foo:latest"));
        assert!(!image_reference_is_reproducible("docker://foo"));
        assert!(!image_reference_is_reproducible("foo:LATEST"));
        // Explicit version tags and digests are reproducible -> no HPC007.
        assert!(image_reference_is_reproducible("docker://foo:1.2.3"));
        assert!(image_reference_is_reproducible(
            "docker://foo@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
        // A registry port must not be mistaken for a tag.
        assert!(!image_reference_is_reproducible(
            "docker://registry.example:5000/foo"
        ));
        assert!(image_reference_is_reproducible(
            "docker://registry.example:5000/foo:1.0"
        ));
        // Enroot's `#` registry separator is stripped before tag inspection.
        assert!(!image_reference_is_reproducible(
            "docker://registry.example#proj/app:latest"
        ));
        assert!(image_reference_is_reproducible(
            "docker://registry.example#proj/app:2.0"
        ));
    }

    #[test]
    fn split_mount_trims_parts_and_rejects_malformed() {
        // host/container are trimmed; the mode token must match `ro`/`rw` exactly.
        assert_eq!(
            split_mount("  /data : /mnt :ro"),
            Some(("/data", "/mnt", Some("ro")))
        );
        assert_eq!(split_mount("/data:/mnt"), Some(("/data", "/mnt", None)));
        // An unsupported mode token is rejected.
        assert_eq!(split_mount("/data:/mnt:private"), None);
        // A bare path with no container target has an invalid shape.
        assert_eq!(split_mount("/only-one"), None);
    }

    #[test]
    fn path_is_under_matches_self_and_descendants_only() {
        assert!(path_is_under("/scratch", "/scratch"));
        assert!(path_is_under("/scratch/job/data", "/scratch"));
        assert!(!path_is_under("/scratchpad", "/scratch"));
        assert!(!path_is_under("/home/user", "/scratch"));
    }

    #[test]
    fn host_looks_shared_uses_explicit_roots_when_provided() {
        let roots = vec!["/shared".to_string(), "/projects".to_string()];
        assert!(host_looks_shared("/shared/data", &roots));
        assert!(host_looks_shared("/projects", &roots));
        assert!(!host_looks_shared("/local/tmp", &roots));
    }

    #[test]
    fn format_bytes_keeps_bytes_exact_and_scales_larger_units() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1_024), "1.0 KiB");
        assert_eq!(format_bytes(1_536), "1.5 KiB");
        assert_eq!(format_bytes(2 * 1_024 * 1_024), "2.0 MiB");
        assert_eq!(format_bytes(3 * 1_024 * 1_024 * 1_024), "3.0 GiB");
    }
}
