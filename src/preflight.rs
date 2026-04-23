//! Login-node environment checks run before submission.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::cluster::ClusterProfile;
use crate::planner::{ImageSource, cache_path_policy_issue, registry_host_for_remote};
use crate::prepare::RuntimePlan;
use crate::readiness_util::readiness_uses_implicit_localhost;
use crate::spec::{MetricsCollector, ReadinessSpec, RuntimeBackend};
use crate::term;

/// Severity level for one preflight item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    /// The check passed.
    Ok,
    /// The check found a non-fatal issue worth surfacing.
    Warn,
    /// The check found a blocking issue.
    Error,
}

/// One preflight finding.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct Item {
    pub level: Level,
    pub message: String,
    pub remediation: Option<String>,
}

/// Flat preflight report before items are grouped for display.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub items: Vec<Item>,
}

/// Count summary for a grouped preflight report.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct ReportSummary {
    pub blockers: usize,
    pub actionable_warnings: usize,
    pub contextual_warnings: usize,
    pub passed_checks: usize,
}

/// Preflight report grouped into blockers, warnings, and passes.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct GroupedReport {
    pub summary: ReportSummary,
    pub blockers: Vec<Item>,
    pub actionable_warnings: Vec<Item>,
    pub contextual_warnings: Vec<Item>,
    pub passed_checks: Vec<Item>,
}

/// Options controlling which tools and checks preflight should require.
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct Options {
    pub enroot_bin: String,
    pub apptainer_bin: String,
    pub singularity_bin: String,
    pub sbatch_bin: String,
    pub srun_bin: String,
    pub scontrol_bin: String,
    pub require_submit_tools: bool,
    pub skip_prepare: bool,
    pub cluster_profile: Option<ClusterProfile>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            enroot_bin: "enroot".to_string(),
            apptainer_bin: "apptainer".to_string(),
            singularity_bin: "singularity".to_string(),
            sbatch_bin: "sbatch".to_string(),
            srun_bin: "srun".to_string(),
            scontrol_bin: "scontrol".to_string(),
            require_submit_tools: true,
            skip_prepare: false,
            cluster_profile: None,
        }
    }
}

impl Report {
    /// Returns `true` when the report contains at least one blocking error.
    pub fn has_errors(&self) -> bool {
        self.items.iter().any(|item| item.level == Level::Error)
    }

    /// Returns `true` when the report contains at least one warning.
    pub fn has_warnings(&self) -> bool {
        self.items.iter().any(|item| item.level == Level::Warn)
    }

    /// Renders the report in the default grouped text format.
    pub fn render(&self) -> String {
        self.render_grouped(false)
    }

    /// Renders the report with passed checks included.
    pub fn render_verbose(&self) -> String {
        self.render_grouped(true)
    }

    /// Returns a grouped representation used by CLI and JSON output.
    pub fn grouped(&self) -> GroupedReport {
        let mut blockers = Vec::new();
        let mut actionable_warnings = Vec::new();
        let mut contextual_warnings = Vec::new();
        let mut passed_checks = Vec::new();

        for item in &self.items {
            match item.level {
                Level::Error => blockers.push(item.clone()),
                Level::Warn if is_contextual_warning(item) => {
                    contextual_warnings.push(item.clone())
                }
                Level::Warn => actionable_warnings.push(item.clone()),
                Level::Ok => passed_checks.push(item.clone()),
            }
        }

        GroupedReport {
            summary: ReportSummary {
                blockers: blockers.len(),
                actionable_warnings: actionable_warnings.len(),
                contextual_warnings: contextual_warnings.len(),
                passed_checks: passed_checks.len(),
            },
            blockers,
            actionable_warnings,
            contextual_warnings,
            passed_checks,
        }
    }

    fn render_grouped(&self, verbose: bool) -> String {
        if self.items.is_empty() {
            return String::new();
        }

        let grouped = self.grouped();
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

fn is_contextual_warning(item: &Item) -> bool {
    matches!(item.level, Level::Warn)
        && (item
            .message
            .starts_with("neither /etc/slurm/task_prolog.hk nor /etc/slurm/task_prolog exists")
            || item.message.starts_with("HAICORE helper path is")
            || item.message.starts_with("metrics collector"))
}

/// Runs all login-node preflight checks for a prepared runtime plan.
pub fn run(plan: &RuntimePlan, options: &Options) -> Report {
    let mut report = Report { items: Vec::new() };

    check_runtime_backend(&mut report, plan, options);

    if options.require_submit_tools {
        let srun_available = check_binary(
            &mut report,
            &options.srun_bin,
            "srun is available",
            "Use a node with Slurm client tools installed or pass --srun-bin.",
        );
        check_binary(
            &mut report,
            &options.sbatch_bin,
            "sbatch is available",
            "Use a node with Slurm client tools installed or pass --sbatch-bin.",
        );
        if plan.slurm.is_multi_node() {
            check_binary(
                &mut report,
                &options.scontrol_bin,
                "scontrol is available",
                "Multi-node runs need scontrol on the submission host so hpc-compose can expand SLURM_JOB_NODELIST at runtime.",
            );
        }
        if srun_available {
            if plan.runtime.backend == RuntimeBackend::Pyxis {
                check_pyxis_support(&mut report, &options.srun_bin);
            }
            check_mpi_support(&mut report, &options.srun_bin, plan);
        }
        if plan.runtime.backend == RuntimeBackend::Pyxis {
            check_haicore_mount_helpers(&mut report);
        }
    }

    check_cache_path_policy(&mut report, plan);
    check_cache_dir_access(&mut report, &plan.cache_dir);
    check_local_and_mount_paths(&mut report, plan);
    check_resume_path(&mut report, plan);
    check_registry_credentials(&mut report, plan);
    check_readiness_host_tools(&mut report, plan);
    check_metrics_collectors(&mut report, plan);

    if options.skip_prepare {
        check_skip_prepare_readiness(&mut report, plan);
    }
    if let Some(profile) = &options.cluster_profile {
        check_cluster_profile(&mut report, plan, profile);
    }

    report
}

fn check_runtime_backend(report: &mut Report, plan: &RuntimePlan, options: &Options) {
    match plan.runtime.backend {
        RuntimeBackend::Pyxis => {
            check_binary(
                report,
                &options.enroot_bin,
                "Enroot is available",
                "Install Enroot on the login node or pass --enroot-bin with the correct path.",
            );
        }
        RuntimeBackend::Apptainer => {
            check_binary(
                report,
                &options.apptainer_bin,
                "Apptainer is available",
                "Install Apptainer on the login node or pass --apptainer-bin with the correct path.",
            );
        }
        RuntimeBackend::Singularity => {
            check_binary(
                report,
                &options.singularity_bin,
                "Singularity is available",
                "Install Singularity on the login node or pass --singularity-bin with the correct path.",
            );
        }
        RuntimeBackend::Host => report.items.push(Item {
            level: Level::Ok,
            message: "host runtime selected; no container runtime required".to_string(),
            remediation: None,
        }),
    }
}

fn check_cluster_profile(report: &mut Report, plan: &RuntimePlan, profile: &ClusterProfile) {
    let warnings = profile.validate_runtime_plan(plan);
    if warnings.is_empty() {
        report.items.push(Item {
            level: Level::Ok,
            message: "cluster profile is compatible with this plan".to_string(),
            remediation: None,
        });
        return;
    }
    for warning in warnings {
        report.items.push(Item {
            level: Level::Warn,
            message: warning.message,
            remediation: warning.remediation,
        });
    }
}

fn check_binary(report: &mut Report, binary: &str, ok_message: &str, remediation: &str) -> bool {
    if let Some(path) = find_binary(binary) {
        report.items.push(Item {
            level: Level::Ok,
            message: format!("{ok_message}: {}", path.display()),
            remediation: None,
        });
        true
    } else {
        report.items.push(Item {
            level: Level::Error,
            message: format!("required binary '{binary}' was not found"),
            remediation: Some(remediation.to_string()),
        });
        false
    }
}

fn check_optional_binary(
    report: &mut Report,
    binary: &str,
    ok_message: &str,
    missing_message: &str,
    remediation: &str,
) -> bool {
    if let Some(path) = find_binary(binary) {
        report.items.push(Item {
            level: Level::Ok,
            message: format!("{ok_message}: {}", path.display()),
            remediation: None,
        });
        true
    } else {
        report.items.push(Item {
            level: Level::Warn,
            message: missing_message.to_string(),
            remediation: Some(remediation.to_string()),
        });
        false
    }
}

fn check_pyxis_support(report: &mut Report, srun_bin: &str) {
    match Command::new(srun_bin).arg("--help").output() {
        Ok(output) => {
            let text = String::from_utf8_lossy(&output.stdout).to_string()
                + &String::from_utf8_lossy(&output.stderr);
            if text.contains("--container-image") {
                report.items.push(Item {
                    level: Level::Ok,
                    message: "srun reports Pyxis container support".to_string(),
                    remediation: None,
                });
            } else {
                report.items.push(Item {
                    level: Level::Error,
                    message: "srun does not advertise --container-image; Pyxis support appears unavailable".to_string(),
                    remediation: Some("Check whether the Pyxis plugin is enabled on this cluster or run on a supported HAICORE login node.".to_string()),
                });
            }
        }
        Err(err) => report.items.push(Item {
            level: Level::Error,
            message: format!("failed to execute '{srun_bin} --help': {err}"),
            remediation: Some(
                "Verify the Slurm client installation and PATH on this node.".to_string(),
            ),
        }),
    }
}

fn check_mpi_support(report: &mut Report, srun_bin: &str, plan: &RuntimePlan) {
    let requested = plan
        .ordered_services
        .iter()
        .filter_map(|service| {
            service
                .slurm
                .mpi
                .as_ref()
                .map(|mpi| (service.name.as_str(), mpi.mpi_type.as_srun_value()))
        })
        .collect::<Vec<_>>();
    if requested.is_empty() {
        return;
    }

    let output = match Command::new(srun_bin).arg("--mpi=list").output() {
        Ok(output) => output,
        Err(err) => {
            report.items.push(Item {
                level: Level::Warn,
                message: format!("failed to query '{srun_bin} --mpi=list': {err}"),
                remediation: Some(
                    "Run 'srun --mpi=list' on the target cluster and confirm the requested x-slurm.mpi.type is available.".to_string(),
                ),
            });
            return;
        }
    };

    let text = String::from_utf8_lossy(&output.stdout).to_string()
        + &String::from_utf8_lossy(&output.stderr);
    if !output.status.success() && text.trim().is_empty() {
        report.items.push(Item {
            level: Level::Warn,
            message: format!("'{srun_bin} --mpi=list' exited without listing MPI plugin types"),
            remediation: Some(
                "Run 'srun --mpi=list' on the target cluster and confirm the requested x-slurm.mpi.type is available.".to_string(),
            ),
        });
        return;
    }

    let advertised = advertised_mpi_types(&text);
    for (service_name, mpi_type) in requested {
        if advertised.iter().any(|value| value == mpi_type) {
            report.items.push(Item {
                level: Level::Ok,
                message: format!("srun reports MPI type '{mpi_type}' for service '{service_name}'"),
                remediation: None,
            });
        } else {
            report.items.push(Item {
                level: Level::Warn,
                message: format!(
                    "service '{service_name}' requests x-slurm.mpi.type='{mpi_type}', but 'srun --mpi=list' did not advertise it"
                ),
                remediation: Some(
                    "Use a supported x-slurm.mpi.type for this cluster, or keep site-specific MPI launch flags in x-slurm.extra_srun_args.".to_string(),
                ),
            });
        }
    }
}

fn advertised_mpi_types(output: &str) -> Vec<String> {
    let mut values = output
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '+')))
        .filter(|token| mpi_advertised_token_looks_useful(token))
        .map(str::to_string)
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

fn mpi_advertised_token_looks_useful(token: &str) -> bool {
    if token.is_empty() || token.starts_with('-') {
        return false;
    }
    let lower = token.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "mpi"
            | "plugin"
            | "plugins"
            | "type"
            | "types"
            | "are"
            | "available"
            | "specific"
            | "version"
            | "versions"
    ) {
        return false;
    }
    token
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'+'))
}

fn check_cache_path_policy(report: &mut Report, plan: &RuntimePlan) {
    if let Some(issue) = cache_path_policy_issue(&plan.cache_dir) {
        report.items.push(Item {
            level: Level::Error,
            message: issue,
            remediation: Some("Set x-slurm.cache_dir to a shared workspace or another filesystem visible from both login and compute nodes.".to_string()),
        });
    } else {
        report.items.push(Item {
            level: Level::Ok,
            message: format!(
                "cache directory passes shared-path policy: {}",
                plan.cache_dir.display()
            ),
            remediation: None,
        });
    }

    if let Some(home) = env::var_os("HOME").map(PathBuf::from)
        && plan.cache_dir.starts_with(&home)
    {
        let message = if plan.slurm.cache_dir.is_none() {
            format!(
                "cache directory defaults under HOME: {}",
                plan.cache_dir.display()
            )
        } else {
            format!(
                "cache directory resolves under HOME: {}",
                plan.cache_dir.display()
            )
        };
        report.items.push(Item {
            level: Level::Warn,
            message,
            remediation: Some("Prepare runs on the login node, but compute nodes must reuse the same cache at runtime. Choose a shared filesystem path such as workspace, project, or other shared storage; a local HOME path can prepare successfully and still fail at runtime.".to_string()),
        });
    }
}

fn check_cache_dir_access(report: &mut Report, cache_dir: &Path) {
    if let Err(err) = fs::create_dir_all(cache_dir) {
        report.items.push(Item {
            level: Level::Error,
            message: format!(
                "cache directory '{}' is not creatable: {err}",
                cache_dir.display()
            ),
            remediation: Some(
                "Choose a writable x-slurm.cache_dir and ensure the parent directory exists."
                    .to_string(),
            ),
        });
        return;
    }

    let probe = cache_dir.join(".hpc-compose-write-probe");
    match fs::write(&probe, "probe") {
        Ok(()) => {
            let _ = fs::remove_file(&probe);
            report.items.push(Item {
                level: Level::Ok,
                message: format!("cache directory is writable: {}", cache_dir.display()),
                remediation: None,
            });
        }
        Err(err) => report.items.push(Item {
            level: Level::Error,
            message: format!(
                "cache directory '{}' is not writable: {err}",
                cache_dir.display()
            ),
            remediation: Some(
                "Pick a writable cache directory on a shared filesystem before submitting jobs."
                    .to_string(),
            ),
        }),
    }
}

fn check_local_and_mount_paths(report: &mut Report, plan: &RuntimePlan) {
    for service in &plan.ordered_services {
        if let ImageSource::LocalSqsh(path) = &service.source {
            if path.exists() && path.is_file() {
                report.items.push(Item {
                    level: Level::Ok,
                    message: format!(
                        "local image for service '{}' is present: {}",
                        service.name,
                        path.display()
                    ),
                    remediation: None,
                });
            } else {
                report.items.push(Item {
                    level: Level::Error,
                    message: format!("local image for service '{}' does not exist: {}", service.name, path.display()),
                    remediation: Some("Fix the image path in compose.yaml or create the .sqsh file before submitting.".to_string()),
                });
            }
        }
        if let ImageSource::LocalSif(path) = &service.source {
            if path.exists() && path.is_file() {
                report.items.push(Item {
                    level: Level::Ok,
                    message: format!(
                        "local SIF image for service '{}' is present: {}",
                        service.name,
                        path.display()
                    ),
                    remediation: None,
                });
            } else {
                report.items.push(Item {
                    level: Level::Error,
                    message: format!(
                        "local SIF image for service '{}' does not exist: {}",
                        service.name,
                        path.display()
                    ),
                    remediation: Some(
                        "Fix the image path in compose.yaml or create the .sif file before submitting."
                            .to_string(),
                    ),
                });
            }
        }

        for mount in &service.volumes {
            check_mount_path(report, &service.name, mount, "runtime volume");
        }
        if let Some(prepare) = &service.prepare {
            for mount in &prepare.mounts {
                check_mount_path(report, &service.name, mount, "prepare mount");
            }
        }
    }
}

fn check_resume_path(report: &mut Report, plan: &RuntimePlan) {
    let Some(resume_dir) = plan.slurm.resume_dir() else {
        return;
    };

    let resume_path = Path::new(resume_dir);
    if resume_path.starts_with("/tmp") || resume_path.starts_with("/var/tmp") {
        report.items.push(Item {
            level: Level::Warn,
            message: format!(
                "resume directory uses a node-local temporary root and may not survive requeue/resume safely: {}",
                resume_path.display()
            ),
            remediation: Some(
                "Use a shared filesystem path such as /shared/$USER/... for x-slurm.resume.path."
                    .to_string(),
            ),
        });
        return;
    }

    report.items.push(Item {
        level: Level::Ok,
        message: format!(
            "resume directory is configured on host storage: {}",
            resume_path.display()
        ),
        remediation: None,
    });
}

fn check_mount_path(report: &mut Report, service_name: &str, mount: &str, kind: &str) {
    let host = host_path_from_mount(mount);
    let path = Path::new(host);
    if path.exists() {
        report.items.push(Item {
            level: Level::Ok,
            message: format!(
                "{kind} for service '{service_name}' is present: {}",
                path.display()
            ),
            remediation: None,
        });
    } else {
        report.items.push(Item {
            level: Level::Error,
            message: format!(
                "{kind} for service '{service_name}' is missing: {}",
                path.display()
            ),
            remediation: Some(
                "Create the host directory/file or fix the mount path in compose.yaml.".to_string(),
            ),
        });
    }
}

fn check_skip_prepare_readiness(report: &mut Report, plan: &RuntimePlan) {
    for service in &plan.ordered_services {
        if matches!(service.source, ImageSource::Host) {
            continue;
        }
        let requires_cached_runtime =
            matches!(service.source, ImageSource::Remote(_)) || service.prepare.is_some();
        if !requires_cached_runtime {
            continue;
        }
        if service.runtime_image.exists() {
            report.items.push(Item {
                level: Level::Ok,
                message: format!(
                    "skip-prepare can reuse runtime image for service '{}': {}",
                    service.name,
                    service.runtime_image.display()
                ),
                remediation: None,
            });
        } else {
            report.items.push(Item {
                level: Level::Error,
                message: format!(
                    "skip-prepare requested, but runtime image for service '{}' is missing: {}",
                    service.name,
                    service.runtime_image.display()
                ),
                remediation: Some(
                    "Run 'hpc-compose prepare -f compose.yaml' first or remove --skip-prepare."
                        .to_string(),
                ),
            });
        }
    }
}

fn check_metrics_collectors(report: &mut Report, plan: &RuntimePlan) {
    if !plan.slurm.metrics_enabled() {
        return;
    }

    for collector in plan.slurm.metrics_collectors() {
        match collector {
            MetricsCollector::Gpu => {
                check_optional_binary(
                    report,
                    "nvidia-smi",
                    "metrics collector 'gpu' can query nvidia-smi",
                    "metrics collector 'gpu' requested but 'nvidia-smi' was not found on this node",
                    "GPU metrics are best-effort. This is expected on some login nodes; verify that compute nodes providing GPUs also provide nvidia-smi if you want runtime GPU telemetry.",
                );
            }
            MetricsCollector::Slurm => {
                check_optional_binary(
                    report,
                    "sstat",
                    "metrics collector 'slurm' can query sstat",
                    "metrics collector 'slurm' requested but 'sstat' was not found on this node",
                    "Step-level CPU and memory telemetry is best-effort. This is expected on some login nodes; verify that compute nodes provide sstat and that Slurm accounting is enabled if you want runtime stats.",
                );
            }
        }
    }
}

fn check_readiness_host_tools(report: &mut Report, plan: &RuntimePlan) {
    let has_http_readiness = plan
        .ordered_services
        .iter()
        .any(|service| matches!(service.readiness, Some(ReadinessSpec::Http { .. })));
    if has_http_readiness {
        check_optional_binary(
            report,
            "curl",
            "HTTP readiness checks can query curl",
            "HTTP readiness checks require 'curl' on the host, but it was not found on this node",
            "Install curl on the host or switch readiness.type to a probe that uses tools already available on the batch node.",
        );
    }

    for service in &plan.ordered_services {
        if service.placement.nodes <= 1 {
            continue;
        }
        if !readiness_uses_implicit_localhost(service.readiness.as_ref()) {
            continue;
        }
        report.items.push(Item {
            level: Level::Error,
            message: format!(
                "multi-node service '{}' uses readiness that still relies on localhost semantics",
                service.name
            ),
            remediation: Some(
                "Use readiness.type=sleep or readiness.type=log, or switch TCP/HTTP readiness to an explicit non-local host or URL."
                    .to_string(),
            ),
        });
    }
}

fn check_haicore_mount_helpers(report: &mut Report) {
    check_haicore_mount_helpers_with_paths(
        report,
        Path::new("/etc/slurm/task_prolog.hk"),
        Path::new("/etc/slurm/task_prolog"),
        &[
            Path::new("/scratch"),
            Path::new("/usr/lib64/slurm/libslurmfull.so"),
            Path::new("/usr/lib64/libhwloc.so.15"),
        ],
    );
}

fn check_haicore_mount_helpers_with_paths(
    report: &mut Report,
    task_prolog: &Path,
    fallback_prolog: &Path,
    helper_paths: &[&Path],
) {
    if task_prolog.exists() || fallback_prolog.exists() {
        report.items.push(Item {
            level: Level::Ok,
            message: "found a Slurm task_prolog helper mount expected by HAICORE/Pyxis".to_string(),
            remediation: None,
        });
    } else {
        report.items.push(Item {
            level: Level::Warn,
            message: "neither /etc/slurm/task_prolog.hk nor /etc/slurm/task_prolog exists on this node".to_string(),
            remediation: Some("This is expected on non-cluster machines, but on HAICORE you should verify the required Pyxis helper mount path.".to_string()),
        });
    }

    for p in helper_paths {
        if p.exists() {
            report.items.push(Item {
                level: Level::Ok,
                message: format!("HAICORE helper path is present: {}", p.display()),
                remediation: None,
            });
        } else {
            report.items.push(Item {
                level: Level::Warn,
                message: format!("HAICORE helper path is absent on this node: {}", p.display()),
                remediation: Some("This is only a problem on the actual cluster if Pyxis requires this helper mount.".to_string()),
            });
        }
    }
}

fn check_registry_credentials(report: &mut Report, plan: &RuntimePlan) {
    if plan.runtime.backend != RuntimeBackend::Pyxis {
        return;
    }
    let credential_path = enroot_credentials_path();
    let entries = credential_entries(credential_path.as_deref()).unwrap_or_default();

    for service in &plan.ordered_services {
        let ImageSource::Remote(remote) = &service.source else {
            continue;
        };
        let registry = registry_for_remote(remote);
        match registry.as_str() {
            "registry-1.docker.io" => {
                if entries.contains("registry-1.docker.io") {
                    report.items.push(Item {
                        level: Level::Ok,
                        message: format!(
                            "Docker Hub credentials detected for service '{}'",
                            service.name
                        ),
                        remediation: None,
                    });
                } else {
                    report.items.push(Item {
                        level: Level::Warn,
                        message: format!("Docker Hub credentials not found for service '{}'; anonymous pulls may be rate-limited", service.name),
                        remediation: Some("Add 'machine registry-1.docker.io ...' to your Enroot credentials file if rate limits become a problem.".to_string()),
                    });
                }
            }
            "nvcr.io" => {
                let has_nvcr = entries.contains("nvcr.io");
                let has_authn = entries.contains("authn.nvidia.com");
                if has_nvcr && has_authn {
                    report.items.push(Item {
                        level: Level::Ok,
                        message: format!("NGC credentials detected for service '{}'", service.name),
                        remediation: None,
                    });
                } else {
                    report.items.push(Item {
                        level: Level::Warn,
                        message: format!("NGC credentials look incomplete for service '{}'", service.name),
                        remediation: Some("Add both 'machine nvcr.io ...' and 'machine authn.nvidia.com ...' entries to ENROOT_CONFIG_PATH/.credentials.".to_string()),
                    });
                }
            }
            host if host == "registry.scc.kit.edu" || host.ends_with(".scc.kit.edu") => {
                if entries.contains(host) {
                    report.items.push(Item {
                        level: Level::Ok,
                        message: format!(
                            "KIT registry credentials detected for service '{}'",
                            service.name
                        ),
                        remediation: None,
                    });
                } else {
                    report.items.push(Item {
                        level: Level::Warn,
                        message: format!(
                            "credentials for registry '{}' were not found for service '{}'",
                            host, service.name
                        ),
                        remediation: Some(format!(
                            "Add 'machine {host} ...' to {} if this image is private.",
                            credential_path_display(credential_path.as_deref())
                        )),
                    });
                }
            }
            host => {
                if entries.contains(host) {
                    report.items.push(Item {
                        level: Level::Ok,
                        message: format!(
                            "registry credentials detected for '{}' (service '{}')",
                            host, service.name
                        ),
                        remediation: None,
                    });
                } else {
                    report.items.push(Item {
                        level: Level::Warn,
                        message: format!(
                            "credentials for registry '{}' were not found for service '{}'",
                            host, service.name
                        ),
                        remediation: Some(format!(
                            "If '{}' is private, add 'machine {} ...' to {}.",
                            host,
                            host,
                            credential_path_display(credential_path.as_deref())
                        )),
                    });
                }
            }
        }
    }
}

fn host_path_from_mount(mount: &str) -> &str {
    mount.split_once(':').map(|(host, _)| host).unwrap_or(mount)
}

fn find_binary(binary: &str) -> Option<PathBuf> {
    if binary.contains(std::path::MAIN_SEPARATOR) {
        let path = PathBuf::from(binary);
        return path.exists().then_some(path);
    }
    let path_var = env::var_os("PATH")?;
    env::split_paths(&path_var)
        .map(|dir| dir.join(binary))
        .find(|path| path.exists())
}

fn registry_for_remote(remote: &str) -> String {
    registry_host_for_remote(remote)
}

fn enroot_credentials_path() -> Option<PathBuf> {
    if let Ok(config_path) = env::var("ENROOT_CONFIG_PATH") {
        return Some(PathBuf::from(config_path).join(".credentials"));
    }
    if let Ok(xdg) = env::var("XDG_CONFIG_HOME") {
        return Some(PathBuf::from(xdg).join("enroot/.credentials"));
    }
    env::var("HOME")
        .ok()
        .map(|home| PathBuf::from(home).join(".config/enroot/.credentials"))
}

fn credential_entries(path: Option<&Path>) -> Result<std::collections::HashSet<String>> {
    let Some(path) = path else {
        return Ok(std::collections::HashSet::new());
    };
    if !path.exists() {
        return Ok(std::collections::HashSet::new());
    }
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut entries = std::collections::HashSet::new();
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        if parts.next() != Some("machine") {
            continue;
        }
        if let Some(host) = parts.next() {
            entries.insert(host.to_string());
        }
    }
    Ok(entries)
}

fn credential_path_display(path: Option<&Path>) -> String {
    path.map(|p| p.display().to_string())
        .unwrap_or_else(|| "ENROOT_CONFIG_PATH/.credentials".to_string())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, OnceLock};

    use super::*;
    use crate::cluster::RuntimeAvailability;
    use crate::planner::{
        ExecutionSpec, ImageSource, PreparedImageSpec, ServicePlacement, ServicePlacementMode,
    };
    use crate::prepare::RuntimeService;
    use crate::spec::{
        MetricsCollector, MetricsConfig, MpiConfig, MpiType, ReadinessSpec, ServiceFailurePolicy,
        ServiceSlurmConfig, SlurmConfig,
    };

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn runtime_plan(tmpdir: &Path) -> RuntimePlan {
        RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![RuntimeService {
                name: "app".into(),
                runtime_image: tmpdir.join("cache/base/app.sqsh"),
                execution: ExecutionSpec::Shell("echo hi".into()),
                environment: Vec::new(),
                volumes: vec![tmpdir.join("src").display().to_string() + ":/src"],
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: Some(PreparedImageSpec {
                    commands: vec!["echo prep".into()],
                    mounts: vec![tmpdir.join("deps").display().to_string() + ":/deps"],
                    env: Vec::new(),
                    root: true,
                    force_rebuild: true,
                }),
                source: ImageSource::Remote("docker://registry.scc.kit.edu#proj/app:latest".into()),
            }],
        }
    }

    fn write_fake_binary(path: &Path, body: &str) {
        fs::write(path, body).expect("write fake binary");
        let mut perms = fs::metadata(path).expect("meta").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("chmod");
    }

    #[test]
    fn preflight_reports_missing_mounts() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let plan = runtime_plan(tmpdir.path());
        let report = run(
            &plan,
            &Options {
                require_submit_tools: false,
                ..Options::default()
            },
        );
        assert!(report.has_errors());
        assert!(report.render().contains("runtime volume"));
    }

    #[test]
    fn preflight_detects_pyxis_missing() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        fs::create_dir_all(tmpdir.path().join("src")).expect("src");
        fs::create_dir_all(tmpdir.path().join("deps")).expect("deps");
        let srun = tmpdir.path().join("srun");
        let sbatch = tmpdir.path().join("sbatch");
        let enroot = tmpdir.path().join("enroot");
        write_fake_binary(&srun, "#!/bin/bash\necho no-pyxis\n");
        write_fake_binary(&sbatch, "#!/bin/bash\nexit 0\n");
        write_fake_binary(&enroot, "#!/bin/bash\nexit 0\n");
        let plan = runtime_plan(tmpdir.path());
        let report = run(
            &plan,
            &Options {
                enroot_bin: enroot.display().to_string(),
                sbatch_bin: sbatch.display().to_string(),
                srun_bin: srun.display().to_string(),
                scontrol_bin: "scontrol".into(),
                require_submit_tools: true,
                skip_prepare: false,
                ..Options::default()
            },
        );
        assert!(
            report
                .render()
                .contains("Pyxis support appears unavailable")
        );
    }

    #[test]
    fn preflight_checks_requested_mpi_types() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        fs::create_dir_all(tmpdir.path().join("src")).expect("src");
        fs::create_dir_all(tmpdir.path().join("deps")).expect("deps");
        let srun = tmpdir.path().join("srun");
        let sbatch = tmpdir.path().join("sbatch");
        let enroot = tmpdir.path().join("enroot");
        write_fake_binary(
            &srun,
            "#!/bin/bash\nif [[ \"${1:-}\" == \"--help\" ]]; then echo 'usage: srun --container-image=IMAGE'; exit 0; fi\nif [[ \"${1:-}\" == \"--mpi=list\" ]]; then echo 'MPI plugin types are...'; echo 'pmix pmi2 openmpi'; exit 0; fi\nexit 0\n",
        );
        write_fake_binary(&sbatch, "#!/bin/bash\nexit 0\n");
        write_fake_binary(&enroot, "#!/bin/bash\nexit 0\n");
        let mut plan = runtime_plan(tmpdir.path());
        plan.ordered_services[0].slurm.mpi = Some(MpiConfig {
            mpi_type: MpiType::new("pmix").expect("mpi type"),
            implementation: None,
            launcher: Default::default(),
            expected_ranks: None,
            host_mpi: None,
        });
        let report = run(
            &plan,
            &Options {
                enroot_bin: enroot.display().to_string(),
                sbatch_bin: sbatch.display().to_string(),
                srun_bin: srun.display().to_string(),
                scontrol_bin: "scontrol".into(),
                require_submit_tools: true,
                skip_prepare: false,
                ..Options::default()
            },
        );
        assert!(
            report.items.iter().any(|item| {
                item.level == Level::Ok && item.message.contains("MPI type 'pmix'")
            })
        );

        plan.ordered_services[0].slurm.mpi = Some(MpiConfig {
            mpi_type: MpiType::new("pmi1").expect("mpi type"),
            implementation: None,
            launcher: Default::default(),
            expected_ranks: None,
            host_mpi: None,
        });
        let report = run(
            &plan,
            &Options {
                enroot_bin: enroot.display().to_string(),
                sbatch_bin: sbatch.display().to_string(),
                srun_bin: srun.display().to_string(),
                scontrol_bin: "scontrol".into(),
                require_submit_tools: true,
                skip_prepare: false,
                ..Options::default()
            },
        );
        assert!(report.items.iter().any(|item| {
            item.level == Level::Warn && item.message.contains("did not advertise")
        }));
    }

    #[test]
    fn preflight_warns_when_mpi_list_query_fails() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        fs::create_dir_all(tmpdir.path().join("src")).expect("src");
        fs::create_dir_all(tmpdir.path().join("deps")).expect("deps");
        let srun = tmpdir.path().join("srun");
        let sbatch = tmpdir.path().join("sbatch");
        let enroot = tmpdir.path().join("enroot");
        write_fake_binary(
            &srun,
            "#!/bin/bash\nif [[ \"${1:-}\" == \"--help\" ]]; then echo 'usage: srun --container-image=IMAGE'; exit 0; fi\nif [[ \"${1:-}\" == \"--mpi=list\" ]]; then exit 2; fi\nexit 0\n",
        );
        write_fake_binary(&sbatch, "#!/bin/bash\nexit 0\n");
        write_fake_binary(&enroot, "#!/bin/bash\nexit 0\n");
        let mut plan = runtime_plan(tmpdir.path());
        plan.ordered_services[0].slurm.mpi = Some(MpiConfig {
            mpi_type: MpiType::new("pmix").expect("mpi type"),
            implementation: None,
            launcher: Default::default(),
            expected_ranks: None,
            host_mpi: None,
        });

        let report = run(
            &plan,
            &Options {
                enroot_bin: enroot.display().to_string(),
                sbatch_bin: sbatch.display().to_string(),
                srun_bin: srun.display().to_string(),
                scontrol_bin: "scontrol".into(),
                require_submit_tools: true,
                skip_prepare: false,
                ..Options::default()
            },
        );
        assert!(report.items.iter().any(|item| {
            item.level == Level::Warn && item.message.contains("exited without listing")
        }));
    }

    #[test]
    fn preflight_skip_prepare_requires_runtime_image() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        fs::create_dir_all(tmpdir.path().join("src")).expect("src");
        fs::create_dir_all(tmpdir.path().join("deps")).expect("deps");
        let plan = runtime_plan(tmpdir.path());
        let report = run(
            &plan,
            &Options {
                require_submit_tools: false,
                skip_prepare: true,
                ..Options::default()
            },
        );
        assert!(report.render().contains("skip-prepare requested"));
    }

    #[test]
    fn report_helpers_cover_empty_and_error_rendering() {
        let empty = Report { items: Vec::new() };
        assert!(!empty.has_errors());
        assert!(!empty.has_warnings());
        assert_eq!(empty.render(), "");

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
                    level: Level::Error,
                    message: "boom".into(),
                    remediation: Some("repair".into()),
                },
            ],
        };
        assert!(report.has_errors());
        assert!(report.has_warnings());
        let rendered = report.render();
        let grouped = report.grouped();
        let summary = grouped.summary;
        assert_eq!(summary.blockers, 1);
        assert_eq!(summary.actionable_warnings, 1);
        assert_eq!(summary.contextual_warnings, 0);
        assert_eq!(summary.passed_checks, 1);
        assert_eq!(grouped.blockers[0].message, "boom");
        assert_eq!(grouped.actionable_warnings[0].message, "warn");
        assert_eq!(grouped.passed_checks[0].message, "fine");
        assert!(rendered.contains("Summary:"));
        let verbose = report.render_verbose();
        assert!(verbose.contains("fine"));
    }

    #[test]
    fn cluster_profile_checks_are_added_to_preflight_report() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let mut plan = runtime_plan(tmpdir.path());
        plan.cache_dir = tmpdir.path().join("cache");

        let compatible = ClusterProfile {
            schema_version: 1,
            generated_at_unix: None,
            slurm_version: None,
            mpi_types: Vec::new(),
            partitions: Vec::new(),
            qos: Vec::new(),
            gpu_models: Vec::new(),
            runtimes: RuntimeAvailability {
                pyxis: true,
                enroot: true,
                apptainer: false,
                singularity: false,
                host: true,
            },
            shared_cache_paths: vec![tmpdir.path().display().to_string()],
        };
        let mut report = Report { items: Vec::new() };
        check_cluster_profile(&mut report, &plan, &compatible);
        assert!(report.items.iter().any(|item| {
            item.level == Level::Ok && item.message.contains("cluster profile is compatible")
        }));

        let incompatible = ClusterProfile {
            runtimes: RuntimeAvailability {
                pyxis: false,
                enroot: true,
                apptainer: false,
                singularity: false,
                host: true,
            },
            shared_cache_paths: vec!["/shared".into()],
            ..compatible
        };
        let mut report = Report { items: Vec::new() };
        check_cluster_profile(&mut report, &plan, &incompatible);
        assert!(report.items.iter().any(|item| {
            item.level == Level::Warn && item.message.contains("runtime.backend=pyxis")
        }));
        assert!(
            report
                .items
                .iter()
                .any(|item| { item.level == Level::Warn && item.message.contains("cache_dir") })
        );
    }

    #[test]
    fn check_binary_and_find_binary_cover_success_and_failure() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let fake = tmpdir.path().join("tool");
        write_fake_binary(&fake, "#!/bin/bash\nexit 0\n");

        let mut report = Report { items: Vec::new() };
        assert!(check_binary(
            &mut report,
            fake.to_str().expect("path"),
            "tool is available",
            "fix it"
        ));
        assert!(report.render_verbose().contains("tool is available"));
        assert_eq!(
            find_binary(fake.to_str().expect("path")),
            Some(fake.clone())
        );

        let missing = tmpdir.path().join("missing-tool");
        assert!(!check_binary(
            &mut report,
            missing.to_str().expect("path"),
            "never",
            "install it"
        ));
        assert!(report.render().contains("required binary"));
    }

    #[test]
    fn check_pyxis_support_error_branch_is_reported() {
        let mut report = Report { items: Vec::new() };
        check_pyxis_support(&mut report, "/path/does/not/exist");
        assert!(report.render().contains("failed to execute"));
    }

    #[test]
    fn cache_path_policy_and_home_warning_are_reported() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let home = std::env::var("HOME").expect("home");
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: PathBuf::from(home).join(".cache/hpc-compose"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: runtime_plan(tmpdir.path()).ordered_services,
        };
        let mut report = Report { items: Vec::new() };
        check_cache_path_policy(&mut report, &plan);
        let text = report.render_verbose();
        assert!(text.contains("passes shared-path policy"));
        assert!(text.contains("defaults under HOME"));
        assert!(text.contains("Prepare runs on the login node"));

        let explicit_home = RuntimePlan {
            slurm: SlurmConfig {
                cache_dir: Some("~/shared-cache".into()),
                ..SlurmConfig::default()
            },
            ..plan.clone()
        };
        let mut report = Report { items: Vec::new() };
        check_cache_path_policy(&mut report, &explicit_home);
        let text = report.render_verbose();
        assert!(text.contains("resolves under HOME"));
        assert!(text.contains("compute nodes must reuse the same cache"));

        let tmp_issue = RuntimePlan {
            cache_dir: PathBuf::from("/tmp/hpc-compose"),
            ..plan
        };
        let mut report = Report { items: Vec::new() };
        check_cache_path_policy(&mut report, &tmp_issue);
        assert!(report.render().contains("not shared"));
    }

    #[test]
    fn cache_dir_access_reports_creation_failure() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let parent_file = tmpdir.path().join("not-a-dir");
        fs::write(&parent_file, "x").expect("file");
        let target = parent_file.join("cache");
        let mut report = Report { items: Vec::new() };
        check_cache_dir_access(&mut report, &target);
        assert!(report.render().contains("not creatable"));
    }

    #[test]
    fn local_sqsh_presence_and_mount_presence_are_reported() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        fs::create_dir_all(tmpdir.path().join("src")).expect("src");
        fs::create_dir_all(tmpdir.path().join("deps")).expect("deps");
        let local_image = tmpdir.path().join("image.sqsh");
        fs::write(&local_image, "image").expect("image");

        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![RuntimeService {
                name: "local".into(),
                runtime_image: local_image.clone(),
                execution: ExecutionSpec::Shell("echo hi".into()),
                environment: Vec::new(),
                volumes: vec![tmpdir.path().join("src").display().to_string() + ":/src"],
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: Some(PreparedImageSpec {
                    commands: vec!["echo prep".into()],
                    mounts: vec![tmpdir.path().join("deps").display().to_string() + ":/deps"],
                    env: Vec::new(),
                    root: true,
                    force_rebuild: true,
                }),
                source: ImageSource::LocalSqsh(local_image),
            }],
        };
        let mut report = Report { items: Vec::new() };
        check_local_and_mount_paths(&mut report, &plan);
        let text = report.render_verbose();
        assert!(text.contains("local image for service 'local' is present"));
        assert!(text.contains("runtime volume for service 'local' is present"));
        assert!(text.contains("prepare mount for service 'local' is present"));
    }

    #[test]
    fn resume_path_reports_ok_and_temp_root_warning() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let mut plan = runtime_plan(tmpdir.path());
        plan.slurm.resume = Some(crate::spec::ResumeConfig {
            path: "/shared/runs/demo".into(),
        });

        let mut report = Report { items: Vec::new() };
        check_resume_path(&mut report, &plan);
        assert!(
            report
                .render_verbose()
                .contains("resume directory is configured on host storage")
        );

        plan.slurm.resume = Some(crate::spec::ResumeConfig {
            path: "/tmp/demo".into(),
        });
        let mut report = Report { items: Vec::new() };
        check_resume_path(&mut report, &plan);
        let text = report.render();
        assert!(text.contains("node-local temporary root"));
        assert!(text.contains("x-slurm.resume.path"));
    }

    #[test]
    fn skip_prepare_skips_local_services_without_prepare() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let local = tmpdir.path().join("local.sqsh");
        fs::write(&local, "x").expect("local");
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![RuntimeService {
                name: "local".into(),
                runtime_image: local.clone(),
                execution: ExecutionSpec::Shell("echo hi".into()),
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: None,
                source: ImageSource::LocalSqsh(local),
            }],
        };
        let mut report = Report { items: Vec::new() };
        check_skip_prepare_readiness(&mut report, &plan);
        assert!(report.items.is_empty());
    }

    #[test]
    fn haicore_helper_positive_branch_is_coverable() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let task_prolog = tmpdir.path().join("task_prolog.hk");
        let helper_a = tmpdir.path().join("scratch");
        let helper_b = tmpdir.path().join("libslurmfull.so");
        let helper_c = tmpdir.path().join("libhwloc.so.15");
        fs::write(&task_prolog, "").expect("task prolog");
        fs::create_dir_all(&helper_a).expect("helper_a");
        fs::write(&helper_b, "").expect("helper_b");
        fs::write(&helper_c, "").expect("helper_c");

        let mut report = Report { items: Vec::new() };
        check_haicore_mount_helpers_with_paths(
            &mut report,
            &task_prolog,
            &tmpdir.path().join("fallback"),
            &[&helper_a, &helper_b, &helper_c],
        );
        let text = report.render_verbose();
        assert!(text.contains("found a Slurm task_prolog helper mount"));
        assert!(text.contains("HAICORE helper path is present"));
    }

    #[test]
    fn metrics_collectors_are_reported_as_contextual_warnings() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let _guard = env_lock().lock().expect("lock");
        let old_path = env::var_os("PATH");
        unsafe {
            env::set_var("PATH", tmpdir.path());
        }
        let mut plan = runtime_plan(tmpdir.path());
        plan.slurm.metrics = Some(MetricsConfig {
            enabled: Some(true),
            interval_seconds: Some(5),
            collectors: vec![MetricsCollector::Gpu, MetricsCollector::Slurm],
        });

        let mut report = Report { items: Vec::new() };
        check_metrics_collectors(&mut report, &plan);
        let grouped = report.grouped();
        assert_eq!(grouped.contextual_warnings.len(), 2);
        assert!(grouped.contextual_warnings.iter().any(|item| {
            item.message
                .contains("metrics collector 'gpu' requested but 'nvidia-smi' was not found")
        }));
        assert!(grouped.contextual_warnings.iter().any(|item| {
            item.message
                .contains("metrics collector 'slurm' requested but 'sstat' was not found")
        }));

        match old_path {
            Some(path) => unsafe { env::set_var("PATH", path) },
            None => unsafe { env::remove_var("PATH") },
        }
    }

    #[test]
    fn http_readiness_requires_curl_on_host() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let _guard = env_lock().lock().expect("lock");
        let old_path = env::var_os("PATH");
        unsafe {
            env::set_var("PATH", tmpdir.path());
        }
        let mut plan = runtime_plan(tmpdir.path());
        plan.ordered_services[0].readiness = Some(ReadinessSpec::Http {
            url: "http://127.0.0.1:8080/health".into(),
            status_code: 200,
            timeout_seconds: Some(30),
        });

        let report = run(&plan, &Options::default());
        assert!(report.items.iter().any(|item| {
            item.message
                .contains("HTTP readiness checks require 'curl' on the host")
        }));

        match old_path {
            Some(path) => unsafe { env::set_var("PATH", path) },
            None => unsafe { env::remove_var("PATH") },
        }
    }

    #[test]
    fn multi_node_preflight_requires_scontrol_and_rejects_localhost_distributed_readiness() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        fs::create_dir_all(tmpdir.path().join("src")).expect("src");
        fs::create_dir_all(tmpdir.path().join("deps")).expect("deps");
        let srun = tmpdir.path().join("srun");
        let sbatch = tmpdir.path().join("sbatch");
        let enroot = tmpdir.path().join("enroot");
        write_fake_binary(
            &srun,
            "#!/bin/bash\nif [[ \"${1:-}\" == \"--help\" ]]; then echo 'usage: srun --container-image=IMAGE'; fi\n",
        );
        write_fake_binary(&sbatch, "#!/bin/bash\nexit 0\n");
        write_fake_binary(&enroot, "#!/bin/bash\nexit 0\n");

        let mut plan = runtime_plan(tmpdir.path());
        plan.slurm.nodes = Some(2);
        plan.ordered_services[0].placement = ServicePlacement {
            mode: ServicePlacementMode::Distributed,
            nodes: 2,
            ntasks: None,
            ntasks_per_node: Some(1),
            pin_to_primary_node: false,
            node_indices: None,
            exclude_indices: Vec::new(),
            allow_overlap: false,
        };
        plan.ordered_services[0].readiness = Some(ReadinessSpec::Tcp {
            host: None,
            port: 29500,
            timeout_seconds: None,
        });

        let report = run(
            &plan,
            &Options {
                enroot_bin: enroot.display().to_string(),
                sbatch_bin: sbatch.display().to_string(),
                srun_bin: srun.display().to_string(),
                scontrol_bin: tmpdir.path().join("missing-scontrol").display().to_string(),
                require_submit_tools: true,
                skip_prepare: false,
                ..Options::default()
            },
        );
        let text = report.render();
        assert!(text.contains("required binary"));
        assert!(text.contains("multi-node service 'app' uses readiness"));
    }

    #[test]
    fn registry_helpers_cover_multiple_paths_and_parsing() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let creds = tmpdir.path().join(".credentials");
        fs::write(
            &creds,
            "\n# comment\nmachine registry-1.docker.io login foo password bar\nmachine nvcr.io login x password y\nmachine authn.nvidia.com login a password b\nmachine registry.scc.kit.edu login c password d\nmachine ghcr.io login e password f\nignored line\n",
        )
        .expect("creds");

        let entries = credential_entries(Some(&creds)).expect("entries");
        assert!(entries.contains("registry-1.docker.io"));
        assert!(entries.contains("nvcr.io"));
        assert!(entries.contains("authn.nvidia.com"));
        assert!(entries.contains("registry.scc.kit.edu"));
        assert!(entries.contains("ghcr.io"));
        assert_eq!(
            registry_for_remote("docker://redis:7"),
            "registry-1.docker.io"
        );
        assert_eq!(host_path_from_mount("/tmp/a:/b"), "/tmp/a");
        assert_eq!(host_path_from_mount("/tmp/a"), "/tmp/a");
        assert_eq!(
            credential_path_display(None),
            "ENROOT_CONFIG_PATH/.credentials"
        );
        assert_eq!(
            credential_path_display(Some(&creds)),
            creds.display().to_string()
        );
    }

    #[test]
    fn registry_credentials_cover_ok_and_warn_variants() {
        let _guard = env_lock().lock().expect("lock");
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let config_dir = tmpdir.path().join("enroot-config");
        fs::create_dir_all(&config_dir).expect("config_dir");
        let creds = config_dir.join(".credentials");
        fs::write(
            &creds,
            "machine registry-1.docker.io login foo password bar\nmachine nvcr.io login x password y\nmachine authn.nvidia.com login a password b\nmachine registry.scc.kit.edu login c password d\nmachine ghcr.io login e password f\n",
        )
        .expect("creds");
        let old_enroot = env::var_os("ENROOT_CONFIG_PATH");
        let old_xdg = env::var_os("XDG_CONFIG_HOME");
        unsafe {
            env::set_var("ENROOT_CONFIG_PATH", &config_dir);
            env::remove_var("XDG_CONFIG_HOME");
        }

        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![
                RuntimeService {
                    name: "hub".into(),
                    runtime_image: tmpdir.path().join("hub.sqsh"),
                    execution: ExecutionSpec::Shell("echo hi".into()),
                    environment: Vec::new(),
                    volumes: Vec::new(),
                    working_dir: None,
                    depends_on: Vec::new(),
                    readiness: None,
                    failure_policy: ServiceFailurePolicy::default(),
                    placement: ServicePlacement::default(),
                    slurm: ServiceSlurmConfig::default(),
                    prepare: None,
                    source: ImageSource::Remote("docker://redis:7".into()),
                },
                RuntimeService {
                    name: "ngc".into(),
                    runtime_image: tmpdir.path().join("ngc.sqsh"),
                    execution: ExecutionSpec::Shell("echo hi".into()),
                    environment: Vec::new(),
                    volumes: Vec::new(),
                    working_dir: None,
                    depends_on: Vec::new(),
                    readiness: None,
                    failure_policy: ServiceFailurePolicy::default(),
                    placement: ServicePlacement::default(),
                    slurm: ServiceSlurmConfig::default(),
                    prepare: None,
                    source: ImageSource::Remote("docker://nvcr.io/nvidia/pytorch:24.01-py3".into()),
                },
                RuntimeService {
                    name: "kit".into(),
                    runtime_image: tmpdir.path().join("kit.sqsh"),
                    execution: ExecutionSpec::Shell("echo hi".into()),
                    environment: Vec::new(),
                    volumes: Vec::new(),
                    working_dir: None,
                    depends_on: Vec::new(),
                    readiness: None,
                    failure_policy: ServiceFailurePolicy::default(),
                    placement: ServicePlacement::default(),
                    slurm: ServiceSlurmConfig::default(),
                    prepare: None,
                    source: ImageSource::Remote(
                        "docker://registry.scc.kit.edu#proj/app:latest".into(),
                    ),
                },
                RuntimeService {
                    name: "ghcr".into(),
                    runtime_image: tmpdir.path().join("ghcr.sqsh"),
                    execution: ExecutionSpec::Shell("echo hi".into()),
                    environment: Vec::new(),
                    volumes: Vec::new(),
                    working_dir: None,
                    depends_on: Vec::new(),
                    readiness: None,
                    failure_policy: ServiceFailurePolicy::default(),
                    placement: ServicePlacement::default(),
                    slurm: ServiceSlurmConfig::default(),
                    prepare: None,
                    source: ImageSource::Remote("docker://ghcr.io/example/private:latest".into()),
                },
                RuntimeService {
                    name: "local".into(),
                    runtime_image: tmpdir.path().join("local.sqsh"),
                    execution: ExecutionSpec::Shell("echo hi".into()),
                    environment: Vec::new(),
                    volumes: Vec::new(),
                    working_dir: None,
                    depends_on: Vec::new(),
                    readiness: None,
                    failure_policy: ServiceFailurePolicy::default(),
                    placement: ServicePlacement::default(),
                    slurm: ServiceSlurmConfig::default(),
                    prepare: None,
                    source: ImageSource::LocalSqsh(tmpdir.path().join("local.sqsh")),
                },
            ],
        };

        let mut report = Report { items: Vec::new() };
        check_registry_credentials(&mut report, &plan);
        let text = report.render_verbose();
        assert!(text.contains("Docker Hub credentials detected"));
        assert!(text.contains("NGC credentials detected"));
        assert!(text.contains("KIT registry credentials detected"));
        assert!(text.contains("registry credentials detected for 'ghcr.io'"));
        assert!(!text.contains("service 'local'"));

        fs::write(&creds, "machine nvcr.io login x password y\n").expect("partial creds");
        let mut report = Report { items: Vec::new() };
        check_registry_credentials(&mut report, &plan);
        let text = report.render();
        assert!(text.contains("Docker Hub credentials not found"));
        assert!(text.contains("NGC credentials look incomplete"));
        assert!(text.contains("credentials for registry 'registry.scc.kit.edu' were not found"));
        assert!(text.contains("credentials for registry 'ghcr.io' were not found"));

        unsafe {
            match old_enroot {
                Some(v) => env::set_var("ENROOT_CONFIG_PATH", v),
                None => env::remove_var("ENROOT_CONFIG_PATH"),
            }
            match old_xdg {
                Some(v) => env::set_var("XDG_CONFIG_HOME", v),
                None => env::remove_var("XDG_CONFIG_HOME"),
            }
        }
    }

    #[test]
    fn enroot_credentials_path_and_entries_cover_env_and_error_cases() {
        let _guard = env_lock().lock().expect("lock");
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let old_enroot = env::var_os("ENROOT_CONFIG_PATH");
        let old_xdg = env::var_os("XDG_CONFIG_HOME");

        let cfg = tmpdir.path().join("cfg");
        let xdg = tmpdir.path().join("xdg");
        unsafe {
            env::set_var("ENROOT_CONFIG_PATH", &cfg);
            env::remove_var("XDG_CONFIG_HOME");
        }
        assert_eq!(enroot_credentials_path(), Some(cfg.join(".credentials")));

        unsafe {
            env::remove_var("ENROOT_CONFIG_PATH");
            env::set_var("XDG_CONFIG_HOME", &xdg);
        }
        assert_eq!(
            enroot_credentials_path(),
            Some(xdg.join("enroot/.credentials"))
        );

        let missing = tmpdir.path().join("missing.credentials");
        assert!(
            credential_entries(Some(&missing))
                .expect("missing")
                .is_empty()
        );
        let dir_err = credential_entries(Some(tmpdir.path())).expect_err("dir should fail");
        assert!(dir_err.to_string().contains("failed to read"));
        assert!(find_binary("definitely-not-on-path").is_none());

        unsafe {
            match old_enroot {
                Some(v) => env::set_var("ENROOT_CONFIG_PATH", v),
                None => env::remove_var("ENROOT_CONFIG_PATH"),
            }
            match old_xdg {
                Some(v) => env::set_var("XDG_CONFIG_HOME", v),
                None => env::remove_var("XDG_CONFIG_HOME"),
            }
        }
    }
}
