use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use hpc_compose::cli::OutputFormat;
use hpc_compose::cluster::{
    ClusterProfile, default_cluster_profile_path, generate_cluster_profile, write_cluster_profile,
};
use hpc_compose::context::{ResolvedBinaries, ResolvedContext};
use hpc_compose::planner::ExecutionSpec;
use hpc_compose::preflight::{Item, Level, Report};
use hpc_compose::prepare::{
    PrepareOptions, RuntimePlan, RuntimeService, build_runtime_plan, prepare_runtime_plan,
};
use hpc_compose::render::{
    RenderOptions, display_srun_command_for_backend, log_file_name_for_service,
    render_script_with_options,
};
use hpc_compose::spec::{ServiceFailurePolicy, SlurmConfig};

use crate::output::{self, common as output_common};

pub(crate) fn doctor(
    format: Option<OutputFormat>,
    binaries: &ResolvedBinaries,
    cluster_report: bool,
    cluster_report_out: Option<PathBuf>,
) -> Result<()> {
    let output_format = output::resolve_output_format(format, false);
    if cluster_report {
        return doctor_cluster_report(output_format, binaries, cluster_report_out);
    }
    let report = run_doctor(binaries);
    match output_format {
        OutputFormat::Text => print_doctor_report(&report),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report.grouped())
                    .map_err(|e| anyhow::anyhow!("failed to serialize doctor report: {e}"))?
            );
        }
    }
    Ok(())
}

pub(crate) fn doctor_mpi_smoke(
    context: ResolvedContext,
    format: Option<OutputFormat>,
    service_name: Option<String>,
    submit: bool,
    script_out: Option<PathBuf>,
    timeout_seconds: u64,
    quiet: bool,
) -> Result<()> {
    if submit && timeout_seconds == 0 {
        bail!("doctor --mpi-smoke --timeout-seconds must be at least 1 when --submit is used");
    }
    let output_format = output::resolve_output_format(format, false);
    let plan = output_common::load_plan_with_interpolation_vars(
        &context.compose_file.value,
        &context.interpolation_vars,
    )?;
    let runtime_plan = build_runtime_plan(&plan);
    let service = select_mpi_service(&runtime_plan, service_name.as_deref())?;
    let expected_ranks = mpi_expected_ranks(service);
    let smoke_plan = build_mpi_smoke_plan(&runtime_plan, service.name.as_str(), expected_ranks)?;
    let smoke_service = smoke_plan
        .ordered_services
        .first()
        .context("MPI smoke plan did not contain a service")?;
    let mpi = service
        .slurm
        .mpi
        .as_ref()
        .context("selected MPI service does not define x-slurm.mpi")?;
    let requested_mpi_type = mpi.mpi_type.as_srun_value().to_string();
    let advertised_mpi_types = run_capture(&context.binaries.srun.value, &["--mpi=list"])
        .map(|raw| advertised_mpi_types(&raw))
        .unwrap_or_default();
    let host_mpi_bind_paths = mpi
        .host_mpi
        .as_ref()
        .map(|host_mpi| host_mpi.bind_paths.clone())
        .unwrap_or_default();
    let host_mpi_env = mpi
        .host_mpi
        .as_ref()
        .map(|host_mpi| host_mpi.env.to_pairs())
        .transpose()?
        .unwrap_or_default();
    let rendered_srun =
        display_srun_command_for_backend(smoke_service, smoke_plan.runtime.backend).join(" ");
    let script = render_script_with_options(
        &smoke_plan,
        &RenderOptions {
            apptainer_bin: context.binaries.apptainer.value.clone(),
            singularity_bin: context.binaries.singularity.value.clone(),
        },
    )?;

    let mut wrote_script = None;
    if let Some(path) = script_out.as_ref() {
        write_script(path, &script)?;
        wrote_script = Some(path.clone());
    }

    let submit_result = if submit {
        prepare_runtime_plan(
            &smoke_plan,
            &PrepareOptions {
                enroot_bin: context.binaries.enroot.value.clone(),
                apptainer_bin: context.binaries.apptainer.value.clone(),
                singularity_bin: context.binaries.singularity.value.clone(),
                keep_failed_prep: false,
                force_rebuild: false,
            },
        )?;
        let (script_path, cleanup) = match wrote_script.clone() {
            Some(path) => (path, false),
            None => {
                let path = temp_smoke_script_path();
                write_script(&path, &script)?;
                (path, true)
            }
        };
        let mut result = run_sbatch_wait(
            &context.binaries.sbatch.value,
            &script_path,
            Duration::from_secs(timeout_seconds),
        )?;
        result.service_log =
            read_smoke_service_log(&context.cwd, &result.stdout, service.name.as_str())?;
        if cleanup {
            let _ = fs::remove_file(&script_path);
        }
        Some(result)
    } else {
        None
    };

    match output_format {
        OutputFormat::Text => {
            if !quiet {
                println!("MPI smoke service: {}", service.name);
                println!("requested MPI type: {requested_mpi_type}");
                if advertised_mpi_types.is_empty() {
                    println!("advertised MPI types: unavailable");
                } else {
                    println!("advertised MPI types: {}", advertised_mpi_types.join(", "));
                }
                println!("expected ranks: {expected_ranks}");
                println!("host MPI bind paths: {}", host_mpi_bind_paths.len());
                for bind_path in &host_mpi_bind_paths {
                    println!("  bind: {bind_path}");
                }
                println!("host MPI env entries: {}", host_mpi_env.len());
                for (key, value) in &host_mpi_env {
                    println!("  env: {key}={value}");
                }
                println!("rendered srun: {rendered_srun}");
                if let Some(path) = wrote_script.as_ref() {
                    println!("script: {}", path.display());
                }
                if !submit {
                    println!("submit: skipped; pass --submit to run this probe");
                    if wrote_script.is_none() {
                        print!("{script}");
                    }
                }
                if let Some(result) = submit_result.as_ref() {
                    print!("{}", result.render_text());
                }
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&MpiSmokeJsonOutput {
                    service: service.name.clone(),
                    requested_mpi_type,
                    advertised_mpi_types,
                    expected_ranks,
                    host_mpi_bind_paths,
                    host_mpi_env,
                    rendered_srun,
                    submitted: submit,
                    script_path: wrote_script,
                    script: (!submit).then_some(script),
                    result: submit_result.clone(),
                })?
            );
        }
    }

    if let Some(result) = submit_result
        && !result.success
    {
        bail!("MPI smoke probe failed");
    }
    Ok(())
}

fn doctor_cluster_report(
    output_format: OutputFormat,
    binaries: &ResolvedBinaries,
    cluster_report_out: Option<PathBuf>,
) -> Result<()> {
    let generated = generate_cluster_profile(binaries);
    let cwd = std::env::current_dir()?;
    let out_path = cluster_report_out.unwrap_or_else(|| default_cluster_profile_path(&cwd));
    let print_toml = out_path.as_os_str() == "-";
    if !print_toml {
        write_cluster_profile(&out_path, &generated.profile)?;
    }
    match output_format {
        OutputFormat::Text => {
            if print_toml {
                println!("{}", toml::to_string_pretty(&generated.profile)?);
            } else {
                println!("cluster profile: {}", out_path.display());
                print_cluster_profile_summary(&generated.profile);
                if generated.diagnostics.has_warnings() {
                    output::print_report(&generated.diagnostics, false);
                }
            }
        }
        OutputFormat::Json => {
            #[derive(serde::Serialize)]
            struct JsonOutput<'a> {
                path: Option<&'a Path>,
                wrote: bool,
                profile: &'a ClusterProfile,
                diagnostics: hpc_compose::preflight::GroupedReport,
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&JsonOutput {
                    path: (!print_toml).then_some(out_path.as_path()),
                    wrote: !print_toml,
                    profile: &generated.profile,
                    diagnostics: generated.diagnostics.grouped(),
                })?
            );
        }
    }
    Ok(())
}

#[derive(Debug, Clone, serde::Serialize)]
struct MpiSmokeSubmitResult {
    success: bool,
    status: Option<i32>,
    stdout: String,
    stderr: String,
    service_log: Option<String>,
    timed_out: bool,
}

impl MpiSmokeSubmitResult {
    fn render_text(&self) -> String {
        let mut output = String::new();
        output.push_str(&format!(
            "submit: {}\n",
            if self.success { "passed" } else { "failed" }
        ));
        if self.timed_out {
            output.push_str("timeout: yes\n");
        }
        if let Some(status) = self.status {
            output.push_str(&format!("exit status: {status}\n"));
        }
        if !self.stdout.trim().is_empty() {
            output.push_str("stdout:\n");
            output.push_str(&self.stdout);
            if !self.stdout.ends_with('\n') {
                output.push('\n');
            }
        }
        if !self.stderr.trim().is_empty() {
            output.push_str("stderr:\n");
            output.push_str(&self.stderr);
            if !self.stderr.ends_with('\n') {
                output.push('\n');
            }
        }
        if let Some(log) = self.service_log.as_ref()
            && !log.trim().is_empty()
        {
            output.push_str("service log:\n");
            output.push_str(log);
            if !log.ends_with('\n') {
                output.push('\n');
            }
        }
        output
    }
}

#[derive(Debug, serde::Serialize)]
struct MpiSmokeJsonOutput {
    service: String,
    requested_mpi_type: String,
    advertised_mpi_types: Vec<String>,
    expected_ranks: u32,
    host_mpi_bind_paths: Vec<String>,
    host_mpi_env: Vec<(String, String)>,
    rendered_srun: String,
    submitted: bool,
    script_path: Option<PathBuf>,
    script: Option<String>,
    result: Option<MpiSmokeSubmitResult>,
}

fn run_doctor(binaries: &ResolvedBinaries) -> Report {
    let mut items = Vec::new();

    check_slurm(&mut items, binaries);
    check_enroot(&mut items, binaries.enroot.value.as_str());
    check_optional_runtime(&mut items, "apptainer", binaries.apptainer.value.as_str());
    check_optional_runtime(
        &mut items,
        "singularity",
        binaries.singularity.value.as_str(),
    );
    check_pyxis(&mut items, binaries.srun.value.as_str());
    check_gpu(&mut items);
    check_cache_dir(&mut items);
    check_completions(&mut items);

    Report { items }
}

fn print_cluster_profile_summary(profile: &ClusterProfile) {
    println!("  partitions: {}", profile.partitions.len());
    println!(
        "  runtimes: pyxis={} apptainer={} singularity={} host={}",
        profile.runtimes.pyxis,
        profile.runtimes.apptainer,
        profile.runtimes.singularity,
        profile.runtimes.host
    );
    if !profile.mpi_types.is_empty() {
        println!("  mpi: {}", profile.mpi_types.join(", "));
    }
}

fn select_mpi_service<'a>(
    plan: &'a RuntimePlan,
    requested: Option<&str>,
) -> Result<&'a RuntimeService> {
    if let Some(name) = requested {
        let service = plan
            .ordered_services
            .iter()
            .find(|service| service.name == name)
            .with_context(|| format!("service '{name}' was not found in the compose plan"))?;
        if service.slurm.mpi.is_none() {
            bail!("service '{name}' does not define x-slurm.mpi");
        }
        return Ok(service);
    }

    let services = plan
        .ordered_services
        .iter()
        .filter(|service| service.slurm.mpi.is_some())
        .collect::<Vec<_>>();
    match services.as_slice() {
        [service] => Ok(*service),
        [] => bail!("doctor --mpi-smoke requires at least one service with x-slurm.mpi"),
        _ => bail!(
            "doctor --mpi-smoke found multiple MPI services; pass --service with one of: {}",
            services
                .iter()
                .map(|service| service.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn build_mpi_smoke_plan(
    plan: &RuntimePlan,
    service_name: &str,
    expected_ranks: u32,
) -> Result<RuntimePlan> {
    let service = plan
        .ordered_services
        .iter()
        .find(|service| service.name == service_name)
        .with_context(|| format!("service '{service_name}' was not found in the runtime plan"))?;
    let mut smoke_service = service.clone();
    smoke_service.execution = ExecutionSpec::Shell(mpi_smoke_shell(expected_ranks));
    smoke_service.working_dir = None;
    smoke_service.depends_on = Vec::new();
    smoke_service.readiness = None;
    smoke_service.failure_policy = ServiceFailurePolicy::default();
    smoke_service.slurm.prologue = None;
    smoke_service.slurm.epilogue = None;
    smoke_service.slurm.scratch = None;

    let smoke_slurm = SlurmConfig {
        job_name: Some(format!("{}-mpi-smoke", plan.name)),
        partition: plan.slurm.partition.clone(),
        account: plan.slurm.account.clone(),
        qos: plan.slurm.qos.clone(),
        time: plan.slurm.time.clone(),
        nodes: plan.slurm.nodes,
        ntasks: plan.slurm.ntasks,
        ntasks_per_node: plan.slurm.ntasks_per_node,
        cpus_per_task: plan.slurm.cpus_per_task,
        mem: plan.slurm.mem.clone(),
        gres: plan.slurm.gres.clone(),
        gpus: plan.slurm.gpus,
        gpus_per_node: plan.slurm.gpus_per_node,
        gpus_per_task: plan.slurm.gpus_per_task,
        cpus_per_gpu: plan.slurm.cpus_per_gpu,
        mem_per_gpu: plan.slurm.mem_per_gpu.clone(),
        gpu_bind: plan.slurm.gpu_bind.clone(),
        cpu_bind: plan.slurm.cpu_bind.clone(),
        mem_bind: plan.slurm.mem_bind.clone(),
        distribution: plan.slurm.distribution.clone(),
        hint: plan.slurm.hint.clone(),
        constraint: plan.slurm.constraint.clone(),
        ..SlurmConfig::default()
    };
    Ok(RuntimePlan {
        name: format!("{}-mpi-smoke", plan.name),
        cache_dir: plan.cache_dir.clone(),
        runtime: plan.runtime.clone(),
        slurm: smoke_slurm,
        ordered_services: vec![smoke_service],
    })
}

fn mpi_expected_ranks(service: &RuntimeService) -> u32 {
    service
        .slurm
        .mpi
        .as_ref()
        .and_then(|mpi| mpi.expected_ranks)
        .unwrap_or_else(|| resolved_rank_count(service))
}

fn resolved_rank_count(service: &RuntimeService) -> u32 {
    service
        .placement
        .ntasks
        .or_else(|| {
            service
                .placement
                .ntasks_per_node
                .map(|per_node| per_node * service.placement.nodes)
        })
        .unwrap_or(1)
}

fn mpi_smoke_shell(expected_ranks: u32) -> String {
    format!(
        r#"expected_ranks={expected_ranks}
rank="${{SLURM_PROCID:-${{PMI_RANK:-${{PMIX_RANK:-unknown}}}}}}"
size="${{SLURM_NTASKS:-${{PMI_SIZE:-${{PMIX_SIZE:-${{SLURM_STEP_NUM_TASKS:-}}}}}}}}"
echo "hpc-compose MPI smoke rank=$rank size=${{size:-unknown}} expected=$expected_ranks"
echo "observed_rank_count=${{size:-unknown}}"
echo "rank_variables SLURM_PROCID=${{SLURM_PROCID:-}} SLURM_LOCALID=${{SLURM_LOCALID:-}} SLURM_NODEID=${{SLURM_NODEID:-}} PMI_RANK=${{PMI_RANK:-}} PMI_SIZE=${{PMI_SIZE:-}} PMIX_RANK=${{PMIX_RANK:-}} PMIX_NAMESPACE=${{PMIX_NAMESPACE:-}}"
if [ -z "$size" ]; then
  echo "MPI smoke could not determine launched rank count from Slurm/PMI environment" >&2
  exit 17
fi
if [ "$size" != "$expected_ranks" ]; then
  echo "MPI smoke expected $expected_ranks ranks but launch reports $size" >&2
  exit 18
fi
if command -v python3 >/dev/null 2>&1; then
  py_status=0
  python3 - "$expected_ranks" <<'PY' || py_status=$?
import sys
expected = int(sys.argv[1])
try:
    from mpi4py import MPI
except ModuleNotFoundError as exc:
    if exc.name == "mpi4py":
        sys.exit(77)
    raise
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
print(f"mpi4py MPI_Init smoke rank={{rank}} size={{size}} expected={{expected}}", flush=True)
if size != expected:
    sys.exit(19)
PY
  case "$py_status" in
    0) ;;
    77) echo "WARN: mpi4py not installed; Slurm/PMI rank environment smoke passed but MPI_Init was not tested" >&2 ;;
    *) exit "$py_status" ;;
  esac
else
  echo "WARN: python3 not found; Slurm/PMI rank environment smoke passed but MPI_Init was not tested" >&2
fi
"#
    )
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

fn write_script(path: &Path, script: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(path, script).with_context(|| format!("failed to write {}", path.display()))
}

fn temp_smoke_script_path() -> PathBuf {
    env::temp_dir().join(format!(
        "hpc-compose-mpi-smoke-{}-{}.sbatch",
        std::process::id(),
        unix_timestamp_millis()
    ))
}

fn unix_timestamp_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn run_sbatch_wait(
    sbatch_bin: &str,
    script_path: &Path,
    timeout: Duration,
) -> Result<MpiSmokeSubmitResult> {
    let mut child = Command::new(sbatch_bin)
        .arg("--wait")
        .arg(script_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| {
            format!(
                "failed to run '{sbatch_bin} --wait {}'",
                script_path.display()
            )
        })?;
    let start = Instant::now();
    loop {
        if child.try_wait()?.is_some() {
            let output = child.wait_with_output()?;
            return Ok(MpiSmokeSubmitResult {
                success: output.status.success(),
                status: output.status.code(),
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                service_log: None,
                timed_out: false,
            });
        }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let output = child.wait_with_output()?;
            return Ok(MpiSmokeSubmitResult {
                success: false,
                status: output.status.code(),
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                service_log: None,
                timed_out: true,
            });
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn read_smoke_service_log(
    cwd: &Path,
    sbatch_stdout: &str,
    service_name: &str,
) -> Result<Option<String>> {
    let Some(job_id) = parse_submitted_job_id(sbatch_stdout) else {
        return Ok(None);
    };
    let path = cwd
        .join(".hpc-compose")
        .join(job_id)
        .join("logs")
        .join(log_file_name_for_service(service_name));
    match fs::read_to_string(&path) {
        Ok(contents) => Ok(Some(contents)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("failed to read {}", path.display())),
    }
}

fn parse_submitted_job_id(output: &str) -> Option<&str> {
    output
        .split_whitespace()
        .collect::<Vec<_>>()
        .windows(4)
        .find_map(|window| {
            (window[0] == "Submitted" && window[1] == "batch" && window[2] == "job")
                .then(|| window.get(3).copied())
                .flatten()
        })
        .or_else(|| {
            output
                .split_whitespace()
                .find(|token| token.chars().all(|ch| ch.is_ascii_digit()))
        })
}

fn check_slurm(items: &mut Vec<Item>, binaries: &ResolvedBinaries) {
    let sbatch_out = run_capture(binaries.sbatch.value.as_str(), &["--version"]);
    match sbatch_out {
        Some(version) => items.push(Item {
            level: Level::Ok,
            message: format!("sbatch: {version}"),
            remediation: None,
        }),
        None => items.push(Item {
            level: Level::Error,
            message: "sbatch not found".into(),
            remediation: Some("Install Slurm workload manager".into()),
        }),
    }

    let srun_out = run_capture(binaries.srun.value.as_str(), &["--version"]);
    match srun_out {
        Some(version) => items.push(Item {
            level: Level::Ok,
            message: format!("srun: {version}"),
            remediation: None,
        }),
        None => items.push(Item {
            level: Level::Error,
            message: "srun not found".into(),
            remediation: Some("Install Slurm workload manager".into()),
        }),
    }

    let squeue_out = run_capture(binaries.squeue.value.as_str(), &["--version"]);
    match squeue_out {
        Some(version) => items.push(Item {
            level: Level::Ok,
            message: format!("squeue: {version}"),
            remediation: None,
        }),
        None => items.push(Item {
            level: Level::Warn,
            message: "squeue not found".into(),
            remediation: Some("squeue is needed for live status and watch".into()),
        }),
    }

    let sacct_out = run_capture(binaries.sacct.value.as_str(), &["--version"]);
    match sacct_out {
        Some(version) => items.push(Item {
            level: Level::Ok,
            message: format!("sacct: {version}"),
            remediation: None,
        }),
        None => items.push(Item {
            level: Level::Warn,
            message: "sacct not found".into(),
            remediation: Some("sacct is needed for post-job status and stats".into()),
        }),
    }

    let scancel_out = run_capture(binaries.scancel.value.as_str(), &["--version"]);
    match scancel_out {
        Some(version) => items.push(Item {
            level: Level::Ok,
            message: format!("scancel: {version}"),
            remediation: None,
        }),
        None => items.push(Item {
            level: Level::Warn,
            message: "scancel not found".into(),
            remediation: Some("scancel is needed for the cancel/down commands".into()),
        }),
    }
}

fn check_enroot(items: &mut Vec<Item>, enroot_bin: &str) {
    let version = run_capture(enroot_bin, &["version"]);
    match version {
        Some(v) => items.push(Item {
            level: Level::Ok,
            message: format!("enroot: {v}"),
            remediation: None,
        }),
        None => items.push(Item {
            level: Level::Error,
            message: "enroot not found".into(),
            remediation: Some("Install Enroot and ensure 'enroot' is on PATH".into()),
        }),
    }
}

fn check_optional_runtime(items: &mut Vec<Item>, name: &str, runtime_bin: &str) {
    let version =
        run_capture(runtime_bin, &["--version"]).or_else(|| run_capture(runtime_bin, &["version"]));
    match version {
        Some(v) => items.push(Item {
            level: Level::Ok,
            message: format!("{name}: {v}"),
            remediation: None,
        }),
        None => items.push(Item {
            level: Level::Warn,
            message: format!("{name} not found"),
            remediation: Some(format!("Only needed when using runtime.backend={name}")),
        }),
    }
}

fn check_pyxis(items: &mut Vec<Item>, srun_bin: &str) {
    match Command::new(srun_bin).arg("--help").output() {
        Ok(output) => {
            let text = String::from_utf8_lossy(&output.stdout).to_string()
                + &String::from_utf8_lossy(&output.stderr);
            if text.contains("--container-image") {
                items.push(Item {
                    level: Level::Ok,
                    message: "Pyxis: available".into(),
                    remediation: None,
                });
            } else {
                items.push(Item {
                    level: Level::Error,
                    message:
                        "Pyxis not available (srun --help does not advertise --container-image)"
                            .into(),
                    remediation: Some("Install or enable the Pyxis Slurm plugin".into()),
                });
            }
        }
        Err(_) => items.push(Item {
            level: Level::Error,
            message: "Pyxis not available (failed to run srun --help)".into(),
            remediation: Some("Install or enable the Pyxis Slurm plugin".into()),
        }),
    }
}

fn check_gpu(items: &mut Vec<Item>) {
    let output = Command::new("nvidia-smi").arg("-L").output();
    match output {
        Ok(out) if out.status.success() => {
            let count = String::from_utf8_lossy(&out.stdout)
                .lines()
                .filter(|l| l.contains("GPU"))
                .count();
            items.push(Item {
                level: Level::Ok,
                message: format!("GPU: {count} device(s) detected"),
                remediation: None,
            });
        }
        _ => items.push(Item {
            level: Level::Warn,
            message: "nvidia-smi not available".into(),
            remediation: Some("GPU metrics collection requires nvidia-smi".into()),
        }),
    }
}

fn check_cache_dir(items: &mut Vec<Item>) {
    let cache_dir = env::var("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .ok()
        .or_else(|| {
            env::var("HOME")
                .ok()
                .map(|h| PathBuf::from(h).join(".cache"))
        })
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join("hpc-compose");

    if let Err(e) = std::fs::create_dir_all(&cache_dir) {
        items.push(Item {
            level: Level::Error,
            message: format!("cache dir {}: cannot create ({e})", cache_dir.display()),
            remediation: Some("Ensure the cache directory path is writable".into()),
        });
        return;
    }

    let probe = cache_dir.join(".doctor-probe");
    match std::fs::write(&probe, b"test") {
        Ok(()) => {
            let _ = std::fs::remove_file(&probe);
            items.push(Item {
                level: Level::Ok,
                message: format!("cache dir: {} (writable)", cache_dir.display()),
                remediation: None,
            });
        }
        Err(e) => items.push(Item {
            level: Level::Error,
            message: format!("cache dir {}: not writable ({e})", cache_dir.display()),
            remediation: Some("Ensure the cache directory is writable".into()),
        }),
    }
}

fn check_completions(items: &mut Vec<Item>) {
    let home = env::var("HOME").ok().map(PathBuf::from);
    let Some(home) = home else {
        return;
    };

    let shell_rcs = [(home.join(".bashrc"), "bash"), (home.join(".zshrc"), "zsh")];

    let mut found = false;
    for (rc_path, shell) in &shell_rcs {
        if let Ok(contents) = std::fs::read_to_string(rc_path)
            && contents.contains("hpc-compose")
        {
            items.push(Item {
                level: Level::Ok,
                message: format!("shell completions: found in {shell} config"),
                remediation: None,
            });
            found = true;
            break;
        }
    }

    if !found {
        items.push(Item {
            level: Level::Warn,
            message: "shell completions: not found".into(),
            remediation: Some("Run 'hpc-compose completions bash >> ~/.bashrc' or 'hpc-compose completions zsh >> ~/.zshrc'".into()),
        });
    }
}

fn run_capture(bin: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(bin).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if stdout.is_empty() {
        None
    } else {
        Some(stdout)
    }
}

fn print_doctor_report(report: &Report) {
    let grouped = report.grouped();
    for item in &grouped.passed_checks {
        println!("  {} {}", crate::term::symbol_ok(), item.message);
    }
    for item in &grouped.actionable_warnings {
        println!("  {} {}", crate::term::styled_warning("WARN"), item.message);
        if let Some(ref remediation) = item.remediation {
            println!("    remediation: {remediation}");
        }
    }
    for item in &grouped.blockers {
        println!("  {} {}", crate::term::styled_error("FAIL"), item.message);
        if let Some(ref remediation) = item.remediation {
            println!("    remediation: {remediation}");
        }
    }
    println!(
        "\nSummary: {} passed, {} warnings, {} errors",
        grouped.summary.passed_checks,
        grouped.summary.actionable_warnings,
        grouped.summary.blockers
    );
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use hpc_compose::context::{ResolvedValue, ValueSource};
    use hpc_compose::planner::{ImageSource, ServicePlacement};
    use hpc_compose::spec::{MpiConfig, MpiLauncher, MpiType, RuntimeConfig, ServiceSlurmConfig};

    fn resolved_string(value: &str) -> ResolvedValue<String> {
        ResolvedValue {
            value: value.to_string(),
            source: ValueSource::Cli,
        }
    }

    fn resolved_path(value: &Path) -> ResolvedValue<PathBuf> {
        ResolvedValue {
            value: value.to_path_buf(),
            source: ValueSource::Cli,
        }
    }

    fn binaries_with_srun(srun: &Path) -> ResolvedBinaries {
        ResolvedBinaries {
            enroot: resolved_string("/definitely/missing-enroot"),
            apptainer: resolved_string("/definitely/missing-apptainer"),
            singularity: resolved_string("/definitely/missing-singularity"),
            sbatch: resolved_string("/definitely/missing-sbatch"),
            srun: resolved_string(&srun.display().to_string()),
            scontrol: resolved_string("/definitely/missing-scontrol"),
            sinfo: resolved_string("/definitely/missing-sinfo"),
            squeue: resolved_string("/definitely/missing-squeue"),
            sacct: resolved_string("/definitely/missing-sacct"),
            sstat: resolved_string("/definitely/missing-sstat"),
            scancel: resolved_string("/definitely/missing-scancel"),
        }
    }

    fn write_executable(path: &Path, body: &str) {
        fs::write(path, body).expect("write executable");
        let mut perms = fs::metadata(path).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("chmod");
    }

    fn mpi_config(expected_ranks: Option<u32>) -> MpiConfig {
        MpiConfig {
            mpi_type: MpiType::new("pmix").expect("mpi type"),
            implementation: None,
            launcher: MpiLauncher::default(),
            expected_ranks,
            host_mpi: None,
        }
    }

    fn runtime_service(name: &str, mpi: Option<MpiConfig>) -> RuntimeService {
        RuntimeService {
            name: name.into(),
            runtime_image: PathBuf::from(format!("/cache/{name}.sqsh")),
            execution: ExecutionSpec::Shell("echo ready".into()),
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig {
                mpi,
                ..ServiceSlurmConfig::default()
            },
            prepare: None,
            source: ImageSource::Remote("docker://example.com/app:1".into()),
        }
    }

    fn runtime_plan(services: Vec<RuntimeService>) -> RuntimePlan {
        RuntimePlan {
            name: "demo".into(),
            cache_dir: PathBuf::from("/cache"),
            runtime: RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: services,
        }
    }

    #[test]
    fn mpi_service_selection_reports_actionable_errors() {
        let no_mpi = runtime_plan(vec![runtime_service("plain", None)]);
        assert!(
            select_mpi_service(&no_mpi, None)
                .expect_err("no mpi")
                .to_string()
                .contains("requires at least one service")
        );
        assert!(
            select_mpi_service(&no_mpi, Some("missing"))
                .expect_err("missing")
                .to_string()
                .contains("was not found")
        );
        assert!(
            select_mpi_service(&no_mpi, Some("plain"))
                .expect_err("non mpi")
                .to_string()
                .contains("does not define x-slurm.mpi")
        );

        let multiple = runtime_plan(vec![
            runtime_service("a", Some(mpi_config(None))),
            runtime_service("b", Some(mpi_config(None))),
        ]);
        let err = select_mpi_service(&multiple, None).expect_err("multiple mpi services");
        assert!(err.to_string().contains("a, b"));
        assert_eq!(
            select_mpi_service(&multiple, Some("b"))
                .expect("requested service")
                .name,
            "b"
        );
    }

    #[test]
    fn mpi_expected_ranks_prefers_explicit_then_placement() {
        let explicit = runtime_service("explicit", Some(mpi_config(Some(8))));
        assert_eq!(mpi_expected_ranks(&explicit), 8);

        let mut service = runtime_service("ntasks", Some(mpi_config(None)));
        service.placement.ntasks = Some(4);
        assert_eq!(mpi_expected_ranks(&service), 4);

        service.placement.ntasks = None;
        service.placement.ntasks_per_node = Some(3);
        service.placement.nodes = 2;
        assert_eq!(mpi_expected_ranks(&service), 6);

        service.placement.ntasks_per_node = None;
        assert_eq!(mpi_expected_ranks(&service), 1);
    }

    #[test]
    fn sbatch_wait_reports_success_failure_and_timeout() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let script = tmpdir.path().join("job.sbatch");
        fs::write(&script, "#!/bin/sh\n").expect("script");

        let success = tmpdir.path().join("sbatch-success");
        write_executable(
            &success,
            "#!/bin/sh\nprintf 'Submitted batch job 12345\\n'\n",
        );
        let result = run_sbatch_wait(
            success.to_str().expect("success path"),
            &script,
            Duration::from_secs(5),
        )
        .expect("success sbatch");
        assert!(result.success);
        assert_eq!(result.status, Some(0));
        assert_eq!(parse_submitted_job_id(&result.stdout), Some("12345"));

        let failure = tmpdir.path().join("sbatch-failure");
        write_executable(&failure, "#!/bin/sh\nprintf 'boom\\n' >&2\nexit 9\n");
        let result = run_sbatch_wait(
            failure.to_str().expect("failure path"),
            &script,
            Duration::from_secs(5),
        )
        .expect("failed sbatch");
        assert!(!result.success);
        assert_eq!(result.status, Some(9));
        assert!(result.stderr.contains("boom"));

        let slow = tmpdir.path().join("sbatch-slow");
        write_executable(&slow, "#!/bin/sh\nsleep 2\n");
        let result = run_sbatch_wait(
            slow.to_str().expect("slow path"),
            &script,
            Duration::from_millis(100),
        )
        .expect("timeout sbatch");
        assert!(!result.success);
        assert!(result.timed_out);
    }

    #[test]
    fn smoke_result_parsing_text_and_logs_cover_edge_cases() {
        assert_eq!(
            parse_submitted_job_id("Submitted batch job 987\n"),
            Some("987")
        );
        assert_eq!(parse_submitted_job_id("queued 654 status ok"), Some("654"));
        assert_eq!(parse_submitted_job_id("no numeric id"), None);
        assert_eq!(
            advertised_mpi_types("MPI plugin types are: none, pmi2, pmix, pmix_v4"),
            vec!["none", "pmi2", "pmix", "pmix_v4"]
        );

        let rendered = MpiSmokeSubmitResult {
            success: false,
            status: Some(18),
            stdout: "out".into(),
            stderr: "err\n".into(),
            service_log: Some("rank log".into()),
            timed_out: true,
        }
        .render_text();
        assert!(rendered.contains("submit: failed"));
        assert!(rendered.contains("timeout: yes"));
        assert!(rendered.contains("stdout:\nout\n"));
        assert!(rendered.contains("service log:\nrank log\n"));

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log_path = tmpdir
            .path()
            .join(".hpc-compose/222/logs")
            .join(log_file_name_for_service("mpi-app"));
        fs::create_dir_all(log_path.parent().expect("log parent")).expect("log dir");
        fs::write(&log_path, "service output").expect("log");
        assert_eq!(
            read_smoke_service_log(tmpdir.path(), "Submitted batch job 222", "mpi-app")
                .expect("read log")
                .as_deref(),
            Some("service output")
        );
        assert!(
            read_smoke_service_log(tmpdir.path(), "no job id", "mpi-app")
                .expect("no id")
                .is_none()
        );
    }

    #[test]
    fn doctor_mpi_smoke_no_submit_renders_script_with_host_mpi_details() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(
            &compose,
            format!(
                r#"name: demo
services:
  mpi-app:
    image: docker://example.com/mpi:1
    command: /bin/true
    x-slurm:
      ntasks: 2
      mpi:
        type: pmix
        expected_ranks: 2
        host_mpi:
          bind_paths:
            - /opt/mpi:/opt/mpi
          env:
            LD_LIBRARY_PATH: /opt/mpi/lib
x-slurm:
  cache_dir: {}
"#,
                tmpdir.path().join("cache").display()
            ),
        )
        .expect("compose");
        let srun = tmpdir.path().join("srun");
        write_executable(
            &srun,
            "#!/bin/sh\nif [ \"$1\" = \"--mpi=list\" ]; then printf 'MPI types are: pmi2 pmix pmix_v4\\n'; exit 0; fi\nexit 0\n",
        );
        let script_out = tmpdir.path().join("mpi-smoke.sbatch");
        let context = ResolvedContext {
            cwd: tmpdir.path().to_path_buf(),
            settings_path: None,
            settings_base_dir: None,
            selected_profile: None,
            compose_file: resolved_path(&compose),
            binaries: binaries_with_srun(&srun),
            interpolation_vars: BTreeMap::new(),
            interpolation_var_sources: BTreeMap::new(),
        };

        doctor_mpi_smoke(
            context,
            Some(OutputFormat::Json),
            None,
            false,
            Some(script_out.clone()),
            1,
            false,
        )
        .expect("doctor mpi smoke");
        let script = fs::read_to_string(script_out).expect("script");
        assert!(script.contains("expected_ranks=2"));
        assert!(script.contains("--mpi=pmix"));
        assert!(script.contains("/opt/mpi:/opt/mpi"));
        assert!(script.contains("LD_LIBRARY_PATH"));
    }
}
