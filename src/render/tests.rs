use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

use super::*;
use crate::cluster::{ClusterProfile, DistributedProfile, RuntimeAvailability};
use crate::planner::ServicePlacement;
use crate::spec::{
    ComposeSpec, DependencyCondition, DependsOnSpec, EnvironmentSpec, MpiConfig, MpiProfile,
    MpiType, ParallelismConfig, ReadinessSpec, RendezvousRegisterConfig, ResumeConfig,
    RuntimeConfig, RuntimeGpuPolicy, ScratchCleanupPolicy, ScratchConfig, ServiceAssertSpec,
    ServiceDependency, ServiceEnrootConfig, ServiceEventHookSpec, ServiceFailurePolicy,
    ServiceHookContext, ServiceHookEvent, ServiceHookSpec, ServiceRendezvousConfig,
    ServiceRuntimeConfig, ServiceScratchConfig, ServiceSlurmConfig, ServiceSpec, SignalConfig,
    SignalName, SignalShellTarget, SlurmConfig, StageInConfig, StageMode, StageOutConfig,
    StageOutWhen,
};

fn runtime_service() -> RuntimeService {
    RuntimeService {
        name: "worker".into(),
        runtime_image: PathBuf::from("/shared/cache/worker.sqsh"),
        execution: ExecutionSpec::Exec(vec!["/bin/app".into(), "--port".into(), "8080".into()]),
        environment: vec![("A".into(), "B".into())],
        volumes: vec!["/shared/data:/data".into()],
        working_dir: Some("/workspace".into()),
        depends_on: Vec::new(),
        readiness: Some(ReadinessSpec::Tcp {
            host: Some("127.0.0.1".into()),
            port: 8080,
            timeout_seconds: Some(20),
        }),
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: crate::planner::ImageSource::LocalSqsh(PathBuf::from("/shared/cache/worker.sqsh")),
    }
}

fn assert_render_contract(plan: &RuntimePlan) {
    let script = render_script(plan).expect("script");
    assert_bash_syntax(&script);
    assert_eq!(
        script.matches("  register_service ").count(),
        plan.ordered_services.len(),
        "rendered script should register each planned service exactly once"
    );
    for array in [
        "SERVICE_PIDS",
        "SERVICE_NAMES",
        "SERVICE_STEP_NAMES",
        "SERVICE_LOG_PATHS",
        "SERVICE_HEALTHY",
        "SERVICE_COMPLETED_SUCCESSFULLY",
        "SERVICE_READINESS_CONFIGURED",
        "SERVICE_FAILURE_POLICY_MODE",
        "SERVICE_MAX_RESTARTS",
        "SERVICE_BACKOFF_SECONDS",
        "SERVICE_WINDOW_SECONDS",
        "SERVICE_MAX_RESTARTS_IN_WINDOW",
        "SERVICE_RESTART_COUNT",
        "SERVICE_RESTART_FAILURES_IN_WINDOW",
        "SERVICE_RESTART_FAILURE_TIMESTAMPS",
        "SERVICE_LAST_EXIT_CODE",
        "SERVICE_STARTED_AT",
        "SERVICE_FINISHED_AT",
        "SERVICE_FIRST_FAILURE_AT",
        "SERVICE_FIRST_FAILURE_EXIT_CODE",
        "SERVICE_FIRST_FAILURE_NODE",
        "SERVICE_FIRST_FAILURE_RANK",
        "SERVICE_PLACEMENT_MODE",
        "SERVICE_STEP_NODES",
        "SERVICE_STEP_NTASKS",
        "SERVICE_STEP_NTASKS_PER_NODE",
        "SERVICE_STEP_NODELIST",
        "SERVICE_HOST_EPILOGUE_SCRIPTS",
        "SERVICE_HOST_EPILOGUE_RAN",
        "SERVICE_EVENT_HOOK_MANIFESTS",
        "SERVICE_LAUNCH_FNS",
        "SERVICE_DEPENDENTS",
    ] {
        assert!(
            script.contains(&format!("{array}=()")),
            "missing {array} initialization"
        );
        assert!(
            script.contains(&format!("{array}+=(")),
            "missing {array} append in register_service"
        );
    }
}

fn assert_bash_syntax(script: &str) {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let script_path = tmpdir.path().join("rendered.sbatch");
    fs::write(&script_path, script).expect("write rendered script");
    let output = Command::new(bash_executable())
        .arg("-n")
        .arg(&script_path)
        .output()
        .expect("bash -n");
    assert!(
        output.status.success(),
        "rendered script failed bash -n\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn bash_executable() -> &'static std::path::Path {
    static BASH: OnceLock<PathBuf> = OnceLock::new();
    BASH.get_or_init(resolve_test_bash).as_path()
}

fn resolve_test_bash() -> PathBuf {
    if let Some(path) = std::env::var_os("HPC_COMPOSE_TEST_BASH") {
        let path = PathBuf::from(path);
        if bash_supports_associative_arrays(&path) {
            return path;
        }
    }
    for candidate in [
        PathBuf::from("/opt/homebrew/bin/bash"),
        PathBuf::from("/usr/local/bin/bash"),
        PathBuf::from("bash"),
        PathBuf::from("/bin/bash"),
    ] {
        if bash_supports_associative_arrays(&candidate) {
            return candidate;
        }
    }
    PathBuf::from("bash")
}

fn bash_supports_associative_arrays(path: &std::path::Path) -> bool {
    Command::new(path)
        .arg("-c")
        .arg("declare -A h=([x]=y); [[ ${h[x]} == y ]]")
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn embedded_local_srun_shim(script: &str) -> &str {
    let heredoc_start = "cat > \"$HPC_COMPOSE_LOCAL_BIN_DIR/srun\" <<'HPC_COMPOSE_LOCAL_SRUN'\n";
    let body = script
        .split_once(heredoc_start)
        .expect("local launcher should write srun shim")
        .1;
    body.split_once("\nHPC_COMPOSE_LOCAL_SRUN\n")
        .expect("local launcher should terminate srun shim heredoc")
        .0
}

fn write_executable(path: &std::path::Path, body: &str) {
    fs::write(path, body).expect("write script");
    let mut perms = fs::metadata(path).expect("meta").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("chmod");
}

fn write_fake_runtime_srun(tmpdir: &std::path::Path) {
    let srun = tmpdir.join("srun");
    write_executable(
        &srun,
        r#"#!/bin/bash
set -euo pipefail
if [[ "${1:-}" == "--help" ]]; then
  echo "usage: srun --container-image=IMAGE"
  exit 0
fi
container_mounts=""
for arg in "$@"; do
  case "$arg" in
    --container-mounts=*)
      container_mounts="${arg#--container-mounts=}"
      ;;
  esac
done
job_mount=""
IFS=',' read -r -a mount_items <<< "$container_mounts"
for mount in "${mount_items[@]}"; do
  host="${mount%%:*}"
  dest="${mount#*:}"
  if [[ "$dest" == "/hpc-compose/job" ]]; then
    job_mount="$host"
  fi
done
printf 'resume_dir=%s attempt=%s is_resume=%s\n' "${HPC_COMPOSE_RESUME_DIR:-}" "${HPC_COMPOSE_ATTEMPT:-}" "${HPC_COMPOSE_IS_RESUME:-}"
printf 'node_meta=%s|%s|%s|%s\n' "${HPC_COMPOSE_PRIMARY_NODE:-}" "${HPC_COMPOSE_NODE_COUNT:-}" "${HPC_COMPOSE_NODELIST:-}" "${HPC_COMPOSE_NODELIST_FILE:-}"
printf 'service_node_meta=%s|%s|%s|%s\n' "${HPC_COMPOSE_SERVICE_PRIMARY_NODE:-}" "${HPC_COMPOSE_SERVICE_NODE_COUNT:-}" "${HPC_COMPOSE_SERVICE_NODELIST:-}" "${HPC_COMPOSE_SERVICE_NODELIST_FILE:-}"
if [[ -n "$job_mount" ]]; then
  mkdir -p "$job_mount/checkpoints"
  printf 'checkpoint %s\n' "${HPC_COMPOSE_ATTEMPT:-missing}" > "$job_mount/checkpoints/checkpoint-${HPC_COMPOSE_ATTEMPT:-missing}.txt"
fi
"#,
    );
    write_fake_scontrol(tmpdir);
}

fn write_fake_runtime_srun_with_dependency_restart(tmpdir: &std::path::Path) {
    let srun = tmpdir.join("srun");
    write_executable(
        &srun,
        r#"#!/bin/bash
set -euo pipefail
if [[ "${1:-}" == "--help" ]]; then
  echo "usage: srun --container-image=IMAGE"
  exit 0
fi
job_name=""
for arg in "$@"; do
  case "$arg" in
    --job-name=*)
      job_name="${arg#--job-name=}"
      break
      ;;
  esac
done
state_root="${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/fake-runtime-srun"
mkdir -p "$state_root"
key="$(printf '%s' "$job_name" | tr -c 'A-Za-z0-9._-' '_')"
count_file="$state_root/${key}.count"
count=0
if [[ -f "$count_file" ]]; then
  count="$(cat "$count_file")"
fi
count=$((count + 1))
echo "$count" > "$count_file"
case "$job_name" in
  hpc-compose:api)
    if (( count == 1 )); then
      exit 41
    fi
    sleep 2
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
"#,
    );
    write_fake_scontrol(tmpdir);
}

fn write_fake_runtime_srun_exit_sequence(
    tmpdir: &std::path::Path,
    service_name: &str,
    exits: &[i32],
) {
    let srun = tmpdir.join("srun");
    let mut exit_cases = String::new();
    for (index, code) in exits.iter().enumerate() {
        exit_cases.push_str(&format!("    {}) exit {code} ;;\n", index + 1));
    }
    let fallback_exit = exits.last().copied().unwrap_or(0);
    write_executable(
        &srun,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
if [[ "${{1:-}}" == "--help" ]]; then
  echo "usage: srun --container-image=IMAGE"
  exit 0
fi
job_name=""
for arg in "$@"; do
  case "$arg" in
    --job-name=*)
      job_name="${{arg#--job-name=}}"
      break
      ;;
  esac
done
state_root="${{SLURM_SUBMIT_DIR:-$PWD}}/.hpc-compose/fake-runtime-srun"
mkdir -p "$state_root"
key="$(printf '%s' "$job_name" | tr -c 'A-Za-z0-9._-' '_')"
count_file="$state_root/${{key}}.count"
count=0
if [[ -f "$count_file" ]]; then
  count="$(cat "$count_file")"
fi
count=$((count + 1))
echo "$count" > "$count_file"
if [[ "$job_name" == {} ]]; then
  case "$count" in
{}    *) exit {} ;;
  esac
fi
exit 0
"#,
            shell_quote(&format!("hpc-compose:{service_name}")),
            exit_cases,
            fallback_exit
        ),
    );
    write_fake_scontrol(tmpdir);
}

fn write_fake_scontrol(tmpdir: &std::path::Path) {
    let scontrol = tmpdir.join("scontrol");
    write_executable(
        &scontrol,
        r#"#!/bin/bash
set -euo pipefail
if [[ "${1:-}" == "show" && "${2:-}" == "hostnames" ]]; then
  if [[ $# -ge 3 ]]; then
    raw="${3//,/ }"
    for host in $raw; do
      printf '%s\n' "$host"
    done
  fi
  exit 0
fi
echo "unsupported scontrol invocation" >&2
exit 1
"#,
    );
}

fn run_rendered_script_output(
    tmpdir: &std::path::Path,
    script_path: &std::path::Path,
    restart_count: u32,
) -> std::process::Output {
    let mut path = std::ffi::OsString::from(tmpdir.as_os_str());
    path.push(":");
    path.push(std::env::var_os("PATH").unwrap_or_default());
    Command::new(bash_executable())
        .arg(script_path)
        .current_dir(tmpdir)
        .env("PATH", path)
        .env("SLURM_JOB_ID", "12345")
        .env("SLURM_JOB_NODELIST", "node01")
        .env("SLURMD_NODENAME", "node01")
        .env("SLURM_SUBMIT_DIR", tmpdir)
        .env("SLURM_RESTART_COUNT", restart_count.to_string())
        .output()
        .expect("run rendered script")
}

fn run_rendered_script(
    tmpdir: &std::path::Path,
    script_path: &std::path::Path,
    restart_count: u32,
) {
    let output = run_rendered_script_output(tmpdir, script_path, restart_count);
    assert!(
        output.status.success(),
        "script failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn exec_form_preserves_argv() {
    let service = runtime_service();
    let argv = execution_argv(&service.execution, service.working_dir.as_deref());
    assert_eq!(argv[0], "/bin/sh");
    assert_eq!(argv[5], "/bin/app");
    assert_eq!(argv[6], "--port");
    assert_eq!(argv[7], "8080");
}

#[test]
fn local_launcher_strips_sbatch_and_keeps_shims_shell_syntax_valid() {
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            time: Some("00:10:00".into()),
            mem: Some("4G".into()),
            ..SlurmConfig::default()
        },
        ordered_services: vec![runtime_service()],
    };

    let script = render_local_script_with_options(
        &plan,
        "local job 1",
        "/opt/enroot/bin/enroot",
        &LocalRenderOptions {
            dev_reload: true,
            runtime_root: None,
        },
    )
    .expect("local launcher");
    assert_bash_syntax(&script);
    assert_bash_syntax(embedded_local_srun_shim(&script));
    assert!(script.starts_with("#!/bin/bash\nset -euo pipefail\n"));
    assert!(!script.lines().any(|line| line.starts_with("#SBATCH ")));
    assert!(script.contains("export SLURM_JOB_ID='local job 1'"));
    assert!(script.contains("export HPC_COMPOSE_BACKEND_OVERRIDE=\"local\""));
    assert!(script.contains("export HPC_COMPOSE_DEV_CONTROL_DIR="));
    assert!(script.contains("export HPC_COMPOSE_LOCAL_ENROOT_BIN='/opt/enroot/bin/enroot'"));
    assert!(script.contains("cat > \"$HPC_COMPOSE_LOCAL_BIN_DIR/srun\""));
    assert!(script.contains("exec \"$enroot_bin\" start"));
}

#[test]
fn local_launcher_bakes_runtime_root_override_into_job_root() {
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![runtime_service()],
    };
    let script = render_local_script_with_options(
        &plan,
        "local-1",
        "/opt/enroot/bin/enroot",
        &LocalRenderOptions {
            dev_reload: true,
            runtime_root: Some(PathBuf::from("/shared/runs/.hpc-compose")),
        },
    )
    .expect("local launcher");
    // The body's JOB_ROOT and the supervisor dirs must all resolve under the
    // baked override so they match the submission record (no $SLURM_SUBMIT_DIR).
    assert!(script.contains("JOB_ROOT='/shared/runs/.hpc-compose'/\"${SLURM_JOB_ID}\""));
    assert!(
        script
            .contains("HPC_COMPOSE_LOCAL_JOB_ROOT='/shared/runs/.hpc-compose'/\"${SLURM_JOB_ID}\"")
    );
    assert!(script.contains(
        "export HPC_COMPOSE_DEV_CONTROL_DIR=\"$HPC_COMPOSE_LOCAL_JOB_ROOT/dev-control\""
    ));
    assert!(
        script.contains("HPC_COMPOSE_LOCAL_BIN_DIR=\"$HPC_COMPOSE_LOCAL_JOB_ROOT/.local-bin\"")
    );
}

#[test]
fn render_uses_prepared_paths_and_readiness() {
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            time: Some("00:10:00".into()),
            ..SlurmConfig::default()
        },
        ordered_services: vec![runtime_service()],
    };
    let script = render_script(&plan).expect("script");
    assert!(script.contains("/shared/cache/worker.sqsh"));
    assert!(script.contains("wait_for_tcp"));
    assert!(script.contains("/bin/app"));
    assert!(script.contains("--container-image='/shared/cache/worker.sqsh'"));
    assert!(script.contains("--container-env=A"));
    assert!(script.contains("build_pyxis_mounts"));
    assert!(script.contains("$JOB_TMP:/hpc-compose/job"));
    assert!(script.contains("STATE_FILE=\"$JOB_TMP/state.json\""));
    assert!(script.contains("write_state_file"));
    assert!(script.contains("/scratch:/scratch"));
    assert!(script.contains("/usr/lib64/slurm/libslurmfull.so"));
    assert!(script.contains("/etc/slurm/task_prolog.hk:/etc/slurm/task_prolog"));
    // Lifecycle markers make the container-launch gap and the command exit visible
    // inline in the service log (so a run no longer "appears paused" before output).
    assert!(script.contains("container starting via srun"));
    assert!(script.contains("command exited rc=%s"));
}

#[test]
fn job_root_is_baked_literal_when_runtime_root_is_provided() {
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![runtime_service()],
    };

    // Dry-run preview keeps the portable, machine-independent form so previews
    // and their snapshots stay stable.
    let preview = render_script(&plan).expect("preview script");
    assert!(
        preview.contains("JOB_ROOT=\"${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}\"")
    );

    // A real submission bakes the resolved absolute runtime root, so the job
    // body no longer depends on $SLURM_SUBMIT_DIR being set or shared-visible.
    let baked = render_script_with_options(
        &plan,
        &RenderOptions {
            runtime_root: Some(PathBuf::from("/home/u/proj/.hpc-compose")),
            ..RenderOptions::default()
        },
    )
    .expect("baked script");
    assert!(baked.contains("JOB_ROOT='/home/u/proj/.hpc-compose'/\"${SLURM_JOB_ID}\""));
    assert!(
        !baked.contains("SLURM_SUBMIT_DIR:-$PWD"),
        "baked JOB_ROOT must not fall back to $SLURM_SUBMIT_DIR"
    );
}

#[test]
fn default_output_directive_is_baked_only_for_real_submits() {
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![runtime_service()],
    };

    // Preview (runtime_root == None): keep Slurm's default, emit no --output.
    let preview = render_script(&plan).expect("preview script");
    assert!(!preview.contains("#SBATCH --output="));

    // Real submission: bake a literal --output under the hidden job-id-free dir.
    let baked = render_script_with_options(
        &plan,
        &RenderOptions {
            runtime_root: Some(PathBuf::from("/home/u/proj/.hpc-compose")),
            ..RenderOptions::default()
        },
    )
    .expect("baked script");
    assert!(baked.contains("#SBATCH --output=/home/u/proj/.hpc-compose/logs/hpc-compose-%j.out"));

    let mut path_like_name = plan.clone();
    path_like_name.name = "team/demo".into();
    let baked = render_script_with_options(
        &path_like_name,
        &RenderOptions {
            runtime_root: Some(PathBuf::from("/home/u/proj/.hpc-compose")),
            ..RenderOptions::default()
        },
    )
    .expect("path-like name script");
    assert!(
        baked.contains("#SBATCH --output=/home/u/proj/.hpc-compose/logs/hpc-compose-%j.out"),
        "default output must not include raw job names as path components"
    );
}

#[test]
fn runtime_cache_cleanup_defaults_to_never_and_honors_policy() {
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![runtime_service()],
    };
    let script = render_script(&plan).expect("script");
    // Helper is always defined; default policy is a no-op; trap always calls it.
    assert!(script.contains("cleanup_runtime_cache() {"));
    assert!(script.contains("RUNTIME_CACHE_CLEANUP_POLICY='never'"));
    assert!(script.contains("cleanup_runtime_cache \"$code\" || true"));
    assert!(script.contains(
        "rm -rf \"${ENROOT_CACHE_PATH:-}\" \"${ENROOT_DATA_PATH:-}\" \"${ENROOT_TEMP_PATH:-}\""
    ));

    let mut always = plan.clone();
    always.slurm.cleanup.runtime_cache = crate::spec::RuntimeCacheCleanupPolicy::Always;
    let script = render_script(&always).expect("always script");
    assert!(script.contains("RUNTIME_CACHE_CLEANUP_POLICY='always'"));
}

#[test]
fn render_contract_holds_for_single_service_dependency_and_distributed_plans() {
    let single = RuntimePlan {
        name: "single".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![runtime_service()],
    };
    assert_render_contract(&single);

    let mut provider = runtime_service();
    provider.name = "api".into();
    provider.readiness = Some(ReadinessSpec::Sleep { seconds: 1 });
    let mut client = runtime_service();
    client.name = "client".into();
    client.depends_on = vec![ServiceDependency {
        name: "api".into(),
        condition: DependencyCondition::ServiceHealthy,
        implicit: false,
    }];
    client.readiness = None;
    let dependency = RuntimePlan {
        name: "dependency".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![provider, client],
    };
    assert_render_contract(&dependency);

    let mut distributed = runtime_service();
    distributed.name = "trainer".into();
    distributed.placement = ServicePlacement {
        mode: crate::planner::ServicePlacementMode::Distributed,
        nodes: 2,
        ntasks: Some(4),
        ntasks_per_node: Some(2),
        node_indices: Some(vec![0, 1]),
        ..ServicePlacement::default()
    };
    distributed.slurm.mpi = Some(MpiConfig {
        mpi_type: MpiType::new("pmix").expect("mpi type"),
        profile: Some(MpiProfile::Openmpi),
        implementation: None,
        launcher: crate::spec::MpiLauncher::Srun,
        expected_ranks: None,
        host_mpi: None,
    });
    distributed.readiness = Some(ReadinessSpec::Sleep { seconds: 1 });
    let distributed_plan = RuntimePlan {
        name: "distributed".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            nodes: Some(2),
            metrics: Some(crate::spec::MetricsConfig {
                enabled: Some(true),
                interval_seconds: Some(5),
                collectors: vec![MetricsCollector::Slurm],
            }),
            artifacts: Some(crate::spec::ArtifactsConfig {
                collect: ArtifactCollectPolicy::Always,
                export_dir: Some("./results/${SLURM_JOB_ID}".into()),
                paths: vec!["/hpc-compose/job/metrics/**".into()],
                bundles: BTreeMap::new(),
            }),
            ..SlurmConfig::default()
        },
        ordered_services: vec![distributed],
    };
    assert_render_contract(&distributed_plan);
}

#[test]
fn render_includes_http_readiness_helper() {
    let mut service = runtime_service();
    service.readiness = Some(ReadinessSpec::Http {
        url: "http://127.0.0.1:8080/health".into(),
        status_code: 200,
        timeout_seconds: Some(30),
    });
    let plan = RuntimePlan {
        name: "http-test".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let script = render_script(&plan).expect("script");
    assert!(script.contains("wait_for_http()"));
    assert!(script.contains("curl --silent"));
    assert!(
        script.contains("wait_for_http \"$pid\" \"$name\" 'http://127.0.0.1:8080/health' 200 30")
    );
}

#[test]
fn render_apptainer_backend_uses_host_runtime_wrapper() {
    let mut service = runtime_service();
    service.runtime_image = PathBuf::from("/shared/cache/worker.sif");
    service.source = crate::planner::ImageSource::LocalSif(service.runtime_image.clone());
    service.volumes = vec!["/shared/data:/data:ro".into()];
    let plan = RuntimePlan {
        name: "apptainer-demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Apptainer,
            gpu: RuntimeGpuPolicy::Nvidia,
        },
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("local -a runtime_cmd=('apptainer' 'exec')"));
    assert!(script.contains("runtime_cmd+=(--nv)"));
    assert!(script.contains("runtime_cmd+=(--bind \"$runtime_mounts\")"));
    assert!(script.contains("/shared/cache/worker.sif"));
    assert!(!script.contains("--container-image="));
    assert!(!script.contains("--container-env="));
}

#[test]
fn render_apptainer_and_singularity_honor_binary_overrides() {
    let mut service = runtime_service();
    service.runtime_image = PathBuf::from("/shared/cache/worker.sif");
    service.source = crate::planner::ImageSource::LocalSif(service.runtime_image.clone());

    let apptainer_plan = RuntimePlan {
        name: "apptainer-demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Apptainer,
            gpu: RuntimeGpuPolicy::Auto,
        },
        slurm: SlurmConfig::default(),
        ordered_services: vec![service.clone()],
    };
    let script = render_script_with_options(
        &apptainer_plan,
        &RenderOptions {
            apptainer_bin: "/opt/site/bin/apptainer".into(),
            singularity_bin: "/opt/site/bin/singularity".into(),
            huggingface_cli_bin: "huggingface-cli".into(),
            cluster_profile: None,
            runtime_root: None,
            annotate: false,
        },
    )
    .expect("script");
    assert!(script.contains("local -a runtime_cmd=('/opt/site/bin/apptainer' 'exec')"));

    let singularity_plan = RuntimePlan {
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Singularity,
            gpu: RuntimeGpuPolicy::Auto,
        },
        ordered_services: vec![service],
        ..apptainer_plan
    };
    let script = render_script_with_options(
        &singularity_plan,
        &RenderOptions {
            apptainer_bin: "/opt/site/bin/apptainer".into(),
            singularity_bin: "/opt/site/bin/singularity".into(),
            huggingface_cli_bin: "huggingface-cli".into(),
            cluster_profile: None,
            runtime_root: None,
            annotate: false,
        },
    )
    .expect("script");
    assert!(script.contains("local -a runtime_cmd=('/opt/site/bin/singularity' 'exec')"));
}

#[test]
fn render_scratch_staging_and_burst_buffer_directives() {
    let plan = RuntimePlan {
        name: "scratch-demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            scratch: Some(ScratchConfig {
                scope: crate::spec::ScratchScope::Shared,
                base: "/scratch/jobs".into(),
                mount: "/scratch".into(),
                cleanup: ScratchCleanupPolicy::OnSuccess,
            }),
            stage_in: vec![StageInConfig {
                from: Some("/shared/input".into()),
                to: "/scratch/input".into(),
                mode: StageMode::Rsync,
                hf: None,
            }],
            stage_out: vec![StageOutConfig {
                from: "/scratch/output".into(),
                to: "/shared/output/${SLURM_JOB_ID}".into(),
                when: StageOutWhen::Always,
                mode: StageMode::Copy,
            }],
            burst_buffer: Some(crate::spec::BurstBufferConfig {
                directives: vec!["#BB create_persistent name=data capacity=100G".into()],
            }),
            ..SlurmConfig::default()
        },
        ordered_services: vec![runtime_service()],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("#BB create_persistent name=data capacity=100G"));
    assert!(script.contains("SCRATCH_HOST_PATH=\"$SCRATCH_BASE/${SLURM_JOB_ID}\""));
    assert!(script.contains("append_unique_mount \"$SCRATCH_HOST_PATH:$SCRATCH_CONTAINER_PATH\""));
    assert!(script.contains(
            "  local HPC_COMPOSE_SERVICE_SCRATCH_ENABLED=\"$scratch_enabled\"\n  runtime_mounts=$(build_pyxis_mounts \"${service_mounts[@]}\")"
        ));
    assert!(!script.contains(
            "HPC_COMPOSE_SERVICE_SCRATCH_ENABLED=\"$scratch_enabled\" runtime_mounts=$(build_pyxis_mounts"
        ));
    assert!(script.contains("STAGE_IN_FROM=('/shared/input')"));
    assert!(script.contains("STAGE_OUT_TO=('/shared/output/${SLURM_JOB_ID}')"));
    assert!(script.contains("stage_in_paths"));
    assert!(script.contains("stage_out_paths \"$code\" || stage_out_status=$?"));
    assert!(script.contains("if (( stage_out_status == 0 ));"));
    assert!(script.contains("cleanup_scratch \"$code\" || true"));
}

#[test]
fn hf_stage_in_renders_cluster_side_download_not_a_mount() {
    let plan = RuntimePlan {
        name: "hf-demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            stage_in: vec![
                StageInConfig {
                    from: None,
                    to: "/models/llama".into(),
                    mode: StageMode::Rsync,
                    hf: Some(crate::spec::HfStageSource {
                        repo: "meta-llama/Llama-3.1-8B".into(),
                        revision: "abc1234def".into(),
                        kind: crate::spec::HfStageKind::Model,
                    }),
                },
                // A path entry alongside hf stays on the rsync/cp path unchanged.
                StageInConfig {
                    from: Some("/shared/input".into()),
                    to: "/scratch/input".into(),
                    mode: StageMode::Copy,
                    hf: None,
                },
            ],
            ..SlurmConfig::default()
        },
        ordered_services: vec![runtime_service()],
    };

    let script = render_script_with_options(
        &plan,
        &RenderOptions {
            huggingface_cli_bin: "/opt/hf/huggingface-cli".into(),
            ..RenderOptions::default()
        },
    )
    .expect("script");

    // The cluster-side download is emitted with the configured CLI, pinned
    // revision, and a temp --local-dir; never as an hf:// mount argument.
    assert!(
        script.contains(
            "'/opt/hf/huggingface-cli' download 'meta-llama/Llama-3.1-8B' --revision 'abc1234def' --local-dir \"$hf_tmp\""
        ),
        "expected guarded huggingface-cli download line; got:\n{script}"
    );
    // Atomic into the CAS path: temp dir + rename under a best-effort flock so
    // concurrent array/sweep tasks can't corrupt the shared store.
    assert!(script.contains("flock 9"), "flock-guarded; got:\n{script}");
    assert!(
        script.contains("mv \"$hf_tmp\" \"$HF_STAGE_TARGET\""),
        "atomic rename into the CAS dir; got:\n{script}"
    );
    assert!(
        script.contains("/shared/cache/models/"),
        "downloads into CAS path"
    );
    assert!(script.contains("stage_in_huggingface_artifacts"));
    // Guarded so a repeated job reuses the staged weights.
    assert!(script.contains(".hpc-compose-hf-complete"));
    // Never mounts/binds the hf:// URI and never inlines the token.
    assert!(!script.contains("hf://"), "no literal hf:// in the script");
    assert!(
        !script.contains("HF_TOKEN"),
        "HF_TOKEN must never be inlined"
    );
    // The path entry's existing rsync/cp behavior is untouched.
    assert!(script.contains("STAGE_IN_FROM=('/shared/input')"));
}

#[test]
fn hf_stage_in_resolves_scratch_destination_before_copying() {
    let plan = RuntimePlan {
        name: "hf-scratch-demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            scratch: Some(ScratchConfig {
                scope: crate::spec::ScratchScope::Shared,
                base: "/scratch/jobs".into(),
                mount: "/scratch".into(),
                cleanup: ScratchCleanupPolicy::OnSuccess,
            }),
            stage_in: vec![StageInConfig {
                from: None,
                to: "/scratch/models/llama".into(),
                mode: StageMode::Rsync,
                hf: Some(crate::spec::HfStageSource {
                    repo: "meta-llama/Llama-3.1-8B".into(),
                    revision: "abc1234def".into(),
                    kind: crate::spec::HfStageKind::Model,
                }),
            }],
            ..SlurmConfig::default()
        },
        ordered_services: vec![runtime_service()],
    };

    let script = render_script(&plan).expect("script");

    assert!(
        script.contains("hf_stage_to=$(scratch_host_path_for '/scratch/models/llama')"),
        "hf stage-in destination should use scratch host mapping; got:\n{script}"
    );
    assert!(
        script.contains("stage_copy_path \"$HF_STAGE_TARGET\"/. \"$hf_stage_to\" copy"),
        "hf stage-in should copy into the resolved destination; got:\n{script}"
    );
    assert!(
        !script.contains("stage_copy_path \"$HF_STAGE_TARGET\"/. '/scratch/models/llama' copy"),
        "hf stage-in should not copy to the literal scratch container path"
    );
}

#[test]
fn render_scratch_without_staging_defines_cleanup_helpers() {
    let plan = RuntimePlan {
        name: "scratch-only".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            scratch: Some(ScratchConfig {
                scope: crate::spec::ScratchScope::Shared,
                base: "/scratch/jobs".into(),
                mount: "/scratch".into(),
                cleanup: ScratchCleanupPolicy::Always,
            }),
            ..SlurmConfig::default()
        },
        ordered_services: vec![runtime_service()],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("init_scratch()"));
    assert!(script.contains("cleanup_scratch()"));
    assert!(script.contains("cleanup_scratch \"$code\" || true"));
}

#[test]
fn render_host_runtime_exposes_host_scratch_path_to_service_env() {
    let mut service = runtime_service();
    service.source = crate::planner::ImageSource::Host;
    service.runtime_image = PathBuf::new();
    let plan = RuntimePlan {
        name: "host-scratch".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Host,
            ..RuntimeConfig::default()
        },
        slurm: SlurmConfig {
            scratch: Some(ScratchConfig {
                scope: crate::spec::ScratchScope::Shared,
                base: "/scratch/jobs".into(),
                mount: "/work".into(),
                cleanup: ScratchCleanupPolicy::Always,
            }),
            ..SlurmConfig::default()
        },
        ordered_services: vec![service],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("HPC_COMPOSE_SCRATCH_DIR=$SCRATCH_HOST_PATH"));
    assert!(!script.contains("HPC_COMPOSE_SCRATCH_DIR=$SCRATCH_CONTAINER_PATH"));
    assert!(script.contains("local -a runtime_cmd=(\"${service_cmd[@]}\")"));
}

#[test]
fn render_exposes_job_dir_pointing_at_the_real_path_per_backend() {
    // Host backend: no bind mount exists at /hpc-compose/job, so services must be
    // pointed at the real on-node job directory ($JOB_TMP) instead.
    let mut host_service = runtime_service();
    host_service.source = crate::planner::ImageSource::Host;
    host_service.runtime_image = PathBuf::new();
    let host_plan = RuntimePlan {
        name: "job-dir-host".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Host,
            ..RuntimeConfig::default()
        },
        slurm: SlurmConfig::default(),
        ordered_services: vec![host_service],
    };
    let host_script = render_script(&host_plan).expect("host script");
    assert!(host_script.contains("launch_env+=(\"HPC_COMPOSE_JOB_DIR=$JOB_TMP\")"));
    assert!(!host_script.contains("HPC_COMPOSE_JOB_DIR=/hpc-compose/job"));

    // Container backends bind-mount $JOB_TMP at /hpc-compose/job, so that is the
    // path services see.
    let container_plan = RuntimePlan {
        name: "job-dir-container".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Pyxis,
            ..RuntimeConfig::default()
        },
        slurm: SlurmConfig::default(),
        ordered_services: vec![runtime_service()],
    };
    let container_script = render_script(&container_plan).expect("container script");
    assert!(container_script.contains("launch_env+=(\"HPC_COMPOSE_JOB_DIR=/hpc-compose/job\")"));
    assert!(!container_script.contains("HPC_COMPOSE_JOB_DIR=$JOB_TMP"));
}

#[test]
fn render_service_scratch_disabled_resolves_to_runtime_disabled_flag() {
    let mut service = runtime_service();
    service.slurm.scratch = Some(ServiceScratchConfig {
        enabled: Some(false),
    });
    let plan = RuntimePlan {
        name: "scratch-disabled".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            scratch: Some(ScratchConfig {
                scope: crate::spec::ScratchScope::Shared,
                base: "/scratch/jobs".into(),
                mount: "/scratch".into(),
                cleanup: ScratchCleanupPolicy::Always,
            }),
            ..SlurmConfig::default()
        },
        ordered_services: vec![service],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("  local scratch_enabled=0\n"));
    assert!(script.contains("HPC_COMPOSE_SERVICE_SCRATCH_ENABLED=\"$scratch_enabled\""));
}

#[test]
fn render_node_local_multi_node_scratch_initializes_every_node() {
    let plan = RuntimePlan {
        name: "node-local-scratch".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            nodes: Some(2),
            scratch: Some(ScratchConfig {
                scope: crate::spec::ScratchScope::NodeLocal,
                base: "/scratch/jobs".into(),
                mount: "/scratch".into(),
                cleanup: ScratchCleanupPolicy::Always,
            }),
            stage_in: vec![StageInConfig {
                from: Some("/shared/input".into()),
                to: "/scratch/input".into(),
                mode: StageMode::Copy,
                hf: None,
            }],
            stage_out: vec![StageOutConfig {
                from: "/scratch/output".into(),
                to: "/shared/output".into(),
                when: StageOutWhen::Always,
                mode: StageMode::Copy,
            }],
            ..SlurmConfig::default()
        },
        ordered_services: vec![runtime_service()],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("SCRATCH_SCOPE='node_local'"));
    assert!(script.contains(
            "srun --nodes=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks-per-node=1 bash -lc \"$command\" bash \"$SCRATCH_HOST_PATH\""
        ));
    assert!(script.contains(
            "srun --nodes=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks-per-node=1 bash \"$stage_in_node_script\" \"$SCRATCH_CONTAINER_PATH\" \"$SCRATCH_HOST_PATH\""
        ));
    assert!(script.contains(
            "srun --nodes=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks-per-node=1 bash \"$stage_out_node_script\" \"$exit_code\" \"$SCRATCH_CONTAINER_PATH\" \"$SCRATCH_HOST_PATH\""
        ));
}

#[test]
fn shell_form_uses_sh_lc() {
    let argv = execution_argv(&ExecutionSpec::Shell("echo hi".into()), None);
    assert_eq!(argv, vec!["/bin/sh", "-lc", "echo hi"]);
}

#[test]
fn image_default_services_enable_container_entrypoint() {
    let service = RuntimeService {
        name: "redis".into(),
        runtime_image: PathBuf::from("/shared/cache/redis.sqsh"),
        execution: ExecutionSpec::ImageDefault,
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: None,
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: crate::planner::ImageSource::LocalSqsh(PathBuf::from("/shared/cache/redis.sqsh")),
    };
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("'--container-entrypoint'"));
}

#[test]
fn render_covers_optional_slurm_fields_and_setup_lines() {
    let service = RuntimeService {
        name: "logger".into(),
        runtime_image: PathBuf::from("/shared/cache/logger.sqsh"),
        execution: ExecutionSpec::Shell("echo ready".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: Some(ReadinessSpec::Log {
            pattern: "ready".into(),
            timeout_seconds: None,
        }),
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig {
            cpus_per_task: Some(3),
            gpus: Some(2),
            gres: None,
            extra_srun_args: vec!["--mpi=none".into()],
            failure_policy: None,
            ..ServiceSlurmConfig::default()
        },
        prepare: None,
        source: crate::planner::ImageSource::Remote("docker://redis:7".into()),
    };
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            partition: Some("gpu".into()),
            account: Some("proj".into()),
            qos: Some("normal".into()),
            reservation: Some("maint_2026".into()),
            licenses: Some("ansys:2,comsol:1".into()),
            time: Some("01:00:00".into()),
            cpus_per_task: Some(8),
            mem: Some("32G".into()),
            gres: Some("gpu:a100:1".into()),
            constraint: Some("a100".into()),
            output: Some("job.out".into()),
            error: Some("job.err".into()),
            chdir: Some("/work".into()),
            requeue: Some(true),
            setup: vec!["module load enroot".into()],
            submit_args: vec!["--mail-type=END".into()],
            ..SlurmConfig::default()
        },
        ordered_services: vec![service],
    };
    let script = render_script(&plan).expect("script");
    for expected in [
        "#SBATCH --partition=gpu",
        "#SBATCH --account=proj",
        "#SBATCH --qos=normal",
        "#SBATCH --reservation=maint_2026",
        "#SBATCH --licenses=ansys:2,comsol:1",
        "#SBATCH --time=01:00:00",
        "#SBATCH --cpus-per-task=8",
        "#SBATCH --mem=32G",
        "#SBATCH --gres=gpu:a100:1",
        "#SBATCH --constraint=a100",
        "#SBATCH --output=job.out",
        "#SBATCH --error=job.err",
        "#SBATCH --chdir=/work",
        "#SBATCH --requeue",
        "#SBATCH --mail-type=END",
        "module load enroot",
        "wait_for_log",
        "--mpi=none",
        "--gpus=2",
    ] {
        assert!(script.contains(expected), "missing {expected}");
    }
    let time_pos = script.find("#SBATCH --time=01:00:00").expect("time");
    let strict_pos = script.find("set -euo pipefail").expect("strict mode");
    assert!(
        time_pos < strict_pos,
        "SBATCH header must precede shell commands"
    );
}

fn signal_plan(slurm: SlurmConfig) -> RuntimePlan {
    RuntimePlan {
        name: "preempt".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: RuntimeConfig::default(),
        slurm,
        ordered_services: vec![runtime_service()],
    }
}

#[test]
fn render_requeue_false_emits_no_requeue_directive() {
    let script = render_script(&signal_plan(SlurmConfig {
        requeue: Some(false),
        ..SlurmConfig::default()
    }))
    .expect("script");
    assert!(script.contains("#SBATCH --no-requeue"));
    assert!(!script.contains("#SBATCH --requeue\n"));
}

#[test]
fn render_signal_step_mode_omits_batch_prefix_and_installs_no_trap() {
    let script = render_script(&signal_plan(SlurmConfig {
        signal: Some(SignalConfig {
            name: SignalName::Usr1,
            at_seconds: 60,
            shell: SignalShellTarget::Step,
        }),
        ..SlurmConfig::default()
    }))
    .expect("script");
    // Step delivery reaches the job step directly: no B: prefix, no trap/fn.
    assert!(script.contains("#SBATCH --signal=USR1@60"));
    assert!(!script.contains("B:USR1"));
    assert!(!script.contains("forward_configured_signal"));
}

#[test]
fn render_signal_batch_mode_adds_b_prefix_and_forwarding_trap() {
    let script = render_script(&signal_plan(SlurmConfig {
        signal: Some(SignalConfig {
            name: SignalName::Usr1,
            at_seconds: 120,
            shell: SignalShellTarget::Batch,
        }),
        ..SlurmConfig::default()
    }))
    .expect("script");
    assert!(script.contains("#SBATCH --signal=B:USR1@120"));
    // A non-exiting forwarding fn plus a trap that relays USR1 to services.
    assert!(script.contains("forward_configured_signal() {"));
    assert!(script.contains("kill -s \"$sig\" \"$pid\""));
    assert!(script.contains("trap 'forward_configured_signal USR1' USR1"));
    // The forwarding trap must not exit or set RECEIVED_SIGNAL (job keeps running).
    assert!(!script.contains("forward_configured_signal USR1; exit"));
}

#[test]
fn render_signal_batch_mode_skips_extra_trap_for_int_and_term() {
    let script = render_script(&signal_plan(SlurmConfig {
        signal: Some(SignalConfig {
            name: SignalName::Term,
            at_seconds: 90,
            shell: SignalShellTarget::Batch,
        }),
        ..SlurmConfig::default()
    }))
    .expect("script");
    assert!(script.contains("#SBATCH --signal=B:TERM@90"));
    // The existing TERM teardown trap handles graceful shutdown; no extra fn/trap.
    assert!(!script.contains("forward_configured_signal"));
    assert_eq!(
        script.matches("' TERM\n").count(),
        1,
        "only the existing teardown TERM trap should be installed"
    );
}

#[test]
fn render_omits_signal_and_requeue_directives_when_unset() {
    let script = render_script(&signal_plan(SlurmConfig::default())).expect("script");
    assert!(!script.contains("#SBATCH --requeue"));
    assert!(!script.contains("#SBATCH --no-requeue"));
    assert!(!script.contains("#SBATCH --signal"));
    assert!(!script.contains("forward_configured_signal"));
}

#[test]
fn render_gres_takes_precedence_over_gpus_at_allocation_and_service_levels() {
    let service = RuntimeService {
        name: "trainer".into(),
        runtime_image: PathBuf::from("/shared/cache/trainer.sqsh"),
        execution: ExecutionSpec::Exec(vec!["python".into(), "train.py".into()]),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: None,
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig {
            gpus: Some(8),
            gres: Some("gpu:h100:4".into()),
            ..ServiceSlurmConfig::default()
        },
        prepare: None,
        source: crate::planner::ImageSource::LocalSqsh(PathBuf::from("/shared/cache/trainer.sqsh")),
    };
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            gpus: Some(8),
            gres: Some("gpu:h100:4".into()),
            ..SlurmConfig::default()
        },
        ordered_services: vec![service],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("#SBATCH --gres=gpu:h100:4"));
    assert!(!script.contains("#SBATCH --gpus=8"));

    let args = display_srun_command(&plan.ordered_services[0]);
    assert!(args.contains(&"--gres=gpu:h100:4".to_string()));
    assert!(!args.contains(&"--gpus=8".to_string()));
}

#[test]
fn render_gres_precedence_preserves_gpu_subresource_directives() {
    let mut service = runtime_service();
    service.name = "trainer".into();
    service.slurm = ServiceSlurmConfig {
        gpus: Some(8),
        gres: Some("gpu:h100:4".into()),
        gpus_per_node: Some(2),
        gpus_per_task: Some(1),
        ..ServiceSlurmConfig::default()
    };
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            gpus: Some(8),
            gres: Some("gpu:h100:4".into()),
            gpus_per_node: Some(2),
            gpus_per_task: Some(1),
            ..SlurmConfig::default()
        },
        ordered_services: vec![service],
    };

    let script = render_script(&plan).expect("script");
    for expected in [
        "#SBATCH --gres=gpu:h100:4",
        "#SBATCH --gpus-per-node=2",
        "#SBATCH --gpus-per-task=1",
    ] {
        assert!(script.contains(expected), "missing {expected}");
    }
    assert!(!script.contains("#SBATCH --gpus=8"));

    let args = display_srun_command(&plan.ordered_services[0]);
    assert!(args.contains(&"--gres=gpu:h100:4".to_string()));
    assert!(args.contains(&"--gpus-per-node=2".to_string()));
    assert!(args.contains(&"--gpus-per-task=1".to_string()));
    assert!(!args.contains(&"--gpus=8".to_string()));
}

#[test]
fn display_srun_command_shows_partition_indices_and_excludes() {
    let mut service = runtime_service();
    service.placement = ServicePlacement {
        mode: ServicePlacementMode::Partitioned,
        nodes: 3,
        ntasks: Some(3),
        ntasks_per_node: None,
        pin_to_primary_node: false,
        node_indices: Some(vec![2, 4, 5]),
        exclude_indices: vec![3, 6],
        allow_overlap: false,
    };

    let display = display_srun_command(&service);
    assert!(display.contains(&"--nodelist=<allocation-indices:2,4,5>".to_string()));
    assert!(display.contains(&"--exclude=<allocation-indices:3,6>".to_string()));

    let actual = build_srun_command(&service);
    assert!(
        actual
            .iter()
            .all(|arg| !arg.contains("<allocation-indices:"))
    );
}

#[test]
fn render_rendezvous_registration_serializes_configured_metadata() {
    let mut service = runtime_service();
    service.name = "api".into();
    service.readiness = None;
    service.slurm.rendezvous = Some(ServiceRendezvousConfig {
        register: Some(RendezvousRegisterConfig {
            name: "api".into(),
            port: 8080,
            protocol: Some("http".into()),
            path: Some("/ready".into()),
            ttl_seconds: Some(60),
            metadata: BTreeMap::from([
                ("role".into(), "api".into()),
                ("version".into(), "v1".into()),
            ]),
        }),
    });
    let plan = RuntimePlan {
        name: "rdzv".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };

    let script = render_script(&plan).expect("script");
    assert!(
        script.contains(
            "SERVICE_RDZV_METADATA_JSON[rdzv_index]='{\"role\":\"api\",\"version\":\"v1\"}'"
        ),
        "{script}"
    );
    assert!(script.contains("\"metadata\": $metadata_json"));
    assert!(!script.contains("\"metadata\": {}"));
}

#[test]
fn render_emits_host_and_container_hook_lifecycle() {
    let mut service = runtime_service();
    service.name = "trainer".into();
    service.slurm.prologue = Some(ServiceHookSpec {
        context: ServiceHookContext::Host,
        script: "echo host-prologue \"$HPC_COMPOSE_SERVICE_NAME\"".into(),
    });
    service.slurm.epilogue = Some(ServiceHookSpec {
        context: ServiceHookContext::Container,
        script: "echo container-epilogue \"$HPC_COMPOSE_SERVICE_EXIT_CODE\"".into(),
    });
    let plan = RuntimePlan {
        name: "hooks".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("HOOKS_DIR=\"$JOB_TMP/hooks\""));
    assert!(script.contains("trainer.host-prologue.sh"));
    assert!(script.contains("trainer.container-epilogue.sh"));
    assert!(script.contains("trainer.container-wrapper.sh"));
    assert!(script.contains("run_host_hook \"$host_prologue_script\""));
    assert!(script.contains("service_cmd=(\"/bin/sh\" \"$container_wrapper\""));
    assert!(script.contains("HPC_COMPOSE_SERVICE_LOG=/hpc-compose/job/logs/trainer.log"));
    assert!(script.contains(">>\"$logfile\" 2>&1\n"));
    assert!(script.contains("    ) &"));
}

#[test]
fn readiness_and_argv_helpers_cover_remaining_branches() {
    let mut out = String::new();
    let sleep_service = RuntimeService {
        name: "sleepy".into(),
        runtime_image: PathBuf::from("/tmp/sleepy.sqsh"),
        execution: ExecutionSpec::Shell("echo hi".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: Some(ReadinessSpec::Sleep { seconds: 3 }),
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: crate::planner::ImageSource::Remote("docker://redis:7".into()),
    };
    render_readiness_wait(&mut out, &sleep_service);
    assert!(out.contains("wait_for_sleep \"$pid\" \"$name\" 3"));

    let mut out = String::new();
    let tcp_default_service = RuntimeService {
        readiness: Some(ReadinessSpec::Tcp {
            host: None,
            port: 9000,
            timeout_seconds: None,
        }),
        ..sleep_service.clone()
    };
    render_readiness_wait(&mut out, &tcp_default_service);
    assert!(out.contains("'127.0.0.1' 9000 60"));

    let mut out = String::new();
    let http_service = RuntimeService {
        readiness: Some(ReadinessSpec::Http {
            url: "http://127.0.0.1:8080/health".into(),
            status_code: 200,
            timeout_seconds: Some(30),
        }),
        ..sleep_service.clone()
    };
    render_readiness_wait(&mut out, &http_service);
    assert!(out.contains("wait_for_http"));
    assert!(out.contains("'http://127.0.0.1:8080/health'"));
    assert!(out.contains("200 30"));

    let mut out = String::new();
    let http_default_service = RuntimeService {
        readiness: Some(ReadinessSpec::Http {
            url: "http://localhost:5000/ready".into(),
            status_code: 200,
            timeout_seconds: None,
        }),
        ..sleep_service.clone()
    };
    render_readiness_wait(&mut out, &http_default_service);
    assert!(out.contains("200 60"));

    assert_eq!(
        execution_argv(&ExecutionSpec::ImageDefault, None),
        Vec::<String>::new()
    );
    assert_eq!(
        execution_argv(&ExecutionSpec::Exec(vec!["python".into()]), None),
        vec!["python".to_string()]
    );
    assert_eq!(
        execution_argv(&ExecutionSpec::Shell("echo hi".into()), Some("/work"))[2],
        "cd \"$1\" && shift && exec /bin/sh -lc \"$1\""
    );
}

#[test]
fn build_srun_command_and_string_helpers_cover_remaining_cases() {
    let service = RuntimeService {
        name: "redis/service".into(),
        runtime_image: PathBuf::from("/shared/cache/redis.sqsh"),
        execution: ExecutionSpec::ImageDefault,
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: None,
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig {
            cpus_per_task: Some(2),
            gpus: None,
            gres: Some("gpu:1".into()),
            extra_srun_args: vec!["--exclusive".into()],
            failure_policy: None,
            ..ServiceSlurmConfig::default()
        },
        prepare: None,
        source: crate::planner::ImageSource::LocalSqsh(PathBuf::from("/shared/cache/redis.sqsh")),
    };
    let args = build_srun_command(&service);
    assert!(args.contains(&"--container-entrypoint".to_string()));
    assert!(args.contains(&"--job-name=hpc-compose:redis_x2f_service".to_string()));
    assert!(args.contains(&"--cpus-per-task=2".to_string()));
    assert!(args.contains(&"--gres=gpu:1".to_string()));
    assert!(args.contains(&"--exclusive".to_string()));

    assert_eq!(
        bash_array_literal(&["a".into(), "b c".into()]),
        "('a' 'b c')"
    );
    assert_eq!(service_token("redis/service"), "redis_x2f_service");
    assert_eq!(service_token("redis_service"), "redis_x5f_service");
    assert_eq!(
        service_step_name("redis/service"),
        "hpc-compose:redis_x2f_service"
    );
    assert_eq!(shell_quote(""), "''");
    assert_eq!(shell_quote("a'b"), "'a'\"'\"'b'");
}

#[test]
fn render_mpi_service_emits_srun_flag_hostfile_and_env_passthrough() {
    let mut service = runtime_service();
    service.name = "mpi".into();
    service.slurm.mpi = Some(MpiConfig {
        mpi_type: MpiType::new("pmix").expect("mpi type"),
        profile: Some(MpiProfile::Openmpi),
        implementation: None,
        launcher: Default::default(),
        expected_ranks: None,
        host_mpi: None,
    });
    service.placement = ServicePlacement {
        mode: ServicePlacementMode::Distributed,
        nodes: 2,
        ntasks: None,
        ntasks_per_node: Some(4),
        pin_to_primary_node: false,
        node_indices: None,
        exclude_indices: Vec::new(),
        allow_overlap: false,
    };
    let plan = RuntimePlan {
        name: "mpi-demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            nodes: Some(2),
            ..SlurmConfig::default()
        },
        ordered_services: vec![service.clone()],
    };

    let args = build_srun_command(&service);
    assert!(args.contains(&"--mpi=pmix".to_string()));
    let container_env = args
        .iter()
        .find(|arg| arg.starts_with("--container-env="))
        .expect("container env");
    assert!(container_env.contains("HPC_COMPOSE_MPI_HOSTFILE"));
    assert!(container_env.contains("HPC_COMPOSE_MPI_IMPLEMENTATION"));
    assert!(container_env.contains("HPC_COMPOSE_MPI_PROFILE"));
    assert!(container_env.contains("HPC_COMPOSE_MPI_TYPE"));
    assert!(container_env.contains("PMIX_RANK"));
    assert!(container_env.contains("PMI_RANK"));
    assert!(container_env.contains("SLURM_PROCID"));

    let script = render_script(&plan).expect("script");
    assert!(script.contains("MPI_HOSTFILE_DIR=\"$ALLOCATION_DIR/mpi-hostfiles\""));
    assert!(script.contains("write_mpi_hostfile()"));
    assert!(script.contains("local mpi_hostfile=\"$MPI_HOSTFILE_DIR/mpi.hostfile\""));
    assert!(script.contains("write_mpi_hostfile \"$mpi_hostfile\" \"$service_nodelist\" '4'"));
    assert!(script.contains("launch_env+=(\"HPC_COMPOSE_MPI_HOSTFILE=$mpi_hostfile_container\")"));
    assert!(script.contains("launch_env+=(\"HPC_COMPOSE_MPI_TYPE=$mpi_type\")"));
    assert!(script.contains("HPC_COMPOSE_MPI_PROFILE=openmpi"));
    assert!(script.contains("HPC_COMPOSE_MPI_IMPLEMENTATION=openmpi"));
}

#[test]
fn distributed_env_derives_nproc_from_overrides_gpu_and_tasks() {
    let mut service = runtime_service();
    service.placement = ServicePlacement {
        mode: ServicePlacementMode::Distributed,
        nodes: 2,
        ntasks: None,
        ntasks_per_node: Some(3),
        pin_to_primary_node: false,
        node_indices: None,
        exclude_indices: Vec::new(),
        allow_overlap: false,
    };
    let slurm = SlurmConfig::default();

    service.slurm.gres = Some("gpu:a100:4".into());
    assert_eq!(derive_nproc_per_node(&service, &slurm), 4);

    service.slurm.gres = None;
    service.slurm.gpus = Some(8);
    assert_eq!(derive_nproc_per_node(&service, &slurm), 4);

    service.slurm.gpus = None;
    assert_eq!(derive_nproc_per_node(&service, &slurm), 3);

    service
        .environment
        .push(("HPC_COMPOSE_DIST_NPROC_PER_NODE".into(), "6".into()));
    service.slurm.gpus_per_node = Some(4);
    assert_eq!(derive_nproc_per_node(&service, &slurm), 6);

    assert_eq!(parse_gres_gpu_count("gpu:tesla:8"), Some(8));
    assert_eq!(parse_gres_gpu_count("gres/gpu:h100:2"), Some(2));
    assert_eq!(parse_gres_gpu_count("gpu"), Some(1));
    assert_eq!(parse_gres_gpu_count("cpu:4"), None);
}

#[test]
fn render_distributed_service_emits_helpers_and_profile_env() {
    let mut service = runtime_service();
    service.name = "trainer".into();
    service.environment = vec![("NCCL_DEBUG".into(), "INFO".into())];
    service.placement = ServicePlacement {
        mode: ServicePlacementMode::Distributed,
        nodes: 2,
        ntasks: None,
        ntasks_per_node: Some(1),
        pin_to_primary_node: false,
        node_indices: None,
        exclude_indices: Vec::new(),
        allow_overlap: false,
    };
    service.slurm.gpus_per_node = Some(4);
    let profile = ClusterProfile {
        schema_version: 1,
        generated_at_unix: None,
        slurm_version: None,
        mpi_types: Vec::new(),
        mpi_installations: Vec::new(),
        partitions: Vec::new(),
        qos: Vec::new(),
        gpu_models: Vec::new(),
        runtimes: RuntimeAvailability::default(),
        shared_cache_paths: Vec::new(),
        distributed: DistributedProfile {
            rdzv_port: None,
            rdzv_port_base: Some(31_000),
            rdzv_port_span: Some(17),
            env: BTreeMap::from([
                ("FI_PROVIDER".into(), "efa".into()),
                ("NCCL_DEBUG".into(), "WARN".into()),
                ("UCX_TLS".into(), "rc,cuda_copy,cuda_ipc".into()),
            ]),
        },
        ..ClusterProfile::default()
    };
    let plan = RuntimePlan {
        name: "dist-demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            nodes: Some(2),
            ..SlurmConfig::default()
        },
        ordered_services: vec![service.clone()],
    };

    let script = render_script_with_options(
        &plan,
        &RenderOptions {
            cluster_profile: Some(profile.clone()),
            runtime_root: None,
            ..RenderOptions::default()
        },
    )
    .expect("script");
    assert!(script.contains("DIST_HOSTFILE_DIR=\"$ALLOCATION_DIR/distributed-hostfiles\""));
    assert!(script.contains("local dist_nproc_per_node=4"));
    assert!(script.contains("hpc_compose_dist_port '' 31000 17 0"));
    assert!(
        script.contains("HPC_COMPOSE_DIST_RDZV_ENDPOINT=$service_primary_node:$dist_master_port")
    );
    assert!(script.contains(
        "write_mpi_hostfile \"$dist_hostfile\" \"$service_nodelist\" \"$dist_nproc_per_node\""
    ));
    assert!(script.contains("export HPC_COMPOSE_DIST_NODE_RANK="));
    assert!(script.contains("FI_PROVIDER=efa"));
    assert!(script.contains("UCX_TLS=rc,cuda_copy,cuda_ipc"));
    assert!(!script.contains("NCCL_DEBUG=WARN"));

    let srun_args = build_srun_command_for_backend(&service, crate::spec::RuntimeBackend::Pyxis);
    let container_env = srun_args
        .iter()
        .find(|arg| arg.starts_with("--container-env="))
        .expect("container env");
    assert!(container_env.contains("SLURM_LOCALID"));
    assert!(container_env.contains("SLURM_NODEID"));
    assert!(container_env.contains("SLURM_PROCID"));

    let env_names = distributed_environment_names_for_service(&service, Some(&profile));
    assert!(env_names.contains(&"HPC_COMPOSE_DIST_WORLD_SIZE".to_string()));
    assert!(env_names.contains(&"FI_PROVIDER".to_string()));
    assert!(env_names.contains(&"UCX_TLS".to_string()));
    assert!(!env_names.contains(&"NCCL_DEBUG".to_string()));
}

#[test]
fn render_image_default_distributed_service_uses_task_prolog_for_rank_helpers() {
    let mut service = runtime_service();
    service.name = "default-entrypoint".into();
    service.execution = ExecutionSpec::ImageDefault;
    service.working_dir = None;
    service.placement = ServicePlacement {
        mode: ServicePlacementMode::Distributed,
        nodes: 2,
        ntasks: None,
        ntasks_per_node: Some(2),
        pin_to_primary_node: false,
        node_indices: None,
        exclude_indices: Vec::new(),
        allow_overlap: false,
    };
    let plan = RuntimePlan {
        name: "dist-default".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            nodes: Some(2),
            ..SlurmConfig::default()
        },
        ordered_services: vec![service],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("default_x2d_entrypoint.dist-rank-prolog.sh"));
    assert!(script.contains("srun_cmd+=(\"--task-prolog=$dist_rank_task_prolog\")"));
    assert!(script.contains("export HPC_COMPOSE_DIST_NODE_RANK="));
    assert!(script.contains("export HPC_COMPOSE_DIST_LOCAL_RANK="));
    assert!(script.contains("export HPC_COMPOSE_DIST_GLOBAL_RANK="));
    assert!(script.contains("'--container-entrypoint'"));
}

#[test]
fn render_multi_node_script_emits_allocation_metadata_and_geometry() {
    let mut helper = runtime_service();
    helper.name = "bootstrap".into();
    helper.readiness = Some(ReadinessSpec::Log {
        pattern: "ready".into(),
        timeout_seconds: Some(5),
    });
    helper.placement = ServicePlacement {
        mode: ServicePlacementMode::PrimaryNode,
        nodes: 1,
        ntasks: Some(1),
        ntasks_per_node: None,
        pin_to_primary_node: true,
        node_indices: None,
        exclude_indices: Vec::new(),
        allow_overlap: true,
    };

    let mut distributed = runtime_service();
    distributed.name = "trainer".into();
    distributed.readiness = Some(ReadinessSpec::Sleep { seconds: 1 });
    distributed.placement = ServicePlacement {
        mode: ServicePlacementMode::Distributed,
        nodes: 2,
        ntasks: None,
        ntasks_per_node: Some(4),
        pin_to_primary_node: false,
        node_indices: None,
        exclude_indices: Vec::new(),
        allow_overlap: false,
    };

    let plan = RuntimePlan {
        name: "multi-node".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            nodes: Some(2),
            ntasks_per_node: Some(4),
            ..SlurmConfig::default()
        },
        ordered_services: vec![helper.clone(), distributed.clone()],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("#SBATCH --nodes=2"));
    assert!(script.contains("#SBATCH --ntasks-per-node=4"));
    assert!(script.contains("PRIMARY_NODE_FILE=\"$ALLOCATION_DIR/primary_node\""));
    assert!(script.contains("NODELIST_FILE=\"$ALLOCATION_DIR/nodes.txt\""));
    assert!(script.contains("HPC_COMPOSE_NODELIST_FILE=\"/hpc-compose/job/allocation/nodes.txt\""));
    assert!(script.contains("scontrol show hostnames \"$SLURM_JOB_NODELIST\""));
    assert!(
        display_srun_command(&helper)
            .iter()
            .any(|arg| arg == "--nodelist=$HPC_COMPOSE_PRIMARY_NODE")
    );
    assert!(
        !display_srun_command(&distributed)
            .iter()
            .any(|arg| arg.starts_with("--nodelist="))
    );
}

#[test]
fn rendered_single_node_script_derives_allocation_metadata_without_scontrol() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_fake_runtime_srun(tmpdir.path());
    fs::remove_file(tmpdir.path().join("scontrol")).expect("remove scontrol");

    let plan = RuntimePlan {
        name: "single-node".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![RuntimeService {
            execution: ExecutionSpec::Shell("echo single-node".into()),
            environment: Vec::new(),
            working_dir: None,
            readiness: None,
            ..runtime_service()
        }],
    };
    let script = render_script(&plan).expect("script");
    let script_path = tmpdir.path().join("single-node.sbatch");
    write_executable(&script_path, &script);

    run_rendered_script(tmpdir.path(), &script_path, 0);

    let log =
        fs::read_to_string(tmpdir.path().join(".hpc-compose/12345/logs/worker.log")).expect("log");
    assert!(log.contains("node_meta=node01|1|node01|/hpc-compose/job/allocation/nodes.txt"));
}

#[test]
fn render_uses_distinct_internal_ids_for_punctuation_variants() {
    let mk_service = |name: &str| RuntimeService {
        name: name.into(),
        runtime_image: PathBuf::from(format!("/shared/cache/{name}.sqsh")),
        execution: ExecutionSpec::Shell("echo hi".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: Some(ReadinessSpec::Log {
            pattern: "ready".into(),
            timeout_seconds: Some(5),
        }),
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: crate::planner::ImageSource::Remote("docker://redis:7".into()),
    };
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![mk_service("api.v1"), mk_service("api_v1")],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("launch_api_x2e_v1()"));
    assert!(script.contains("launch_api_x5f_v1()"));
    assert!(script.contains("$LOG_DIR/api_x2e_v1.log"));
    assert!(script.contains("$LOG_DIR/api_x5f_v1.log"));
    assert!(script.contains("'--job-name=hpc-compose:api_x2e_v1'"));
    assert!(script.contains("'--job-name=hpc-compose:api_x5f_v1'"));
}

#[test]
fn top_level_gpu_branch_and_unreachable_working_dir_guard_are_covered() {
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            gpus: Some(4),
            ..SlurmConfig::default()
        },
        ordered_services: vec![RuntimeService {
            name: "svc".into(),
            runtime_image: PathBuf::from("/shared/cache/svc.sqsh"),
            execution: ExecutionSpec::Shell("echo hi".into()),
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: None,
            source: crate::planner::ImageSource::Remote("docker://redis:7".into()),
        }],
    };
    let script = render_script(&plan).expect("script");
    assert!(script.contains("#SBATCH --gpus=4"));

    let panic =
        std::panic::catch_unwind(|| execution_argv(&ExecutionSpec::ImageDefault, Some("/work")));
    assert!(panic.is_err());
}

#[test]
fn render_waits_only_on_declared_dependencies() {
    let provider = RuntimeService {
        name: "a".into(),
        runtime_image: PathBuf::from("/shared/cache/a.sqsh"),
        execution: ExecutionSpec::Shell("echo a".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: Some(ReadinessSpec::Log {
            pattern: "ready".into(),
            timeout_seconds: Some(30),
        }),
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: crate::planner::ImageSource::Remote("docker://redis:7".into()),
    };
    let unrelated = RuntimeService {
        name: "b".into(),
        runtime_image: PathBuf::from("/shared/cache/b.sqsh"),
        execution: ExecutionSpec::Shell("echo b".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: None,
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: crate::planner::ImageSource::Remote("docker://redis:7".into()),
    };
    let dependent = RuntimeService {
        name: "c".into(),
        runtime_image: PathBuf::from("/shared/cache/c.sqsh"),
        execution: ExecutionSpec::Shell("echo c".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: vec![ServiceDependency {
            name: "a".into(),
            condition: DependencyCondition::ServiceHealthy,
            implicit: false,
        }],
        readiness: None,
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: crate::planner::ImageSource::Remote("docker://redis:7".into()),
    };
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![provider, unrelated, dependent],
    };

    let script = render_script(&plan).expect("script");
    let launch_a = script.find("launch_a\n").expect("launch a");
    let launch_b = script.find("launch_b\n").expect("launch b");
    let wait_a = script
        .find("wait_for_service_healthy 'a' 'c' wait_until_a_ready")
        .expect("healthy wait");
    let launch_c = script.find("launch_c\n").expect("launch c");
    assert!(launch_a < launch_b);
    assert!(launch_b < wait_a);
    assert!(wait_a < launch_c);
    let started_check = script
        .find("wait_for_service_started \"$dependency\" \"$target\" || return 1")
        .expect("started check");
    let healthy_cache = script
        .find("if [[ \"${SERVICE_HEALTHY[index]:-0}\" == \"1\" ]]")
        .expect("healthy cache");
    assert!(started_check < healthy_cache);
    assert!(script.contains("\"healthy\":"));
}

#[test]
fn render_supports_completed_dependency_and_binding_flags() {
    let preprocess = RuntimeService {
        name: "preprocess".into(),
        runtime_image: PathBuf::from("/shared/cache/preprocess.sqsh"),
        execution: ExecutionSpec::Shell("echo preprocess".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: None,
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: crate::planner::ImageSource::Remote("docker://alpine:3.20".into()),
    };
    let trainer = RuntimeService {
        name: "trainer".into(),
        runtime_image: PathBuf::from("/shared/cache/trainer.sqsh"),
        execution: ExecutionSpec::Shell("echo trainer".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: vec![ServiceDependency {
            name: "preprocess".into(),
            condition: DependencyCondition::ServiceCompletedSuccessfully,
            implicit: false,
        }],
        readiness: None,
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig {
            gpus_per_task: Some(1),
            cpus_per_gpu: Some(8),
            gpu_bind: Some("closest".into()),
            cpu_bind: Some("cores".into()),
            mpi: Some(MpiConfig {
                mpi_type: MpiType::new("pmix_v4").expect("mpi type"),
                profile: None,
                implementation: None,
                launcher: Default::default(),
                expected_ranks: None,
                host_mpi: None,
            }),
            ..ServiceSlurmConfig::default()
        },
        prepare: None,
        source: crate::planner::ImageSource::Remote("docker://ubuntu:24.04".into()),
    };
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            gpus_per_node: Some(4),
            mem_per_gpu: Some("40G".into()),
            distribution: Some("block:block".into()),
            hint: Some("nomultithread".into()),
            ..SlurmConfig::default()
        },
        ordered_services: vec![preprocess, trainer],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("#SBATCH --gpus-per-node=4"));
    assert!(script.contains("#SBATCH --mem-per-gpu=40G"));
    assert!(script.contains("#SBATCH --distribution=block:block"));
    assert!(script.contains("#SBATCH --hint=nomultithread"));
    assert!(script.contains("wait_for_service_completed_successfully 'preprocess' 'trainer'"));
    assert!(script.contains("\"completed_successfully\":"));
    let srun = display_srun_command(&plan.ordered_services[1]).join(" ");
    assert!(srun.contains("--gpus-per-task=1"));
    assert!(srun.contains("--cpus-per-gpu=8"));
    assert!(srun.contains("--gpu-bind=closest"));
    assert!(srun.contains("--cpu-bind=cores"));
    assert!(srun.contains("--mpi=pmix_v4"));
}

#[test]
fn render_includes_failure_policy_arrays_and_restart_handlers() {
    let restart_service = RuntimeService {
        name: "api".into(),
        runtime_image: PathBuf::from("/shared/cache/api.sqsh"),
        execution: ExecutionSpec::Shell("echo api".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: None,
        assertions: None,
        failure_policy: crate::spec::ServiceFailurePolicy {
            mode: ServiceFailureMode::RestartOnFailure,
            max_restarts: 3,
            backoff_seconds: 5,
            window_seconds: 60,
            max_restarts_in_window: 3,
        },
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: crate::planner::ImageSource::Remote("docker://redis:7".into()),
    };
    let ignore_service = RuntimeService {
        name: "worker".into(),
        runtime_image: PathBuf::from("/shared/cache/worker.sqsh"),
        execution: ExecutionSpec::Shell("echo worker".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: None,
        assertions: None,
        failure_policy: crate::spec::ServiceFailurePolicy {
            mode: ServiceFailureMode::Ignore,
            max_restarts: 0,
            backoff_seconds: 0,
            window_seconds: 0,
            max_restarts_in_window: 0,
        },
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: crate::planner::ImageSource::Remote("docker://python:3.11-slim".into()),
    };
    let dependent = RuntimeService {
        name: "client".into(),
        runtime_image: PathBuf::from("/shared/cache/client.sqsh"),
        execution: ExecutionSpec::Shell("echo client".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: vec![ServiceDependency {
            name: "api".into(),
            condition: DependencyCondition::ServiceStarted,
            implicit: false,
        }],
        readiness: None,
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: crate::planner::ImageSource::Remote("docker://alpine:3".into()),
    };
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![restart_service, ignore_service, dependent],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("SERVICE_FAILURE_POLICY_MODE=()"));
    assert!(script.contains("SERVICE_MAX_RESTARTS=()"));
    assert!(script.contains("SERVICE_BACKOFF_SECONDS=()"));
    assert!(script.contains("SERVICE_WINDOW_SECONDS=()"));
    assert!(script.contains("SERVICE_MAX_RESTARTS_IN_WINDOW=()"));
    assert!(script.contains("SERVICE_RESTART_COUNT=()"));
    assert!(script.contains("SERVICE_RESTART_FAILURES_IN_WINDOW=()"));
    assert!(script.contains("SERVICE_RESTART_FAILURE_TIMESTAMPS=()"));
    assert!(script.contains("SERVICE_LAST_EXIT_CODE=()"));
    assert!(script.contains("json_number_array()"));
    assert!(script.contains("\"failure_policy_mode\""));
    assert!(script.contains("\"restart_count\""));
    assert!(script.contains("\"max_restarts\""));
    assert!(script.contains("\"window_seconds\""));
    assert!(script.contains("\"max_restarts_in_window\""));
    assert!(script.contains("\"restart_failures_in_window\""));
    assert!(script.contains("\"restart_failure_timestamps\""));
    assert!(script.contains("\"last_exit_code\""));
    assert!(script.contains("prune_restart_window()"));
    assert!(script.contains("handle_service_exit()"));
    assert!(script.contains("mode=${SERVICE_FAILURE_POLICY_MODE[index]:-fail_job}"));
    assert!(script.contains("if [[ \"$mode\" == \"ignore\" ]]"));
    assert!(script.contains("if [[ \"$mode\" == \"restart_on_failure\" ]]"));
    assert!(script.contains("local window_seconds=${SERVICE_WINDOW_SECONDS[index]:-0}"));
    assert!(
        script.contains("local max_restarts_in_window=${SERVICE_MAX_RESTARTS_IN_WINDOW[index]:-0}")
    );
    assert!(script.contains(
        "local restart_failures_in_window=${SERVICE_RESTART_FAILURES_IN_WINDOW[index]:-0}"
    ));
    assert!(script.contains("SERVICE_RESTART_COUNT[index]=\"$next_restart\""));
    assert!(script.contains("local launch_fn=${SERVICE_LAUNCH_FNS[index]:-}"));
    assert!(script.contains("local index=${SERVICE_INDEX_BY_NAME[\"$name\"]:-}"));
    assert!(script.contains("emit_dependency_failure_diagnostic()"));
    assert!(script.contains("Service '$failed_service' is required by:"));
    assert!(script.contains("'restart_on_failure' 3 5 60 3"));
    assert!(script.contains("'ignore' 0 0 0 0"));
}

#[test]
fn render_metrics_sampler_when_enabled() {
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            metrics: Some(crate::spec::MetricsConfig {
                enabled: Some(true),
                interval_seconds: Some(3),
                collectors: vec![
                    MetricsCollector::Gpu,
                    MetricsCollector::Slurm,
                    MetricsCollector::Cpu,
                ],
            }),
            ..SlurmConfig::default()
        },
        ordered_services: vec![runtime_service()],
    };

    let script = render_script(&plan).expect("script");
    assert_bash_syntax(&script);
    assert!(script.contains("METRICS_DIR=\"$JOB_TMP/metrics\""));
    assert!(script.contains("start_metrics_sampler"));
    assert!(script.contains("metrics_sampler_loop"));
    assert!(script.contains("nvidia-smi --query-gpu="));
    assert!(script.contains("sstat --allsteps --jobs \"$SLURM_JOB_ID\""));
    // AllocTRES is a sacct (allocation) field that sstat rejects; the collector
    // must request usage fields only and parse the resulting 6 columns.
    assert!(!script.contains("AllocTRES"));
    assert!(script.contains("--format=JobID,NTasks,AveCPU,AveRSS,MaxRSS,TRESUsageInAve"));
    assert!(script.contains("(( ${#fields[@]} != 6 ))"));
    assert!(script.contains("stop_metrics_sampler"));
    // E1: stop must flush one final synchronous sample, bounded so a hung
    // nvidia-smi/sstat cannot delay job teardown, before killing the loop.
    assert!(script.contains("final_metrics_sample"));
    assert!(script.contains("  final_metrics_sample\n"));
    assert!(script.contains("local budget_seconds=10"));
    assert!(script.contains("sample_metrics_once &"));
    assert!(script.contains("GPU_COLLECTOR_ENABLED=1"));
    assert!(script.contains("SLURM_COLLECTOR_ENABLED=1"));
    assert!(script.contains("sample_gpu_metrics_all_nodes"));
    assert!(script.contains("--ntasks-per-node=1 --exact --overlap bash \"$script_path\""));
    // E6: multi-node GPU fanout must degrade to batch-node sampling on srun
    // failure rather than marking the whole collector unavailable.
    assert!(
        script.contains("multi-node GPU fanout failed through srun; sampling the batch node only")
    );
    assert!(script.contains("multi-node GPU fanout degraded to batch-node sampling"));
    assert!(script.contains("local fallback_status=$?"));
    assert!(script.contains("METRICS_DIAGNOSTICS_DIR=\"$METRICS_DIR/diagnostics\""));
    assert!(script.contains("nvidia-smi topo -m"));
    // Sampled CPU collector: enabled flag, /proc/stat source, cpu.jsonl output,
    // wired into sample_metrics_once, plus its own multi-node fanout + fallback.
    assert!(script.contains("CPU_COLLECTOR_ENABLED=1"));
    assert!(script.contains("CPU_METRICS_FILE=\"$METRICS_DIR/cpu.jsonl\""));
    assert!(script.contains("emit_cpu_sample_row"));
    assert!(script.contains("${HPC_COMPOSE_PROC_STAT_PATH:-/proc/stat}"));
    assert!(script.contains("  sample_cpu_metrics\n"));
    assert!(script.contains("sample_cpu_metrics_all_nodes"));
    assert!(script.contains("write_cpu_sample_node_script"));
    assert!(
        script.contains("multi-node CPU fanout failed through srun; sampling the batch node only")
    );
    assert!(script.contains("multi-node CPU fanout degraded to batch-node sampling"));
    assert!(script.contains(": > \"$CPU_METRICS_FILE\""));
    // Per-service GPU attribution: process rows must carry the raw cgroup and
    // SLURM_PROCID/SLURM_LOCALID captures (parsed later by `stats`), and the
    // sampler must record the live step-id -> step-name map in steps.jsonl.
    assert!(script.contains("STEP_MAP_FILE=\"$METRICS_DIR/steps.jsonl\""));
    assert!(script.contains(": > \"$STEP_MAP_FILE\""));
    assert!(script.contains("capture_step_map"));
    assert!(script.contains("squeue --noheader --steps --jobs \"$SLURM_JOB_ID\""));
    assert!(script.contains("gpu_process_cgroup"));
    assert!(script.contains("gpu_process_environ_value"));
    assert!(script.contains("\"cgroup\":%s,\"slurm_procid\":%s,\"slurm_localid\":%s"));
    // The attribution probes appear in both the in-process sampler and the
    // self-contained multi-node fanout script.
    assert!(script.matches("gpu_process_cgroup() {").count() >= 2);
    // Attribution capture failures use the warn-once diagnostics channel and
    // must never fail the sampler tick or the job.
    assert!(script.contains("STEPS_WARNING_EMITTED"));
    assert!(script.contains("per-service GPU attribution will stay null"));
}

#[test]
fn render_structured_software_env_before_setup_and_service_launch() {
    let mut service = runtime_service();
    service.slurm.software_env = crate::spec::SoftwareEnvConfig {
        modules: crate::spec::ModuleEnvSpec {
            purge: false,
            load: vec!["netcdf/4.9".into()],
        },
        spack: None,
        env: BTreeMap::from([("OMP_NUM_THREADS".into(), "8".into())]),
    };
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            setup: vec!["echo setup".into()],
            software_env: crate::spec::SoftwareEnvConfig {
                modules: crate::spec::ModuleEnvSpec {
                    purge: true,
                    load: vec!["cuda/12.4".into()],
                },
                spack: Some(crate::spec::SpackEnvSpec {
                    view: "/shared/spack/views/ml".into(),
                }),
                env: BTreeMap::from([
                    ("HDF5_USE_FILE_LOCKING".into(), "FALSE".into()),
                    ("OMP_NUM_THREADS".into(), "2".into()),
                ]),
            },
            ..SlurmConfig::default()
        },
        ordered_services: vec![service],
    };

    let script = render_script(&plan).expect("script");
    let top_level = script
        .find("hpc_compose_module purge")
        .expect("top-level x-env");
    let setup = script.find("echo setup").expect("setup");
    assert!(top_level < setup);
    assert!(script.contains("hpc_compose_module load 'cuda/12.4'"));
    assert!(script.contains("hpc_compose_module load 'netcdf/4.9'"));
    assert!(script.contains("if [[ -d '/shared/spack/views/ml'/bin ]]; then export PATH='/shared/spack/views/ml'/bin:\"$PATH\"; fi"));
    assert!(
        script
            .contains("local -a software_env=('HDF5_USE_FILE_LOCKING=FALSE' 'OMP_NUM_THREADS=8')")
    );
    assert!(script.contains("HDF5_USE_FILE_LOCKING"));
    assert!(script.contains("OMP_NUM_THREADS"));
}

#[test]
fn render_artifact_collection_helpers_when_enabled() {
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            artifacts: Some(crate::spec::ArtifactsConfig {
                collect: ArtifactCollectPolicy::OnFailure,
                export_dir: Some("./results/${SLURM_JOB_ID}".into()),
                paths: vec![
                    "/hpc-compose/job/metrics/**".into(),
                    "/hpc-compose/job/checkpoints/*.pt".into(),
                ],
                bundles: BTreeMap::new(),
            }),
            ..SlurmConfig::default()
        },
        ordered_services: vec![runtime_service()],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("ARTIFACTS_DIR=\"$JOB_TMP/artifacts\""));
    assert!(script.contains("ARTIFACTS_MANIFEST_FILE=\"$ARTIFACTS_DIR/manifest.json\""));
    assert!(script.contains("ARTIFACTS_COLLECT_POLICY='on_failure'"));
    assert!(script.contains("ARTIFACT_BUNDLE_NAMES=('default')"));
    assert!(script.contains("ARTIFACT_PATTERN_BUNDLES=('default' 'default')"));
    assert!(script.contains("ARTIFACT_SOURCE_PATTERNS=('/hpc-compose/job/metrics/**' '/hpc-compose/job/checkpoints/*.pt')"));
    assert!(script.contains("collect_artifacts \"$code\" || true"));
    assert!(script.contains("host_pattern=\"$JOB_TMP${declared_pattern#/hpc-compose/job}\""));
    assert!(script.contains("container_match=\"/hpc-compose/job${matched#\"$JOB_TMP\"}\""));
    assert!(script.contains("write_artifact_bundles_json"));
}

#[test]
fn render_service_assertions_in_cleanup_before_artifacts() {
    let mut service = runtime_service();
    service.assertions = Some(ServiceAssertSpec {
        exit_code: Some(0),
        artifacts_contain: Some("model/*.pt".into()),
        max_duration_seconds: Some(7200),
    });
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            artifacts: Some(crate::spec::ArtifactsConfig {
                collect: ArtifactCollectPolicy::Always,
                export_dir: Some("./results/${SLURM_JOB_ID}".into()),
                paths: vec!["/hpc-compose/job/model/*.pt".into()],
                bundles: BTreeMap::new(),
            }),
            ..SlurmConfig::default()
        },
        ordered_services: vec![service],
    };

    let script = render_script(&plan).expect("script");
    assert!(script.contains("SERVICE_ASSERT_EXIT_CODES=()"));
    assert!(script.contains("evaluate_assertions || assertion_status=$?"));
    assert!(
        script.contains(
            "SERVICE_ASSERT_ARTIFACT_PATTERNS[assert_index]='/hpc-compose/job/model/*.pt'"
        )
    );
    assert!(script.contains("SERVICE_ASSERT_MAX_DURATIONS[assert_index]='7200'"));
    let assertions_pos = script
        .find("evaluate_assertions || assertion_status=$?")
        .expect("assertions");
    let artifacts_pos = script
        .find("collect_artifacts \"$code\" || true")
        .expect("artifacts");
    assert!(assertions_pos < artifacts_pos);
}

#[test]
fn render_resume_helpers_when_enabled() {
    let mut plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![runtime_service()],
    };
    plan.slurm.resume = Some(ResumeConfig {
        path: "/shared/runs/demo".into(),
    });

    let script = render_script(&plan).expect("script");
    assert!(script.contains("RESUME_ENABLED=1"));
    assert!(script.contains("JOB_TMP=\"$JOB_ROOT/attempts/$ATTEMPT\""));
    assert!(script.contains("append_unique_mount \"$RESUME_HOST_PATH:$RESUME_CONTAINER_PATH\""));
    assert!(script.contains("launch_env+=(\"HPC_COMPOSE_RESUME_DIR=$RESUME_CONTAINER_PATH\")"));
    assert!(script.contains("launch_env+=(\"HPC_COMPOSE_ATTEMPT=$ATTEMPT\")"));
    assert!(script.contains("launch_env+=(\"HPC_COMPOSE_IS_RESUME=$IS_RESUME\")"));
    assert!(script.contains("update_latest_runtime_links"));
    assert!(script.contains("write_resume_metadata"));
}

#[test]
fn render_resume_dir_is_backend_aware() {
    // Host backend: HPC_COMPOSE_RESUME_DIR must be the real on-node
    // $RESUME_HOST_PATH, not the container mount point /hpc-compose/resume which
    // is never mounted under `host` (regression test — a resuming host service
    // would otherwise read an unmounted path).
    let mut host_service = runtime_service();
    host_service.source = crate::planner::ImageSource::Host;
    host_service.runtime_image = PathBuf::new();
    let host_plan = RuntimePlan {
        name: "resume-host".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Host,
            ..RuntimeConfig::default()
        },
        slurm: SlurmConfig {
            resume: Some(ResumeConfig {
                path: "/shared/runs/demo".into(),
            }),
            ..SlurmConfig::default()
        },
        ordered_services: vec![host_service],
    };
    let host_script = render_script(&host_plan).expect("host script");
    assert!(host_script.contains("launch_env+=(\"HPC_COMPOSE_RESUME_DIR=$RESUME_HOST_PATH\")"));
    assert!(
        !host_script.contains("launch_env+=(\"HPC_COMPOSE_RESUME_DIR=$RESUME_CONTAINER_PATH\")")
    );

    // Container backends bind-mount $RESUME_HOST_PATH at $RESUME_CONTAINER_PATH.
    let container_plan = RuntimePlan {
        name: "resume-container".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Pyxis,
            ..RuntimeConfig::default()
        },
        slurm: SlurmConfig {
            resume: Some(ResumeConfig {
                path: "/shared/runs/demo".into(),
            }),
            ..SlurmConfig::default()
        },
        ordered_services: vec![runtime_service()],
    };
    let container_script = render_script(&container_plan).expect("container script");
    assert!(
        container_script
            .contains("launch_env+=(\"HPC_COMPOSE_RESUME_DIR=$RESUME_CONTAINER_PATH\")")
    );
    assert!(
        !container_script.contains("launch_env+=(\"HPC_COMPOSE_RESUME_DIR=$RESUME_HOST_PATH\")")
    );
}

#[test]
fn rendered_resume_script_preserves_prior_attempts_and_updates_latest_links() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_fake_runtime_srun(tmpdir.path());
    let resume_dir = tmpdir.path().join("resume");
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            artifacts: Some(crate::spec::ArtifactsConfig {
                collect: ArtifactCollectPolicy::Always,
                export_dir: Some("./results/${SLURM_JOB_ID}".into()),
                paths: vec!["/hpc-compose/job/checkpoints/**".into()],
                bundles: BTreeMap::new(),
            }),
            resume: Some(ResumeConfig {
                path: resume_dir.display().to_string(),
            }),
            ..SlurmConfig::default()
        },
        ordered_services: vec![RuntimeService {
            execution: ExecutionSpec::Shell("echo resume".into()),
            environment: Vec::new(),
            working_dir: None,
            readiness: None,
            ..runtime_service()
        }],
    };
    let script = render_script(&plan).expect("script");
    let script_path = tmpdir.path().join("resume.sbatch");
    write_executable(&script_path, &script);

    run_rendered_script(tmpdir.path(), &script_path, 0);
    run_rendered_script(tmpdir.path(), &script_path, 1);

    let job_root = tmpdir.path().join(".hpc-compose/12345");
    let attempt0_log = job_root.join("attempts/0/logs/worker.log");
    let attempt1_log = job_root.join("attempts/1/logs/worker.log");
    assert!(attempt0_log.exists());
    assert!(attempt1_log.exists());
    assert!(
        fs::read_to_string(&attempt0_log)
            .expect("attempt0 log")
            .contains("resume_dir=/hpc-compose/resume attempt=0 is_resume=0")
    );
    assert!(
        fs::read_to_string(&attempt1_log)
            .expect("attempt1 log")
            .contains("resume_dir=/hpc-compose/resume attempt=1 is_resume=1")
    );

    assert_eq!(
        fs::read_link(job_root.join("logs")).expect("logs symlink"),
        job_root.join("attempts/1/logs")
    );
    assert_eq!(
        fs::read_link(job_root.join("artifacts")).expect("artifacts symlink"),
        job_root.join("attempts/1/artifacts")
    );
    assert_eq!(
        fs::read_link(job_root.join("state.json")).expect("state symlink"),
        job_root.join("attempts/1/state.json")
    );

    assert!(
        job_root
            .join("attempts/0/artifacts/payload/checkpoints/checkpoint-0.txt")
            .exists()
    );
    assert!(
        job_root
            .join("attempts/1/artifacts/payload/checkpoints/checkpoint-1.txt")
            .exists()
    );

    let state: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(job_root.join("state.json")).expect("state"))
            .expect("state json");
    assert_eq!(state["attempt"], 1);
    assert_eq!(state["is_resume"], true);
    assert_eq!(state["resume_dir"], resume_dir.display().to_string());

    let manifest: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(job_root.join("artifacts/manifest.json")).expect("manifest"),
    )
    .expect("manifest json");
    assert_eq!(manifest["schema_version"], 3);
    assert_eq!(manifest["attempt"], 1);
    assert_eq!(manifest["is_resume"], true);
    assert_eq!(manifest["resume_dir"], resume_dir.display().to_string());
}

#[test]
fn rendered_resume_script_detects_existing_resume_metadata() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_fake_runtime_srun(tmpdir.path());
    let resume_dir = tmpdir.path().join("resume");
    fs::create_dir_all(resume_dir.join("_hpc-compose")).expect("resume meta dir");
    fs::write(
            resume_dir.join("_hpc-compose/latest.json"),
            r#"{"schema_version":1,"compose_name":"demo","job_id":"old","attempt":7,"updated_at":"2026-04-05T10:00:00Z"}"#,
        )
        .expect("seed metadata");
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            resume: Some(ResumeConfig {
                path: resume_dir.display().to_string(),
            }),
            ..SlurmConfig::default()
        },
        ordered_services: vec![RuntimeService {
            execution: ExecutionSpec::Shell("echo resume".into()),
            environment: Vec::new(),
            working_dir: None,
            readiness: None,
            ..runtime_service()
        }],
    };
    let script = render_script(&plan).expect("script");
    let script_path = tmpdir.path().join("resume-detect.sbatch");
    write_executable(&script_path, &script);

    run_rendered_script(tmpdir.path(), &script_path, 0);
    let log = fs::read_to_string(
        tmpdir
            .path()
            .join(".hpc-compose/12345/attempts/0/logs/worker.log"),
    )
    .expect("log");
    assert!(log.contains("is_resume=1"));
}

#[test]
fn rendered_script_restarts_failed_dependency_before_healthy_dependents_launch() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_fake_runtime_srun_with_dependency_restart(tmpdir.path());
    let provider = RuntimeService {
        name: "api".into(),
        runtime_image: PathBuf::from("/shared/cache/api.sqsh"),
        execution: ExecutionSpec::Shell("echo api".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: Some(ReadinessSpec::Sleep { seconds: 1 }),
        assertions: None,
        failure_policy: ServiceFailurePolicy {
            mode: ServiceFailureMode::RestartOnFailure,
            max_restarts: 1,
            backoff_seconds: 1,
            window_seconds: 60,
            max_restarts_in_window: 1,
        },
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: crate::planner::ImageSource::Remote("docker://redis:7".into()),
    };
    let client = RuntimeService {
        name: "client".into(),
        runtime_image: PathBuf::from("/shared/cache/client.sqsh"),
        execution: ExecutionSpec::Shell("echo client".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: vec![ServiceDependency {
            name: "api".into(),
            condition: DependencyCondition::ServiceHealthy,
            implicit: false,
        }],
        readiness: None,
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: crate::planner::ImageSource::Remote("docker://alpine:3".into()),
    };
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![provider, client],
    };
    let script = render_script(&plan).expect("script");
    let script_path = tmpdir.path().join("restart-dependency.sbatch");
    write_executable(&script_path, &script);

    run_rendered_script(tmpdir.path(), &script_path, 0);

    assert_eq!(
        fs::read_to_string(
            tmpdir
                .path()
                .join(".hpc-compose/fake-runtime-srun/hpc-compose_api.count"),
        )
        .expect("api count")
        .trim(),
        "2"
    );
    assert_eq!(
        fs::read_to_string(
            tmpdir
                .path()
                .join(".hpc-compose/fake-runtime-srun/hpc-compose_client.count"),
        )
        .expect("client count")
        .trim(),
        "1"
    );

    let state: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(tmpdir.path().join(".hpc-compose/12345/state.json")).expect("state"),
    )
    .expect("state json");
    let api = state["services"]
        .as_array()
        .expect("services")
        .iter()
        .find(|service| service["service_name"] == "api")
        .expect("api");
    assert_eq!(api["restart_count"], 1);
    assert_eq!(api["window_seconds"], 60);
    assert_eq!(api["max_restarts_in_window"], 1);
    assert_eq!(api["restart_failures_in_window"], 1);
    assert_eq!(
        api["restart_failure_timestamps"]
            .as_array()
            .expect("timestamps")
            .len(),
        1
    );
    assert!(
        api["restart_failure_timestamps"]
            .as_array()
            .expect("timestamps")
            .iter()
            .all(|value| value.as_u64().is_some())
    );
}

#[test]
fn rendered_script_runs_host_hooks_on_each_restart_attempt() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_fake_runtime_srun_with_dependency_restart(tmpdir.path());
    let service = RuntimeService {
            name: "api".into(),
            runtime_image: PathBuf::from("/shared/cache/api.sqsh"),
            execution: ExecutionSpec::Shell("echo api".into()),
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy {
                mode: ServiceFailureMode::RestartOnFailure,
                max_restarts: 1,
                backoff_seconds: 1,
                window_seconds: 60,
                max_restarts_in_window: 1,
            },
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig {
                prologue: Some(ServiceHookSpec {
                    context: ServiceHookContext::Host,
                    script: "printf 'prologue:%s\\n' \"$HPC_COMPOSE_SERVICE_NAME\" >> \"$SLURM_SUBMIT_DIR/hooks.log\"".into(),
                }),
                epilogue: Some(ServiceHookSpec {
                    context: ServiceHookContext::Host,
                    script: "printf 'epilogue:%s\\n' \"$HPC_COMPOSE_SERVICE_EXIT_CODE\" >> \"$SLURM_SUBMIT_DIR/hooks.log\"".into(),
                }),
                ..ServiceSlurmConfig::default()
            },
            prepare: None,
            source: crate::planner::ImageSource::Remote("docker://redis:7".into()),
        };
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let script = render_script(&plan).expect("script");
    let script_path = tmpdir.path().join("hooks-restart.sbatch");
    write_executable(&script_path, &script);

    run_rendered_script(tmpdir.path(), &script_path, 0);

    let hooks = fs::read_to_string(tmpdir.path().join("hooks.log")).expect("hooks log");
    assert_eq!(
        hooks.lines().collect::<Vec<_>>(),
        vec!["prologue:api", "epilogue:41", "prologue:api", "epilogue:0"]
    );
}

#[test]
fn rendered_script_runs_restart_event_hooks_best_effort() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_fake_runtime_srun_with_dependency_restart(tmpdir.path());
    let service = RuntimeService {
            name: "api".into(),
            runtime_image: PathBuf::from("/shared/cache/api.sqsh"),
            execution: ExecutionSpec::Shell("echo api".into()),
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy {
                mode: ServiceFailureMode::RestartOnFailure,
                max_restarts: 1,
                backoff_seconds: 0,
                window_seconds: 60,
                max_restarts_in_window: 1,
            },
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig {
                hooks: vec![ServiceEventHookSpec {
                    on: ServiceHookEvent::Restart,
                    context: ServiceHookContext::Host,
                    script: "printf 'restart:%s:%s:%s:%s/%s:%s\\n' \"$HPC_COMPOSE_HOOK_PHASE\" \"$HPC_COMPOSE_SERVICE_NAME\" \"$HPC_COMPOSE_SERVICE_EXIT_CODE\" \"$HPC_COMPOSE_RESTART_COUNT\" \"$HPC_COMPOSE_MAX_RESTARTS\" \"$HPC_COMPOSE_ATTEMPT\" >> \"$SLURM_SUBMIT_DIR/events.log\"\nexit 9".into(),
                }],
                ..ServiceSlurmConfig::default()
            },
            prepare: None,
            source: crate::planner::ImageSource::Remote("docker://redis:7".into()),
        };
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let script = render_script(&plan).expect("script");
    let script_path = tmpdir.path().join("restart-event-hook.sbatch");
    write_executable(&script_path, &script);

    run_rendered_script(tmpdir.path(), &script_path, 0);

    let events = fs::read_to_string(tmpdir.path().join("events.log")).expect("events log");
    assert_eq!(
        events.lines().collect::<Vec<_>>(),
        vec!["restart:restart:api:41:1/1:0"]
    );
    let service_log = fs::read_to_string(tmpdir.path().join(".hpc-compose/12345/logs/api.log"))
        .expect("service log");
    assert!(service_log.contains("Event hook 'restart' for service 'api' exited with status 9"));
}

#[test]
fn rendered_script_runs_window_exhausted_hook_once() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_fake_runtime_srun_exit_sequence(tmpdir.path(), "loopy", &[41, 42]);
    let service = RuntimeService {
            name: "loopy".into(),
            runtime_image: PathBuf::from("/shared/cache/loopy.sqsh"),
            execution: ExecutionSpec::Shell("echo loopy".into()),
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy {
                mode: ServiceFailureMode::RestartOnFailure,
                max_restarts: 5,
                backoff_seconds: 0,
                window_seconds: 60,
                max_restarts_in_window: 1,
            },
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig {
                hooks: vec![ServiceEventHookSpec {
                    on: ServiceHookEvent::WindowExhausted,
                    context: ServiceHookContext::Host,
                    script: "printf 'window:%s:%s:%s:%s/%s\\n' \"$HPC_COMPOSE_HOOK_PHASE\" \"$HPC_COMPOSE_SERVICE_NAME\" \"$HPC_COMPOSE_SERVICE_EXIT_CODE\" \"$HPC_COMPOSE_RESTART_FAILURES_IN_WINDOW\" \"$HPC_COMPOSE_MAX_RESTARTS_IN_WINDOW\" >> \"$SLURM_SUBMIT_DIR/events.log\"".into(),
                }],
                ..ServiceSlurmConfig::default()
            },
            prepare: None,
            source: crate::planner::ImageSource::Remote("docker://redis:7".into()),
        };
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let script = render_script(&plan).expect("script");
    let script_path = tmpdir.path().join("window-event-hook.sbatch");
    write_executable(&script_path, &script);

    let output = run_rendered_script_output(tmpdir.path(), &script_path, 0);
    assert!(
        !output.status.success(),
        "script unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let events = fs::read_to_string(tmpdir.path().join("events.log")).expect("events log");
    assert_eq!(
        events.lines().collect::<Vec<_>>(),
        vec!["window:window_exhausted:loopy:42:1/1"]
    );
}

#[test]
fn render_omits_metrics_sampler_when_disabled() {
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![runtime_service()],
    };

    let script = render_script(&plan).expect("script");
    assert!(!script.contains("METRICS_DIR=\"$JOB_TMP/metrics\""));
    assert!(!script.contains("start_metrics_sampler"));
    assert!(!script.contains("metrics_sampler_loop"));
}

// --- Behavioral coverage of the generated rendezvous bash --------------------
//
// `render_rendezvous_helpers` emits ~100% line-covered bash that, until now, was
// never executed by a test. These tests assemble that bash with faithful stand-ins
// for the two cross-cutting helpers it depends on (`json_escape`/`first_word`, which
// live elsewhere in render.rs) and actually run register/resolve/timeout, asserting
// on the JSON record written and the env it exports.

fn run_rendezvous_bash(tmpdir: &std::path::Path, driver: &str) -> std::process::Output {
    let mut script = String::from("#!/usr/bin/env bash\nset -u\n");
    // Minimal but faithful versions of the helpers defined elsewhere in render.rs;
    // our inputs contain no characters that require JSON escaping.
    script.push_str("json_escape() { printf '%s' \"$1\"; }\n");
    script.push_str("first_word() { local v=${1-}; printf '%s' \"${v%% *}\"; }\n\n");
    render_rendezvous_helpers(&mut script);
    script.push_str(driver);
    let path = tmpdir.join("rdzv.sh");
    fs::write(&path, &script).expect("write rendezvous script");
    Command::new(bash_executable())
        .arg(&path)
        .output()
        .expect("run rendezvous script")
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs()
}

#[test]
fn rendezvous_register_writes_valid_record_and_latest_pointer() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = tmpdir.path().join("cache");
    let driver = format!(
        r#"
CACHE_ROOT="{cache}"
SLURM_JOB_ID="12345"
HPC_COMPOSE_PRIMARY_NODE="node01"
SERVICE_NAMES=(model-server)
SERVICE_RDZV_NAMES=(model)
SERVICE_RDZV_PORTS=(8080)
SERVICE_RDZV_PROTOCOLS=(http)
SERVICE_RDZV_PATHS=(/infer)
SERVICE_RDZV_TTLS=(3600)
SERVICE_RDZV_METADATA_JSON=('{{"role":"server"}}')
SERVICE_STEP_NODELIST=("")
SERVICE_RDZV_REGISTERED=(0)
register_service_rendezvous_by_index 0
"#,
        cache = cache.display()
    );
    let output = run_rendezvous_bash(tmpdir.path(), &driver);
    assert!(
        output.status.success(),
        "register failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("Registered rendezvous 'model'"),
        "missing registration notice"
    );
    let latest = cache.join("rendezvous/model/latest.json");
    let record: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&latest).expect("read latest.json"))
            .expect("latest.json is valid JSON");
    assert_eq!(record["schema_version"], 1);
    assert_eq!(record["name"], "model");
    assert_eq!(record["service"], "model-server");
    assert_eq!(record["host"], "node01");
    assert_eq!(record["port"], 8080);
    assert_eq!(record["url"], "http://node01:8080/infer");
    assert_eq!(record["job_id"], "12345");
    assert_eq!(record["metadata"]["role"], "server");
}

#[test]
fn rendezvous_resolve_exports_namespaced_env_from_live_record() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = tmpdir.path().join("cache");
    let dir = cache.join("rendezvous/model");
    fs::create_dir_all(&dir).expect("rendezvous dir");
    // Seed a live record (registered just now, long TTL).
    let record = format!(
        "{{\n  \"url\": \"http://node01:8080/infer\",\n  \"host\": \"node01\",\n  \"port\": 8080,\n  \"protocol\": \"http\",\n  \"path\": \"/infer\",\n  \"job_id\": \"999\",\n  \"service\": \"model-server\",\n  \"registered_at\": {now},\n  \"ttl_seconds\": 3600\n}}\n",
        now = unix_now()
    );
    fs::write(dir.join("latest.json"), record).expect("seed latest.json");
    let driver = format!(
        r#"
CACHE_ROOT="{cache}"
RDZV_CLIENT_NAMES=(model)
RDZV_CLIENT_TIMEOUT_SECONDS=0
RDZV_CLIENT_REQUIRED=1
resolve_rendezvous_dependencies
printf '%s\n' "${{RDZV_LAUNCH_ENV[@]}}"
"#,
        cache = cache.display()
    );
    let output = run_rendezvous_bash(tmpdir.path(), &driver);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "resolve failed: {stdout}");
    assert!(
        stdout.contains("HPC_COMPOSE_RDZV_URL=http://node01:8080/infer"),
        "{stdout}"
    );
    // Namespaced (token-uppercased) variant.
    assert!(
        stdout.contains("HPC_COMPOSE_RDZV_MODEL_URL=http://node01:8080/infer"),
        "{stdout}"
    );
    assert!(
        stdout.contains("HPC_COMPOSE_RDZV_MODEL_PORT=8080"),
        "{stdout}"
    );
}

#[test]
fn rendezvous_resolve_required_times_out_but_optional_warns() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = tmpdir.path().join("cache"); // intentionally never created
    // Required + immediate timeout -> exit 1 with a timeout error.
    let required = format!(
        r#"
CACHE_ROOT="{cache}"
RDZV_CLIENT_NAMES=(missing)
RDZV_CLIENT_TIMEOUT_SECONDS=0
RDZV_CLIENT_REQUIRED=1
resolve_rendezvous_dependencies
echo "exit=$?"
"#,
        cache = cache.display()
    );
    let out = run_rendezvous_bash(tmpdir.path(), &required);
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("exit=1"),
        "required should fail"
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("Timed out resolving rendezvous 'missing'"),
        "missing timeout error"
    );
    // Optional + immediate timeout -> exit 0 with a warning.
    let optional = format!(
        r#"
CACHE_ROOT="{cache}"
RDZV_CLIENT_NAMES=(missing)
RDZV_CLIENT_TIMEOUT_SECONDS=0
RDZV_CLIENT_REQUIRED=0
resolve_rendezvous_dependencies
echo "exit=$?"
"#,
        cache = cache.display()
    );
    let out = run_rendezvous_bash(tmpdir.path(), &optional);
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("exit=0"),
        "optional should succeed"
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("warning: rendezvous 'missing' not resolved"),
        "missing optional warning"
    );
}

#[test]
fn rendezvous_deregister_is_ownership_guarded() {
    fn seed_and_deregister(owner_job_id: &str, self_job_id: &str) -> bool {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let cache = tmpdir.path().join("cache");
        let dir = cache.join("rendezvous/model");
        fs::create_dir_all(&dir).expect("rendezvous dir");
        fs::write(
            dir.join("latest.json"),
            format!("{{\n  \"job_id\": \"{owner_job_id}\"\n}}\n"),
        )
        .expect("seed latest.json");
        let driver = format!(
            r#"
CACHE_ROOT="{cache}"
SLURM_JOB_ID="{self_job_id}"
SERVICE_NAMES=(model-server)
SERVICE_RDZV_NAMES=(model)
deregister_rendezvous_records
"#,
            cache = cache.display()
        );
        let out = run_rendezvous_bash(tmpdir.path(), &driver);
        assert!(out.status.success(), "deregister should not error");
        dir.join("latest.json").exists()
    }
    // The owner reaps its own record ...
    assert!(
        !seed_and_deregister("12345", "12345"),
        "owner must remove its record"
    );
    // ... but a different job must not delete someone else's record.
    assert!(
        seed_and_deregister("999", "12345"),
        "non-owner must preserve the record"
    );
}

fn run_metrics_bash(tmpdir: &std::path::Path, driver: &str) -> std::process::Output {
    let mut script = String::from("#!/usr/bin/env bash\nset -u\n");
    // Helpers defined elsewhere in render.rs; faithful for our simple inputs.
    script.push_str("json_escape() { printf '%s' \"$1\"; }\n");
    script.push_str(
        "json_number_or_null() { local v=${1-}; if [[ -z \"$v\" ]]; then printf 'null'; else printf '%s' \"$v\"; fi; }\n",
    );
    script.push_str(
        "json_string_or_null() { local v=${1-}; if [[ -z \"$v\" ]]; then printf 'null'; else printf '\"%s\"' \"$(json_escape \"$v\")\"; fi; }\n\n",
    );
    render_metrics_helpers(&mut script);
    script.push_str(driver);
    let path = tmpdir.join("metrics.sh");
    fs::write(&path, &script).expect("write metrics script");
    Command::new(bash_executable())
        .arg(&path)
        .output()
        .expect("run metrics script")
}

#[test]
fn metrics_meta_is_written_as_valid_json_with_collector_states() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let meta = tmpdir.path().join("metrics-meta.json");
    let driver = format!(
        r#"
METRICS_META_FILE="{meta}"
METRICS_INTERVAL_SECONDS=5
SAMPLER_PID=4242
GPU_COLLECTOR_ENABLED=1
GPU_COLLECTOR_AVAILABLE=0
GPU_COLLECTOR_NOTE="nvidia-smi is not available on this node"
GPU_COLLECTOR_LAST_SAMPLED_AT=""
SLURM_COLLECTOR_ENABLED=1
SLURM_COLLECTOR_AVAILABLE=1
SLURM_COLLECTOR_NOTE=""
SLURM_COLLECTOR_LAST_SAMPLED_AT="2024-01-01T00:00:00Z"
CPU_COLLECTOR_ENABLED=1
CPU_COLLECTOR_AVAILABLE=1
CPU_COLLECTOR_NOTE=""
CPU_COLLECTOR_LAST_SAMPLED_AT="2024-01-01T00:00:05Z"
write_metrics_meta
"#,
        meta = meta.display()
    );
    let output = run_metrics_bash(tmpdir.path(), &driver);
    assert!(
        output.status.success(),
        "write_metrics_meta failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&meta).expect("read meta"))
            .expect("metrics meta must be valid JSON");
    assert_eq!(value["sampler_pid"], 4242);
    assert_eq!(value["interval_seconds"], 5);
    let collectors = value["collectors"].as_array().expect("collectors array");
    assert_eq!(collectors.len(), 3);
    assert_eq!(collectors[0]["name"], "gpu");
    assert_eq!(collectors[0]["enabled"], true);
    assert_eq!(collectors[0]["available"], false);
    assert_eq!(
        collectors[0]["note"],
        "nvidia-smi is not available on this node"
    );
    assert_eq!(collectors[0]["last_sampled_at"], serde_json::Value::Null);
    assert_eq!(collectors[1]["name"], "slurm");
    assert_eq!(collectors[1]["available"], true);
    assert_eq!(collectors[1]["note"], serde_json::Value::Null);
    assert_eq!(collectors[2]["name"], "cpu");
    assert_eq!(collectors[2]["enabled"], true);
    assert_eq!(collectors[2]["available"], true);
    assert_eq!(collectors[2]["note"], serde_json::Value::Null);
    assert_eq!(collectors[2]["last_sampled_at"], "2024-01-01T00:00:05Z");
}

#[test]
fn emit_cpu_sample_row_computes_delta_across_two_proc_stat_snapshots() {
    // Drive the generated cpu sampling function against two fixture /proc/stat
    // snapshots (path override), sharing one state file so the second call sees
    // the first call's counters and computes a non-idle/total delta.
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let stat1 = tmpdir.path().join("stat1");
    let stat2 = tmpdir.path().join("stat2");
    let loadavg = tmpdir.path().join("loadavg");
    let state = tmpdir.path().join("cpu.state");
    let out = tmpdir.path().join("cpu.jsonl");
    // Aggregate cpu line: user nice system idle iowait irq softirq steal ...
    // Sample1: idle_all=800, non_idle=200, total=1000.
    fs::write(&stat1, "cpu  100 0 100 700 100 0 0 0 0 0\ncpu0 50 0 50 350 50 0 0 0 0 0\ncpu1 50 0 50 350 50 0 0 0 0 0\nintr 12345\n").expect("stat1");
    // Sample2: idle_all=1400, non_idle=400, total=1800. dt=800, di=600 -> 25.0%.
    fs::write(&stat2, "cpu  200 0 200 1300 100 0 0 0 0 0\ncpu0 100 0 100 650 50 0 0 0 0 0\ncpu1 100 0 100 650 50 0 0 0 0 0\nintr 99999\n").expect("stat2");
    fs::write(&loadavg, "0.50 0.40 0.30 1/234 5678\n").expect("loadavg");
    let driver = format!(
        r#"
emit_cpu_sample_row "2024-01-01T00:00:00Z" "nodeA" "{stat1}" "{loadavg}" "{state}" "{out}"
emit_cpu_sample_row "2024-01-01T00:00:05Z" "nodeA" "{stat2}" "{loadavg}" "{state}" "{out}"
"#,
        stat1 = stat1.display(),
        stat2 = stat2.display(),
        loadavg = loadavg.display(),
        state = state.display(),
        out = out.display(),
    );
    let output = run_metrics_bash(tmpdir.path(), &driver);
    assert!(
        output.status.success(),
        "emit_cpu_sample_row failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows: Vec<serde_json::Value> = fs::read_to_string(&out)
        .expect("read cpu.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).expect("cpu row must be valid JSON"))
        .collect();
    assert_eq!(rows.len(), 2, "one row per sample");
    // First sample has no prior counters: util is null, but cores/load present.
    assert_eq!(rows[0]["node"], "nodeA");
    assert_eq!(rows[0]["cpu_util_pct"], serde_json::Value::Null);
    assert_eq!(rows[0]["core_count"], 2);
    assert_eq!(rows[0]["loadavg_1m"], 0.5);
    // Second sample computes the delta.
    assert_eq!(rows[1]["cpu_util_pct"], 25.0);
    assert_eq!(rows[1]["core_count"], 2);
}

#[test]
fn emit_cpu_sample_row_marks_missing_proc_stat_unavailable() {
    // A missing /proc/stat must make the function return non-zero (so the
    // collector is marked unavailable) rather than erroring.
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let out = tmpdir.path().join("cpu.jsonl");
    let driver = format!(
        r#"
if emit_cpu_sample_row "2024-01-01T00:00:00Z" "nodeA" "{missing}" "/nonexistent/loadavg" "{state}" "{out}"; then
  echo "unexpected-success"
else
  echo "unavailable rc=$?"
fi
"#,
        missing = tmpdir.path().join("does-not-exist").display(),
        state = tmpdir.path().join("cpu.state").display(),
        out = out.display(),
    );
    let output = run_metrics_bash(tmpdir.path(), &driver);
    assert!(output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("unavailable rc=1"),
        "missing /proc/stat should return non-zero: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(!out.exists(), "no cpu row should be written");
}

#[test]
fn final_metrics_sample_flushes_and_returns_promptly() {
    // E1: final_metrics_sample must run one synchronous sample and return once
    // it is flush-complete. With both collectors disabled the sample is a
    // near-instant no-op, so the bounded wait must not spin for its full budget.
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let driver = r#"
GPU_COLLECTOR_ENABLED=0
SLURM_COLLECTOR_ENABLED=0
final_metrics_sample
echo "final-sample-returned rc=$?"
"#;
    let start = std::time::Instant::now();
    let output = run_metrics_bash(tmpdir.path(), driver);
    let elapsed = start.elapsed();
    assert!(
        output.status.success(),
        "final_metrics_sample failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("final-sample-returned rc=0"),
        "final_metrics_sample did not return cleanly: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        elapsed.as_secs() < 8,
        "final_metrics_sample should not spin its full budget on a no-op sample (took {elapsed:?})"
    );
}

fn run_artifact_bash(tmpdir: &std::path::Path, driver: &str) -> std::process::Output {
    // No `set -u`: the manifest writer expands several arrays that are legitimately
    // empty (e.g. bundles), which would trip unbound-variable checks.
    let mut script = String::from("#!/usr/bin/env bash\n");
    script.push_str("json_escape() { printf '%s' \"$1\"; }\n\n");
    render_artifact_helpers(&mut script);
    script.push_str(driver);
    let path = tmpdir.join("artifact.sh");
    fs::write(&path, &script).expect("write artifact script");
    Command::new(bash_executable())
        .arg(&path)
        .output()
        .expect("run artifact script")
}

#[test]
fn artifact_manifest_is_written_as_valid_json() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let manifest = tmpdir.path().join("manifest.json");
    let driver = format!(
        r#"
SLURM_JOB_ID=12345
ARTIFACTS_MANIFEST_FILE="{manifest}"
ARTIFACTS_COLLECT_POLICY=on_success
RESUME_ENABLED=0
ARTIFACT_SOURCE_PATTERNS=("out/*.txt" "logs/")
ARTIFACT_COPIED_RELATIVE_PATHS=("out/a.txt")
ARTIFACT_WARNINGS=("pattern logs/ matched nothing")
ARTIFACT_BUNDLE_NAMES=()
write_artifact_manifest success "out/a.txt"
"#,
        manifest = manifest.display()
    );
    let output = run_artifact_bash(tmpdir.path(), &driver);
    assert!(
        output.status.success(),
        "write_artifact_manifest failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&manifest).expect("read manifest"))
            .expect("artifact manifest must be valid JSON");
    assert_eq!(value["schema_version"], 3);
    assert_eq!(value["job_id"], "12345");
    assert_eq!(value["collect_policy"], "on_success");
    assert_eq!(value["job_outcome"], "success");
    // RESUME_ENABLED=0 -> resume fields are null.
    assert_eq!(value["attempt"], serde_json::Value::Null);
    assert_eq!(value["is_resume"], serde_json::Value::Null);
    assert_eq!(
        value["declared_source_patterns"],
        serde_json::json!(["out/*.txt", "logs/"])
    );
    assert_eq!(
        value["matched_source_paths"],
        serde_json::json!(["out/a.txt"])
    );
    assert_eq!(
        value["copied_relative_paths"],
        serde_json::json!(["out/a.txt"])
    );
    assert_eq!(
        value["warnings"],
        serde_json::json!(["pattern logs/ matched nothing"])
    );
    assert!(value["bundles"].is_object(), "bundles must be an object");
}

#[test]
fn render_exports_parallelism_env_for_single_node_service() {
    // Default placement is single-node (nodes == 1), so the distributed helper
    // family is NOT emitted; TP/PP must still be exported.
    let mut service = runtime_service();
    service.slurm = ServiceSlurmConfig {
        parallelism: Some(ParallelismConfig {
            tensor: 2,
            pipeline: 4,
        }),
        ..ServiceSlurmConfig::default()
    };
    assert_eq!(service.placement.nodes, 1, "regression: single-node");
    let plan = RuntimePlan {
        name: "tp-pp".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let script = render_script(&plan).expect("script");
    assert!(
        script.contains("launch_env+=(\"HPC_COMPOSE_TP_SIZE=2\")"),
        "missing TP size export"
    );
    assert!(
        script.contains("launch_env+=(\"HPC_COMPOSE_PP_SIZE=4\")"),
        "missing PP size export"
    );
    // Single-node: must not have brought in the distributed master-addr export.
    assert!(
        !script.contains("HPC_COMPOSE_DIST_MASTER_ADDR"),
        "single-node service should not emit distributed helpers"
    );
}

#[test]
fn render_omits_parallelism_env_when_unset() {
    let service = runtime_service();
    let plan = RuntimePlan {
        name: "no-tp".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let script = render_script(&plan).expect("script");
    assert!(!script.contains("HPC_COMPOSE_TP_SIZE"));
    assert!(!script.contains("HPC_COMPOSE_PP_SIZE"));
}

#[test]
fn parallelism_environment_names_surface_only_when_declared() {
    let mut service = runtime_service();
    assert!(parallelism_environment_names_for_service(&service).is_empty());
    service.slurm.parallelism = Some(ParallelismConfig {
        tensor: 1,
        pipeline: 1,
    });
    assert_eq!(
        parallelism_environment_names_for_service(&service),
        vec![
            "HPC_COMPOSE_TP_SIZE".to_string(),
            "HPC_COMPOSE_PP_SIZE".to_string()
        ]
    );
}

// Pins the global vs. per-service `x-slurm` precedence rule documented in
// docs/src/spec-reference.md ("Global vs. per-service `x-slurm` precedence"):
//   * The `#SBATCH` allocation header is rendered from the top-level `x-slurm`
//     block ONLY; per-service overrides never reach it.
//   * Each service's `srun` line is rendered from that service's per-service
//     `x-slurm` block; for the GPU/CPU/binding/distribution fields the global
//     value is NOT inherited onto the `srun` line.
//   * `ntasks` / `ntasks_per_node` DO inherit from the global block when the
//     service omits them (per-service overrides, then falls back to global).
fn plan_service_spec(image: &str, slurm: ServiceSlurmConfig) -> ServiceSpec {
    ServiceSpec {
        image: Some(image.to_string()),
        command: None,
        entrypoint: None,
        script: None,
        env_file: None,
        environment: EnvironmentSpec::None,
        volumes: Vec::new(),
        working_dir: None,
        depends_on: DependsOnSpec::None,
        readiness: None,
        healthcheck: None,
        assertions: None,
        software_env: crate::spec::SoftwareEnvConfig::default(),
        slurm,
        runtime: ServiceRuntimeConfig::default(),
        enroot: ServiceEnrootConfig::default(),
    }
}

fn render_from_spec(global: SlurmConfig, services: BTreeMap<String, ServiceSpec>) -> String {
    let spec = ComposeSpec {
        secrets: BTreeMap::new(),
        name: Some("precedence-demo".into()),
        runtime: RuntimeConfig::default(),
        software_env: crate::spec::SoftwareEnvConfig::default(),
        slurm: global,
        services,
        sweep: None,
    };
    let plan = crate::planner::build_plan_with_options(
        std::path::Path::new("."),
        spec,
        crate::planner::PlanOptions::default(),
    )
    .expect("plan");
    let runtime = crate::prepare::build_runtime_plan(&plan);
    render_script(&runtime).expect("script")
}

fn sbatch_header_lines(script: &str) -> Vec<&str> {
    script
        .lines()
        .filter(|line| line.starts_with("#SBATCH "))
        .collect()
}

fn srun_cmd_line(script: &str) -> &str {
    script
        .lines()
        .find(|line| line.contains("srun_cmd="))
        .expect("service srun_cmd line")
}

#[test]
fn global_and_per_service_x_slurm_precedence_is_pinned() {
    // --- Scope separation: header from global, srun line from per-service. ---
    // NOTE: `gpus` is deliberately NOT set alongside `gres` here — since the
    // gpus/gres cross-field guard landed, setting both in one scope is a hard
    // validation error (the old silent "gres wins" precedence no longer exists).
    let global = SlurmConfig {
        cpus_per_task: Some(8),
        gres: Some("gpu:global:4".into()),
        distribution: Some("cyclic".into()),
        ..SlurmConfig::default()
    };
    let worker = plan_service_spec(
        "alpine:latest",
        ServiceSlurmConfig {
            cpus_per_task: Some(2),
            gres: Some("gpu:svc:1".into()),
            distribution: Some("block:block".into()),
            ..ServiceSlurmConfig::default()
        },
    );
    let script = render_from_spec(global, BTreeMap::from([("worker".to_string(), worker)]));

    // The #SBATCH header carries the GLOBAL values, and only those.
    let header = sbatch_header_lines(&script).join("\n");
    assert!(
        header.contains("#SBATCH --cpus-per-task=8"),
        "header cpus: {header}"
    );
    assert!(
        header.contains("#SBATCH --gres=gpu:global:4"),
        "header gres: {header}"
    );
    assert!(
        header.contains("#SBATCH --distribution=cyclic"),
        "header dist: {header}"
    );
    // Per-service overrides never reach the allocation header.
    assert!(
        !header.contains("--cpus-per-task=2"),
        "header leaked svc cpus: {header}"
    );
    assert!(
        !header.contains("gpu:svc:1"),
        "header leaked svc gres: {header}"
    );
    assert!(
        !header.contains("--distribution=block:block"),
        "header leaked svc dist: {header}"
    );
    // A gres request alone emits no plain `--gpus` flag on the header.
    assert!(
        !header.contains("#SBATCH --gpus="),
        "header should carry gres only: {header}"
    );

    // The service srun line carries the PER-SERVICE values, and only those.
    let srun = srun_cmd_line(&script);
    assert!(srun.contains("--cpus-per-task=2"), "srun cpus: {srun}");
    assert!(srun.contains("--gres=gpu:svc:1"), "srun gres: {srun}");
    assert!(
        srun.contains("--distribution=block:block"),
        "srun dist: {srun}"
    );
    // Global values are NOT inherited onto the srun line for these fields.
    assert!(
        !srun.contains("--cpus-per-task=8"),
        "srun leaked global cpus: {srun}"
    );
    assert!(
        !srun.contains("gpu:global:4"),
        "srun leaked global gres: {srun}"
    );
    assert!(
        !srun.contains("--distribution=cyclic"),
        "srun leaked global dist: {srun}"
    );
    // A gres request alone emits no plain `--gpus` flag on the srun line either.
    assert!(
        !srun.contains("--gpus="),
        "srun should carry gres only: {srun}"
    );

    // --- ntasks_per_node: inherited from global when the service omits it. ---
    let global = SlurmConfig {
        nodes: Some(2),
        ntasks_per_node: Some(3),
        ..SlurmConfig::default()
    };
    let inheritor = plan_service_spec("alpine:latest", ServiceSlurmConfig::default());
    let script = render_from_spec(
        global.clone(),
        BTreeMap::from([("worker".to_string(), inheritor)]),
    );
    let header = sbatch_header_lines(&script).join("\n");
    assert!(
        header.contains("#SBATCH --ntasks-per-node=3"),
        "header tpn: {header}"
    );
    let srun = srun_cmd_line(&script);
    assert!(
        srun.contains("--ntasks-per-node=3"),
        "srun should inherit global ntasks-per-node: {srun}"
    );

    // --- ntasks_per_node: per-service value overrides the global one. ---
    let overrider = plan_service_spec(
        "alpine:latest",
        ServiceSlurmConfig {
            ntasks_per_node: Some(1),
            ..ServiceSlurmConfig::default()
        },
    );
    let script = render_from_spec(global, BTreeMap::from([("worker".to_string(), overrider)]));
    let header = sbatch_header_lines(&script).join("\n");
    // Header still reflects the global allocation request.
    assert!(
        header.contains("#SBATCH --ntasks-per-node=3"),
        "header tpn override: {header}"
    );
    let srun = srun_cmd_line(&script);
    assert!(
        srun.contains("--ntasks-per-node=1"),
        "srun should use per-service ntasks-per-node: {srun}"
    );
    assert!(
        !srun.contains("--ntasks-per-node=3"),
        "srun should not inherit global when overridden: {srun}"
    );
}

fn annotate_fixture_plan() -> RuntimePlan {
    let mut api = runtime_service();
    api.name = "api".into();
    api.readiness = Some(ReadinessSpec::Tcp {
        host: Some("127.0.0.1".into()),
        port: 8080,
        timeout_seconds: Some(20),
    });
    let mut worker = runtime_service();
    worker.name = "worker".into();
    worker.readiness = None;
    worker.depends_on = vec![ServiceDependency {
        name: "api".into(),
        condition: DependencyCondition::ServiceHealthy,
        implicit: false,
    }];
    RuntimePlan {
        name: "annotated".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig {
            time: Some("00:10:00".into()),
            mem: Some("4G".into()),
            partition: Some("gpu".into()),
            requeue: Some(true),
            submit_args: vec!["--exclusive".into()],
            setup: vec!["module load cuda".into()],
            metrics: Some(crate::spec::MetricsConfig {
                enabled: Some(true),
                interval_seconds: Some(5),
                collectors: vec![MetricsCollector::Slurm],
            }),
            artifacts: Some(crate::spec::ArtifactsConfig {
                collect: ArtifactCollectPolicy::Always,
                export_dir: Some("./results/${SLURM_JOB_ID}".into()),
                paths: vec!["/hpc-compose/job/metrics/**".into()],
                bundles: BTreeMap::new(),
            }),
            ..SlurmConfig::default()
        },
        ordered_services: vec![api, worker],
    }
}

fn render_annotated_script(plan: &RuntimePlan) -> String {
    render_script_with_options(
        plan,
        &RenderOptions {
            annotate: true,
            ..RenderOptions::default()
        },
    )
    .expect("annotated script")
}

#[test]
fn annotate_places_field_comments_directly_above_their_directives() {
    let plan = annotate_fixture_plan();
    let script = render_annotated_script(&plan);
    // Field comments sit on their own line immediately above the directive
    // they explain (never trailing on the #SBATCH line itself).
    assert!(script.contains("# <- x-slurm.time\n#SBATCH --time=00:10:00\n"));
    assert!(script.contains("# <- x-slurm.mem\n#SBATCH --mem=4G\n"));
    assert!(script.contains("# <- x-slurm.partition\n#SBATCH --partition=gpu\n"));
    assert!(script.contains("# <- name\n#SBATCH --job-name=annotated\n"));
    assert!(script.contains("# <- x-slurm.requeue\n#SBATCH --requeue\n"));
    assert!(script.contains("# <- x-slurm.submit_args\n#SBATCH --exclusive\n"));
    // Readiness gates and dependency waits map back to their service fields.
    assert!(script.contains("# <- services.api.readiness.tcp\nwait_until_api_ready()"));
    assert!(
        script.contains(
            "# <- services.worker.depends_on[api].condition\nwait_for_service_healthy 'api' 'worker' wait_until_api_ready\n"
        )
    );
    // Feature-block banners name the section and the enabling field.
    assert!(script.contains("# --- artifact helpers (x-slurm.artifacts) ---\n"));
    assert!(script.contains("# --- metrics helpers (x-slurm.metrics) ---\n"));
    assert!(script.contains("# --- setup commands (x-slurm.setup) ---\nmodule load cuda\n"));
    assert!(script.contains("# --- service api (services.api) ---\n"));
    assert!(script.contains("# --- service worker (services.worker) ---\n"));
    // Annotated output must still be valid bash.
    assert_bash_syntax(&script);
}

#[test]
fn annotate_uses_job_name_source_when_set() {
    let mut plan = annotate_fixture_plan();
    plan.slurm.job_name = Some("annotated".into());
    let script = render_annotated_script(&plan);
    assert!(script.contains("# <- x-slurm.job_name\n#SBATCH --job-name=annotated\n"));
}

#[test]
fn annotate_off_renders_byte_identically_and_without_markers() {
    let plan = annotate_fixture_plan();
    let plain = render_script(&plan).expect("plain script");
    // The default render carries no annotation vocabulary at all.
    assert!(!plain.contains("# <- "));
    assert!(!plain.contains("# --- "));
    // Rendering through the span-collecting entry point with annotate off is
    // byte-identical to the plain render.
    let (unannotated, spans) =
        render_script_annotated(&plan, &RenderOptions::default()).expect("unannotated");
    assert_eq!(plain, unannotated);
    assert!(
        !spans.is_empty(),
        "spans are recorded even without comments"
    );
    // Stripping exactly the interleaved comment lines from an annotated render
    // reproduces the plain render byte-for-byte, so annotation cannot change
    // any generated line.
    let annotated = render_annotated_script(&plan);
    let stripped = annotated
        .lines()
        .filter(|line| !line.starts_with("# <- ") && !line.starts_with("# --- "))
        .map(|line| format!("{line}\n"))
        .collect::<String>();
    assert_eq!(plain, stripped);
}

#[test]
fn annotate_banners_appear_only_when_their_feature_block_is_enabled() {
    let bare = RuntimePlan {
        name: "bare".into(),
        cache_dir: PathBuf::from("/shared/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![runtime_service()],
    };
    let script = render_annotated_script(&bare);
    assert!(!script.contains("artifact helpers (x-slurm.artifacts)"));
    assert!(!script.contains("metrics helpers (x-slurm.metrics)"));
    assert!(!script.contains("setup commands (x-slurm.setup)"));
    assert!(!script.contains("rendezvous helpers (x-slurm.rendezvous)"));
    // Service banners always render: every service owns a launch function.
    assert!(script.contains("# --- service worker (services.worker) ---\n"));
}

#[test]
fn annotate_renders_cleanly_across_backends() {
    for backend in [RuntimeBackend::Host, RuntimeBackend::Pyxis] {
        let mut plan = annotate_fixture_plan();
        plan.runtime = crate::spec::RuntimeConfig {
            backend,
            gpu: RuntimeGpuPolicy::default(),
        };
        let script = render_annotated_script(&plan);
        assert!(script.contains("# <- x-slurm.time\n#SBATCH --time=00:10:00\n"));
        assert_bash_syntax(&script);
    }
}
