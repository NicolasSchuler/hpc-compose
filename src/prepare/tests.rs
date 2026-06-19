use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::sync::{Mutex, OnceLock};

use super::*;
use crate::planner::{ImageSource, Plan, PlannedService, PreparedImageSpec, ServicePlacement};
use crate::spec::{ServiceFailurePolicy, SlurmConfig};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn fake_service(tmpdir: &Path) -> RuntimeService {
    RuntimeService {
        name: "svc".into(),
        runtime_image: tmpdir.join("prepared/svc.sqsh"),
        execution: ExecutionSpec::Shell("echo ready".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: None,
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: Some(PreparedImageSpec {
            commands: vec!["echo setup".into()],
            mounts: Vec::new(),
            env: vec![("A".into(), "B".into())],
            root: true,
            force_rebuild: false,
        }),
        source: ImageSource::Remote("docker://redis:7".into()),
    }
}

#[test]
fn runtime_plan_conversion_preserves_planned_service_contract() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let prepare = PreparedImageSpec {
        commands: vec!["echo setup".into()],
        mounts: vec!["/host/input:/input:ro".into()],
        env: vec![("A".into(), "B".into())],
        root: true,
        force_rebuild: true,
    };
    let planned = PlannedService {
        name: "app".into(),
        image: ImageSource::Remote("docker://python:3.11-slim".into()),
        execution: ExecutionSpec::Exec(vec!["python".into(), "-m".into(), "app".into()]),
        environment: vec![("ENV".into(), "prod".into())],
        volumes: vec!["/host/app:/app".into()],
        working_dir: Some("/app".into()),
        depends_on: vec![ServiceDependency {
            name: "db".into(),
            condition: crate::spec::DependencyCondition::ServiceStarted,
            implicit: false,
        }],
        readiness: Some(ReadinessSpec::Sleep { seconds: 1 }),
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement {
            nodes: 2,
            ntasks: Some(4),
            node_indices: Some(vec![0, 1]),
            ..ServicePlacement::default()
        },
        slurm: ServiceSlurmConfig {
            cpus_per_task: Some(2),
            ..ServiceSlurmConfig::default()
        },
        prepare: Some(prepare),
    };
    let plan = Plan {
        name: "demo".into(),
        project_dir: tmpdir.path().to_path_buf(),
        spec_path: tmpdir.path().join("compose.yaml"),
        runtime: RuntimeConfig::default(),
        cache_dir: tmpdir.path().join("cache"),
        slurm: SlurmConfig {
            time: Some("00:10:00".into()),
            ..SlurmConfig::default()
        },
        ordered_services: vec![planned.clone()],
    };

    let runtime_plan = build_runtime_plan(&plan);
    assert_eq!(runtime_plan.name, plan.name);
    assert_eq!(runtime_plan.cache_dir, plan.cache_dir);
    assert_eq!(runtime_plan.runtime.backend, plan.runtime.backend);
    assert_eq!(runtime_plan.slurm.time, plan.slurm.time);
    let runtime = runtime_plan.ordered_services.first().expect("service");
    assert_eq!(runtime.name, planned.name);
    assert_eq!(runtime.execution, planned.execution);
    assert_eq!(runtime.environment, planned.environment);
    assert_eq!(runtime.volumes, planned.volumes);
    assert_eq!(runtime.working_dir, planned.working_dir);
    assert_eq!(runtime.depends_on, planned.depends_on);
    assert_eq!(runtime.readiness, planned.readiness);
    assert_eq!(runtime.failure_policy, planned.failure_policy);
    assert_eq!(runtime.placement, planned.placement);
    assert_eq!(runtime.slurm.cpus_per_task, planned.slurm.cpus_per_task);
    assert_eq!(runtime.prepare, planned.prepare);
    assert_eq!(runtime.source, planned.image);
    assert!(
        runtime
            .runtime_image
            .starts_with(plan.cache_dir.join("prepared"))
    );
}

fn write_fake_enroot(tmpdir: &Path, log_path: &Path) -> PathBuf {
    let script = tmpdir.join("fake-enroot.sh");
    let template = r#"#!/bin/bash
set -euo pipefail
echo "$@" >> __LOG_PATH__
cmd="$1"
shift
case "$cmd" in
  import)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output)
          output="$2"
          shift 2
          ;;
        *)
          shift
          ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  create)
    name=""
    while (($#)); do
      case "$1" in
        -n|--name)
          name="$2"
          shift 2
          ;;
        -f|--force)
          shift
          ;;
        *)
          image="$1"
          shift
          ;;
      esac
    done
    mkdir -p "$ENROOT_DATA_PATH/$name"
    ;;
  start)
    if printf '%s\n' "$@" | grep -q "fail-me"; then
      exit 41
    fi
    ;;
  export)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output|--output=*)
          if [[ "$1" == *=* ]]; then
            output="${1#*=}"
            shift
          else
            output="$2"
            shift 2
          fi
          ;;
        -f|--force)
          shift
          ;;
        *)
          shift
          ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  remove)
    while (($#)); do
      case "$1" in
        -f|--force)
          shift
          ;;
        *)
          rm -rf "$ENROOT_DATA_PATH/$1"
          shift
          ;;
      esac
    done
    ;;
esac
"#;
    let content = template.replace(
        "__LOG_PATH__",
        &shell_quote_for_test(&log_path.display().to_string()),
    );
    fs::write(&script, content).expect("write fake enroot");
    let mut perms = fs::metadata(&script).expect("meta").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&script, perms).expect("chmod");
    script
}

fn write_fake_sif_runtime(tmpdir: &Path, log_path: &Path) -> PathBuf {
    let script = tmpdir.join("fake-sif-runtime.sh");
    let template = r#"#!/bin/bash
set -euo pipefail
echo "$@" >> __LOG_PATH__
cmd="${1:-}"
if [[ $# -gt 0 ]]; then
  shift
fi
case "$cmd" in
  build)
    sandbox=0
    target=""
    while (($#)); do
      case "$1" in
        --sandbox)
          sandbox=1
          shift
          ;;
        --force|--fakeroot)
          shift
          ;;
        *)
          target="$1"
          break
          ;;
      esac
    done
    if [[ -z "$target" ]]; then
      echo "missing build target" >&2
      exit 64
    fi
    if (( sandbox )); then
      mkdir -p "$target"
    else
      mkdir -p "$(dirname "$target")"
      touch "$target"
    fi
    ;;
  exec)
    if printf '%s\n' "$@" | grep -q "fail-me"; then
      echo "prepare failed" >&2
      exit 41
    fi
    ;;
esac
"#;
    let content = template.replace(
        "__LOG_PATH__",
        &shell_quote_for_test(&log_path.display().to_string()),
    );
    fs::write(&script, content).expect("write fake sif runtime");
    let mut perms = fs::metadata(&script).expect("meta").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&script, perms).expect("chmod");
    script
}

fn shell_quote_for_test(value: &str) -> String {
    let escaped = value.replace('\'', "'\"'\"'");
    format!("'{escaped}'")
}

#[test]
fn prepare_pipeline_imports_and_exports() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);

    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![fake_service(tmpdir.path())],
    };
    let options = PrepareOptions {
        enroot_bin: fake.display().to_string(),
        keep_failed_prep: false,
        force_rebuild: false,
        ..PrepareOptions::default()
    };

    let summary = prepare_runtime_plan(&plan, &options).expect("prepare");
    assert!(plan.ordered_services[0].runtime_image.exists());
    assert_eq!(
        summary.services[0].runtime_image.action,
        ArtifactAction::Built
    );
    let log_content = fs::read_to_string(log).expect("log");
    assert!(log_content.contains("import"));
    assert!(log_content.contains("create --force --name"));
    assert!(log_content.contains("export --force --output"));
    assert!(crate::cache::manifest_path_for(&plan.ordered_services[0].runtime_image).exists());
}

#[test]
fn cached_prepared_image_skips_rebuild_without_mounts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);

    let service = fake_service(tmpdir.path());
    let runtime_image = service.runtime_image.clone();
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let options = PrepareOptions {
        enroot_bin: fake.display().to_string(),
        keep_failed_prep: false,
        force_rebuild: false,
        ..PrepareOptions::default()
    };

    prepare_runtime_plan(&plan, &options).expect("prepare once");
    fs::write(&log, "").expect("clear log");
    fs::write(&runtime_image, "cached").expect("seed");
    prepare_runtime_plan(&plan, &options).expect("prepare twice");
    let log_content = fs::read_to_string(log).expect("log");
    assert!(!log_content.contains("create --force"));
}

#[test]
fn prepare_mounts_force_rebuild_even_with_existing_image() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);

    let mut service = fake_service(tmpdir.path());
    service.prepare.as_mut().expect("prepare").mounts = vec!["/host:/mnt".into()];
    service.prepare.as_mut().expect("prepare").force_rebuild = true;
    fs::create_dir_all(service.runtime_image.parent().expect("parent")).expect("mkdir");
    fs::write(&service.runtime_image, "cached").expect("seed");

    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let options = PrepareOptions {
        enroot_bin: fake.display().to_string(),
        keep_failed_prep: false,
        force_rebuild: false,
        ..PrepareOptions::default()
    };

    prepare_runtime_plan(&plan, &options).expect("prepare");
    let log_content = fs::read_to_string(log).expect("log");
    assert!(log_content.contains("create --force --name"));
}

#[test]
fn identical_remote_images_share_base_cache_path() {
    let service_a = RuntimeService {
        name: "a".into(),
        runtime_image: PathBuf::from("/tmp/a.sqsh"),
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
        source: ImageSource::Remote("docker://redis:7".into()),
    };
    let service_b = RuntimeService {
        name: "b".into(),
        ..service_a.clone()
    };
    assert_eq!(
        base_image_path(Path::new("/shared/cache"), &service_a),
        base_image_path(Path::new("/shared/cache"), &service_b)
    );
}

#[test]
fn sif_backends_use_sif_cache_paths_for_remote_images() {
    let service = RuntimeService {
        name: "app".into(),
        runtime_image: PathBuf::from("/tmp/app.sif"),
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
        source: ImageSource::Remote("docker://ubuntu:24.04".into()),
    };

    let cache_dir = Path::new("/shared/cache");
    assert!(
        base_image_path_for_backend(cache_dir, &service, RuntimeBackend::Apptainer)
            .display()
            .to_string()
            .ends_with(".sif")
    );
    assert!(
        base_image_path_for_backend(cache_dir, &service, RuntimeBackend::Pyxis)
            .display()
            .to_string()
            .ends_with(".sqsh")
    );
}

#[test]
fn failed_prepare_cleans_up_by_default() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);

    let mut service = fake_service(tmpdir.path());
    service.prepare.as_mut().expect("prepare").commands = vec!["fail-me".into()];
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let options = PrepareOptions {
        enroot_bin: fake.display().to_string(),
        keep_failed_prep: false,
        force_rebuild: false,
        ..PrepareOptions::default()
    };

    let err = prepare_runtime_plan(&plan, &options).expect_err("should fail");
    assert!(err.to_string().contains("prepare command"));
    let log_content = fs::read_to_string(log).expect("log");
    assert!(log_content.contains("remove --force"));
}

#[test]
fn force_rebuild_option_rebuilds_prepared_images() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);

    let service = fake_service(tmpdir.path());
    let runtime_image = service.runtime_image.clone();
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };

    prepare_runtime_plan(
        &plan,
        &PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
            ..PrepareOptions::default()
        },
    )
    .expect("prepare once");
    fs::write(&log, "").expect("clear log");
    fs::write(&runtime_image, "cached").expect("seed");

    let summary = prepare_runtime_plan(
        &plan,
        &PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: true,
            ..PrepareOptions::default()
        },
    )
    .expect("prepare twice");
    let log_content = fs::read_to_string(log).expect("log");
    assert!(log_content.contains("create --force --name"));
    assert_eq!(
        summary.services[0].runtime_image.action,
        ArtifactAction::Built
    );
    assert_eq!(
        summary.services[0].runtime_image.note.as_deref(),
        Some("rebuilt because --force/--force-rebuild was requested")
    );
}

#[test]
fn helper_defaults_and_paths_cover_remaining_prepare_helpers() {
    let defaults = PrepareOptions::default();
    assert_eq!(defaults.enroot_bin, "enroot");
    assert!(!defaults.keep_failed_prep);
    assert!(!defaults.force_rebuild);

    let cache_dir = Path::new("/shared/cache");
    let service = RuntimeService {
        name: "svc/name".into(),
        runtime_image: PathBuf::from("/tmp/runtime.sqsh"),
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
        source: ImageSource::LocalSqsh(PathBuf::from("/tmp/local-image.sqsh")),
    };
    assert_eq!(base_image_cache_key(&service).len(), 64);
    assert!(temporary_rootfs_name(&service).starts_with("hpc-compose-svc_name-"));
    assert_eq!(short_hash("1234567890abcdef1234"), "1234567890abcdef");
    assert_eq!(sanitize_name("svc/name"), "svc_name");
    assert_eq!(image_label(&service.source), "local-image");
    let envs = enroot_env(cache_dir);
    assert_eq!(envs.len(), 2);
    assert!(envs[0].1.contains("enroot/cache"));
    assert!(!envs.iter().any(|(key, _)| key == "ENROOT_TEMP_PATH"));
}

#[test]
fn prepare_runtime_plan_covers_local_missing_and_remote_without_prepare() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);

    let local_present_path = tmpdir.path().join("present.sqsh");
    fs::write(&local_present_path, "x").expect("present local");
    let local_present = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache-local"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![RuntimeService {
            name: "local-present".into(),
            runtime_image: local_present_path.clone(),
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
            source: ImageSource::LocalSqsh(local_present_path.clone()),
        }],
    };
    let local_summary = prepare_runtime_plan(
        &local_present,
        &PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
            ..PrepareOptions::default()
        },
    )
    .expect("local present");
    assert_eq!(
        local_summary.services[0].runtime_image.action,
        ArtifactAction::Present
    );
    assert_eq!(
        local_summary.services[0].runtime_image.note.as_deref(),
        Some("uses local .sqsh directly")
    );

    let local_missing = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![RuntimeService {
            name: "local".into(),
            runtime_image: tmpdir.path().join("local.sqsh"),
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
            source: ImageSource::LocalSqsh(tmpdir.path().join("missing.sqsh")),
        }],
    };
    let err = prepare_runtime_plan(
        &local_missing,
        &PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
            ..PrepareOptions::default()
        },
    )
    .expect_err("local missing");
    assert!(err.to_string().contains("does not exist"));

    let remote_no_prepare = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache2"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![RuntimeService {
            name: "redis".into(),
            runtime_image: tmpdir.path().join("cache2/base/redis.sqsh"),
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
            source: ImageSource::Remote("docker://redis:7".into()),
        }],
    };
    let summary = prepare_runtime_plan(
        &remote_no_prepare,
        &PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
            ..PrepareOptions::default()
        },
    )
    .expect("remote no prepare");
    assert_eq!(
        summary.services[0].runtime_image.action,
        ArtifactAction::Built
    );
    assert_eq!(
        summary.services[0].runtime_image.note.as_deref(),
        Some("base cache artifact is used directly at runtime")
    );
}

#[test]
fn local_sqsh_prepare_and_helper_failures_cover_remaining_branches() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);
    let local_base = tmpdir.path().join("local-base.sqsh");
    fs::write(&local_base, "x").expect("local base");

    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![RuntimeService {
            name: "local-prepared".into(),
            runtime_image: tmpdir.path().join("cache/prepared/local-prepared.sqsh"),
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
            prepare: Some(PreparedImageSpec {
                commands: vec!["echo local".into()],
                mounts: vec!["/host:/mnt".into()],
                env: vec![("KEY".into(), "VALUE".into())],
                root: false,
                force_rebuild: false,
            }),
            source: ImageSource::LocalSqsh(local_base),
        }],
    };
    let summary = prepare_runtime_plan(
        &plan,
        &PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
            ..PrepareOptions::default()
        },
    )
    .expect("local prepare");
    assert!(summary.services[0].base_image.is_none());
    assert_eq!(
        summary.services[0].runtime_image.action,
        ArtifactAction::Built
    );
    let log_content = fs::read_to_string(&log).expect("log");
    assert!(!log_content.contains("import"));
    assert!(log_content.contains("--mount /host:/mnt"));
    assert!(!log_content.contains("start --root --rw"));

    let err = ensure_parent_dir(Path::new("/")).expect_err("root has no parent");
    assert!(err.to_string().contains("does not have a parent directory"));

    let err = run_enroot(
        "/definitely/missing/enroot",
        &[],
        vec!["version".to_string()],
        "probe missing binary",
    )
    .expect_err("missing binary execution");
    assert!(err.to_string().contains("failed to execute"));
}

#[test]
fn keep_failed_prep_and_binary_errors_cover_failure_paths() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);

    let mut service = fake_service(tmpdir.path());
    service.prepare.as_mut().expect("prepare").commands = vec!["fail-me".into()];
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let err = prepare_runtime_plan(
        &plan,
        &PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: true,
            force_rebuild: false,
            ..PrepareOptions::default()
        },
    )
    .expect_err("should fail");
    assert!(err.to_string().contains("prepare command"));
    let log_content = fs::read_to_string(log).expect("log");
    let remove_count = log_content.matches("remove --force").count();
    assert_eq!(remove_count, 1);

    let err = ensure_binary_available("/definitely/missing/enroot", "missing")
        .expect_err("missing binary");
    assert!(err.to_string().contains("missing"));
}

#[test]
fn helper_paths_binary_search_and_run_failures_are_reported() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: cache_dir.clone(),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: Vec::new(),
    };
    create_cache_dirs(&plan).expect("create cache dirs");
    for suffix in [
        "base",
        "prepared",
        "enroot/cache",
        "enroot/data",
        "enroot/tmp",
    ] {
        assert!(cache_dir.join(suffix).exists(), "{suffix} missing");
    }

    let _guard = env_lock().lock().expect("env lock");
    let bin_dir = tmpdir.path().join("bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    let helper = bin_dir.join("enroot-ok");
    fs::write(
            &helper,
            "#!/bin/bash\nset -euo pipefail\nif [[ \"${1:-}\" == fail ]]; then echo boom >&2; exit 7; fi\nexit 0\n",
        )
        .expect("helper");
    let mut perms = fs::metadata(&helper).expect("meta").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&helper, perms).expect("chmod");
    let old_path = env::var_os("PATH");
    let joined = env::join_paths(
        std::iter::once(bin_dir.clone())
            .chain(old_path.as_ref().into_iter().flat_map(env::split_paths)),
    )
    .expect("join path");
    unsafe {
        env::set_var("PATH", joined);
    }
    ensure_binary_available("enroot-ok", "missing in path").expect("binary on path");
    let err = run_enroot(
        helper.to_str().expect("helper"),
        &[],
        vec!["fail".to_string()],
        "run failing command",
    )
    .expect_err("failing helper");
    assert!(
        err.to_string()
            .contains("failed to run failing command: boom")
    );
    match old_path {
        Some(value) => unsafe {
            env::set_var("PATH", value);
        },
        None => unsafe {
            env::remove_var("PATH");
        },
    }
}

#[test]
fn sif_remote_base_builds_reuses_and_writes_manifest() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("sif-runtime.log");
    let fake = write_fake_sif_runtime(tmpdir.path(), &log);
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services: {}\n").expect("compose");
    let plan = Plan {
        name: "demo".into(),
        project_dir: tmpdir.path().to_path_buf(),
        spec_path: compose,
        cache_dir: tmpdir.path().join("cache"),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Apptainer,
            ..RuntimeConfig::default()
        },
        slurm: SlurmConfig::default(),
        ordered_services: vec![PlannedService {
            name: "app".into(),
            image: ImageSource::Remote("docker://example.com/app:1".into()),
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
        }],
    };
    let runtime_plan = build_runtime_plan(&plan);
    let options = PrepareOptions {
        apptainer_bin: fake.display().to_string(),
        ..PrepareOptions::default()
    };

    let first = prepare_runtime_plan(&runtime_plan, &options).expect("first prepare");
    assert_eq!(
        first.services[0].base_image.as_ref().expect("base").action,
        ArtifactAction::Built
    );
    assert_eq!(
        first.services[0].runtime_image.note.as_deref(),
        Some("base SIF cache artifact is used directly at runtime")
    );
    assert!(runtime_plan.ordered_services[0].runtime_image.exists());
    let manifest = crate::cache::read_manifest(&runtime_plan.ordered_services[0].runtime_image)
        .expect("base manifest");
    assert_eq!(manifest.kind, crate::cache::CacheEntryKind::Base);
    assert!(
        fs::read_to_string(&log)
            .expect("log")
            .contains("docker://example.com/app:1")
    );

    fs::write(&log, "").expect("clear log");
    let second = prepare_runtime_plan(&runtime_plan, &options).expect("second prepare");
    assert_eq!(
        second.services[0].base_image.as_ref().expect("base").action,
        ArtifactAction::Reused
    );
    assert!(
        !fs::read_to_string(&log)
            .expect("log")
            .contains("build --force")
    );
}

#[test]
fn sif_local_images_are_validated_for_sif_backends() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("sif-runtime.log");
    let fake = write_fake_sif_runtime(tmpdir.path(), &log);
    let local_sif = tmpdir.path().join("local.sif");
    fs::write(&local_sif, "sif").expect("local sif");
    let local_sqsh = tmpdir.path().join("local.sqsh");
    fs::write(&local_sqsh, "sqsh").expect("local sqsh");

    let present = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache-present"),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Singularity,
            ..RuntimeConfig::default()
        },
        slurm: SlurmConfig::default(),
        ordered_services: vec![RuntimeService {
            name: "local-sif".into(),
            runtime_image: local_sif.clone(),
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
            source: ImageSource::LocalSif(local_sif.clone()),
        }],
    };
    let summary = prepare_runtime_plan(
        &present,
        &PrepareOptions {
            singularity_bin: fake.display().to_string(),
            ..PrepareOptions::default()
        },
    )
    .expect("local sif present");
    assert_eq!(
        summary.services[0].runtime_image.action,
        ArtifactAction::Present
    );
    assert_eq!(
        summary.services[0].runtime_image.note.as_deref(),
        Some("uses local .sif directly")
    );

    let missing = RuntimePlan {
        cache_dir: tmpdir.path().join("cache-missing"),
        ordered_services: vec![RuntimeService {
            name: "missing-sif".into(),
            runtime_image: tmpdir.path().join("missing.sif"),
            source: ImageSource::LocalSif(tmpdir.path().join("missing.sif")),
            ..present.ordered_services[0].clone()
        }],
        ..present.clone()
    };
    let err = prepare_runtime_plan(
        &missing,
        &PrepareOptions {
            singularity_bin: fake.display().to_string(),
            ..PrepareOptions::default()
        },
    )
    .expect_err("missing local sif");
    assert!(err.to_string().contains("does not exist"));

    let wrong_format = RuntimePlan {
        cache_dir: tmpdir.path().join("cache-sqsh"),
        ordered_services: vec![RuntimeService {
            name: "local-sqsh".into(),
            runtime_image: local_sqsh.clone(),
            source: ImageSource::LocalSqsh(local_sqsh),
            ..present.ordered_services[0].clone()
        }],
        ..present
    };
    let err = prepare_runtime_plan(
        &wrong_format,
        &PrepareOptions {
            singularity_bin: fake.display().to_string(),
            ..PrepareOptions::default()
        },
    )
    .expect_err("sqsh rejected by sif backend");
    assert!(err.to_string().contains("requires SIF images"));
}

#[test]
fn sif_prepare_sequence_uses_sandbox_flags_and_backend_cache_key() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("sif-runtime.log");
    let fake = write_fake_sif_runtime(tmpdir.path(), &log);
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services: {}\n").expect("compose");
    let prepare = PreparedImageSpec {
        commands: vec!["echo setup".into()],
        mounts: vec!["/host:/mnt".into()],
        env: vec![("KEY".into(), "VALUE".into())],
        root: true,
        force_rebuild: false,
    };
    let plan = Plan {
        name: "demo".into(),
        project_dir: tmpdir.path().to_path_buf(),
        spec_path: compose,
        cache_dir: tmpdir.path().join("cache"),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Apptainer,
            ..RuntimeConfig::default()
        },
        slurm: SlurmConfig::default(),
        ordered_services: vec![PlannedService {
            name: "prepared-sif".into(),
            image: ImageSource::Remote("docker://example.com/prepared:1".into()),
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
            prepare: Some(prepare.clone()),
        }],
    };
    let runtime_plan = build_runtime_plan(&plan);
    let service = &runtime_plan.ordered_services[0];

    let summary = prepare_runtime_plan(
        &runtime_plan,
        &PrepareOptions {
            apptainer_bin: fake.display().to_string(),
            ..PrepareOptions::default()
        },
    )
    .expect("sif prepare");
    assert_eq!(
        summary.services[0].runtime_image.action,
        ArtifactAction::Built
    );
    assert!(service.runtime_image.exists());

    let log_content = fs::read_to_string(&log).expect("log");
    assert!(log_content.contains("build --force --sandbox --fakeroot"));
    assert!(log_content.contains("exec --writable --fakeroot"));
    assert!(log_content.contains("--bind /host:/mnt"));
    assert!(log_content.contains("--env KEY=VALUE"));
    assert!(log_content.contains(&service.runtime_image.display().to_string()));
    assert!(
        !fs::read_dir(runtime_plan.cache_dir.join("prepared"))
            .expect("prepared dir")
            .any(|entry| entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .ends_with(".sandbox"))
    );

    let manifest = crate::cache::read_manifest(&service.runtime_image).expect("manifest");
    let expected_from_plan = prepared_image_cache_key_from_plan(
        &plan.ordered_services[0],
        &prepare,
        RuntimeBackend::Apptainer,
    );
    assert_eq!(manifest.cache_key, expected_from_plan);
    assert_eq!(
        manifest.cache_key,
        prepared_image_cache_key(service, &prepare, RuntimeBackend::Apptainer)
    );
}

#[test]
fn failed_sif_prepare_cleanup_respects_keep_failed_prep() {
    for (keep_failed_prep, should_keep_sandbox) in [(false, false), (true, true)] {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("sif-runtime.log");
        let fake = write_fake_sif_runtime(tmpdir.path(), &log);
        let local_sif = tmpdir.path().join("base.sif");
        fs::write(&local_sif, "sif").expect("local sif");
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: RuntimeConfig {
                backend: RuntimeBackend::Apptainer,
                ..RuntimeConfig::default()
            },
            slurm: SlurmConfig::default(),
            ordered_services: vec![RuntimeService {
                name: "bad-prepare".into(),
                runtime_image: tmpdir.path().join("cache/prepared/bad-prepare.sif"),
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
                prepare: Some(PreparedImageSpec {
                    commands: vec!["fail-me".into()],
                    mounts: Vec::new(),
                    env: Vec::new(),
                    root: false,
                    force_rebuild: false,
                }),
                source: ImageSource::LocalSif(local_sif),
            }],
        };
        let err = prepare_runtime_plan(
            &plan,
            &PrepareOptions {
                apptainer_bin: fake.display().to_string(),
                keep_failed_prep,
                ..PrepareOptions::default()
            },
        )
        .expect_err("prepare failure");
        assert!(err.to_string().contains("run prepare command"));
        let sandbox_left = fs::read_dir(plan.cache_dir.join("prepared"))
            .expect("prepared dir")
            .any(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".sandbox")
            });
        assert_eq!(sandbox_left, should_keep_sandbox);
    }
}

#[test]
fn runtime_path_and_command_helpers_cover_remaining_branches() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services: {}\n").expect("compose");
    let plan = Plan {
        name: "demo".into(),
        project_dir: tmpdir.path().to_path_buf(),
        spec_path: compose,
        cache_dir: tmpdir.path().join("cache"),
        runtime: RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![
            PlannedService {
                name: "local".into(),
                image: ImageSource::LocalSqsh(PathBuf::from("/tmp/local.sqsh")),
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
            },
            PlannedService {
                name: "prepared".into(),
                image: ImageSource::LocalSqsh(PathBuf::from("/tmp/base.sqsh")),
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
                prepare: Some(PreparedImageSpec {
                    commands: vec!["echo hi".into()],
                    mounts: Vec::new(),
                    env: Vec::new(),
                    root: true,
                    force_rebuild: false,
                }),
            },
        ],
    };
    let runtime = build_runtime_plan(&plan);
    assert_eq!(
        runtime.ordered_services[0].runtime_image,
        PathBuf::from("/tmp/local.sqsh")
    );
    assert!(
        runtime.ordered_services[1]
            .runtime_image
            .display()
            .to_string()
            .contains("/prepared/")
    );
    assert!(
        prepared_image_cache_key_from_plan(
            &plan.ordered_services[1],
            plan.ordered_services[1].prepare.as_ref().expect("prepare"),
            plan.runtime.backend
        )
        .len()
            > 10
    );
    assert!(
        prepared_image_cache_key(
            &runtime.ordered_services[1],
            runtime.ordered_services[1]
                .prepare
                .as_ref()
                .expect("prepare"),
            plan.runtime.backend
        )
        .len()
            > 10
    );
}
