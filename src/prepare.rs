//! Runtime artifact preparation and runtime-plan derivation.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::cache::{touch_manifest, upsert_base_manifest, upsert_prepared_manifest};
use crate::planner::{
    ExecutionSpec, ImageSource, Plan, PlannedService, PreparedImageSpec, ServicePlacement,
};
use crate::spec::{
    ReadinessSpec, ServiceDependency, ServiceFailurePolicy, ServiceSlurmConfig, SlurmConfig,
};

/// A plan with concrete runtime image paths for every service.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct RuntimePlan {
    pub name: String,
    pub cache_dir: PathBuf,
    pub slurm: SlurmConfig,
    pub ordered_services: Vec<RuntimeService>,
}

/// A runtime-ready service entry with resolved image artifact paths.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeService {
    pub name: String,
    pub runtime_image: PathBuf,
    pub execution: ExecutionSpec,
    pub environment: Vec<(String, String)>,
    pub volumes: Vec<String>,
    pub working_dir: Option<String>,
    pub depends_on: Vec<ServiceDependency>,
    pub readiness: Option<ReadinessSpec>,
    pub failure_policy: ServiceFailurePolicy,
    pub placement: ServicePlacement,
    pub slurm: ServiceSlurmConfig,
    pub prepare: Option<PreparedImageSpec>,
    pub source: ImageSource,
}

/// Options that control image import and prepare behavior.
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct PrepareOptions {
    pub enroot_bin: String,
    pub keep_failed_prep: bool,
    pub force_rebuild: bool,
}

impl Default for PrepareOptions {
    fn default() -> Self {
        Self {
            enroot_bin: "enroot".to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
        }
    }
}

/// How a runtime artifact was obtained during preparation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactAction {
    /// The artifact already existed and was used as-is.
    Present,
    /// The artifact existed in cache and was refreshed for tracking purposes.
    Reused,
    /// The artifact was built or imported during this run.
    Built,
}

/// Status for one concrete artifact path produced or reused during prepare.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct ArtifactStatus {
    pub path: PathBuf,
    pub action: ArtifactAction,
    pub note: Option<String>,
}

/// Preparation results for one service.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct ServicePrepareResult {
    pub service_name: String,
    pub base_image: Option<ArtifactStatus>,
    pub runtime_image: ArtifactStatus,
}

/// Summary of all service preparations in a runtime plan.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Serialize)]
pub struct PrepareSummary {
    pub services: Vec<ServicePrepareResult>,
}

/// Converts a normalized [`Plan`] into a runtime plan with cache artifact paths.
pub fn build_runtime_plan(plan: &Plan) -> RuntimePlan {
    RuntimePlan {
        name: plan.name.clone(),
        cache_dir: plan.cache_dir.clone(),
        slurm: plan.slurm.clone(),
        ordered_services: plan
            .ordered_services
            .iter()
            .map(|service| RuntimeService {
                name: service.name.clone(),
                runtime_image: runtime_image_path(plan, service),
                execution: service.execution.clone(),
                environment: service.environment.clone(),
                volumes: service.volumes.clone(),
                working_dir: service.working_dir.clone(),
                depends_on: service.depends_on.clone(),
                readiness: service.readiness.clone(),
                failure_policy: service.failure_policy.clone(),
                placement: service.placement.clone(),
                slurm: service.slurm.clone(),
                prepare: service.prepare.clone(),
                source: service.image.clone(),
            })
            .collect(),
    }
}

/// Imports and prepares any missing runtime artifacts for the given plan.
pub fn prepare_runtime_plan(
    plan: &RuntimePlan,
    options: &PrepareOptions,
) -> Result<PrepareSummary> {
    ensure_binary_available(
        &options.enroot_bin,
        "Enroot is required for submit; install it or pass a valid enroot binary path",
    )?;
    let envs = enroot_env(&plan.cache_dir);
    create_cache_dirs(plan)?;
    let mut summary = PrepareSummary::default();
    let mut refreshed_base_images = HashSet::new();

    for service in &plan.ordered_services {
        let mut result = ServicePrepareResult {
            service_name: service.name.clone(),
            base_image: None,
            runtime_image: ArtifactStatus {
                path: service.runtime_image.clone(),
                action: ArtifactAction::Present,
                note: None,
            },
        };

        match &service.source {
            ImageSource::LocalSqsh(path) => {
                if !path.exists() {
                    bail!(
                        "service '{}' references local image '{}', but that file does not exist",
                        service.name,
                        path.display()
                    );
                }
                result.runtime_image = ArtifactStatus {
                    path: path.clone(),
                    action: ArtifactAction::Present,
                    note: Some("uses local .sqsh directly".to_string()),
                };
            }
            ImageSource::Remote(remote) => {
                let base_path = base_image_path(&plan.cache_dir, service);
                let base_cache_key = base_image_cache_key(service);
                let needs_import = !base_path.exists()
                    || (options.force_rebuild && !refreshed_base_images.contains(&base_path));
                let base_action = if needs_import {
                    ensure_parent_dir(&base_path)?;
                    run_enroot(
                        &options.enroot_bin,
                        &envs,
                        [
                            "import".to_string(),
                            "-o".to_string(),
                            base_path.display().to_string(),
                            remote.clone(),
                        ],
                        &format!("import base image for service '{}'", service.name),
                    )?;
                    refreshed_base_images.insert(base_path.clone());
                    ArtifactAction::Built
                } else {
                    ArtifactAction::Reused
                };
                upsert_base_manifest(&base_path, &service.name, &service.source, &base_cache_key)?;
                result.base_image = Some(ArtifactStatus {
                    path: base_path.clone(),
                    action: base_action,
                    note: None,
                });
                if service.prepare.is_none() {
                    result.runtime_image = ArtifactStatus {
                        path: base_path,
                        action: base_action,
                        note: Some("base cache artifact is used directly at runtime".to_string()),
                    };
                }
            }
        }

        let Some(prepare) = &service.prepare else {
            if !matches!(service.source, ImageSource::LocalSqsh(_)) {
                touch_manifest(&service.runtime_image)?;
            }
            summary.services.push(result);
            continue;
        };

        let forced_by_mounts = prepare.force_rebuild;
        let should_rebuild =
            options.force_rebuild || forced_by_mounts || !service.runtime_image.exists();
        if should_rebuild {
            ensure_parent_dir(&service.runtime_image)?;
            prepare_service_image(service, prepare, &plan.cache_dir, options, &envs)?;
            let note = if options.force_rebuild {
                Some("rebuilt because --force/--force-rebuild was requested".to_string())
            } else if forced_by_mounts {
                Some("rebuilt because x-enroot.prepare.mounts are present".to_string())
            } else {
                None
            };
            upsert_prepared_manifest(
                &service.runtime_image,
                &service.name,
                &service.source,
                &prepared_image_cache_key(service, prepare),
                prepare,
            )?;
            result.runtime_image = ArtifactStatus {
                path: service.runtime_image.clone(),
                action: ArtifactAction::Built,
                note,
            };
        } else {
            touch_manifest(&service.runtime_image)?;
            result.runtime_image = ArtifactStatus {
                path: service.runtime_image.clone(),
                action: ArtifactAction::Reused,
                note: None,
            };
        }
        summary.services.push(result);
    }

    Ok(summary)
}

fn prepare_service_image(
    service: &RuntimeService,
    prepare: &PreparedImageSpec,
    cache_dir: &Path,
    options: &PrepareOptions,
    envs: &[(String, String)],
) -> Result<()> {
    let rootfs_name = temporary_rootfs_name(service);
    let base_image = match &service.source {
        ImageSource::LocalSqsh(path) => path.clone(),
        ImageSource::Remote(_) => base_image_path(cache_dir, service),
    };

    let cleanup_result =
        run_prepare_sequence(service, prepare, &rootfs_name, &base_image, options, envs);

    match cleanup_result {
        Ok(()) => {
            remove_rootfs(&options.enroot_bin, envs, &rootfs_name)?;
            Ok(())
        }
        Err(err) => {
            if !options.keep_failed_prep {
                let _ = remove_rootfs(&options.enroot_bin, envs, &rootfs_name);
            }
            Err(err)
        }
    }
}

fn run_prepare_sequence(
    service: &RuntimeService,
    prepare: &PreparedImageSpec,
    rootfs_name: &str,
    base_image: &Path,
    options: &PrepareOptions,
    envs: &[(String, String)],
) -> Result<()> {
    let _ = remove_rootfs(&options.enroot_bin, envs, rootfs_name);

    run_enroot(
        &options.enroot_bin,
        envs,
        vec![
            "create".to_string(),
            "--force".to_string(),
            "--name".to_string(),
            rootfs_name.to_string(),
            base_image.display().to_string(),
        ],
        &format!("create prepare rootfs for service '{}'", service.name),
    )?;

    for command in &prepare.commands {
        let mut args = vec!["start".to_string()];
        if prepare.root {
            args.push("--root".to_string());
        }
        args.push("--rw".to_string());
        for mount in &prepare.mounts {
            args.push("--mount".to_string());
            args.push(mount.clone());
        }
        for (key, value) in &prepare.env {
            args.push("--env".to_string());
            args.push(format!("{key}={value}"));
        }
        args.push(rootfs_name.to_string());
        args.push("/bin/sh".to_string());
        args.push("-lc".to_string());
        args.push(command.clone());

        run_enroot(
            &options.enroot_bin,
            envs,
            args,
            &format!("run prepare command for service '{}'", service.name),
        )?;
    }

    run_enroot(
        &options.enroot_bin,
        envs,
        vec![
            "export".to_string(),
            "--force".to_string(),
            "--output".to_string(),
            service.runtime_image.display().to_string(),
            rootfs_name.to_string(),
        ],
        &format!("export prepared image for service '{}'", service.name),
    )?;

    Ok(())
}

fn remove_rootfs(enroot_bin: &str, envs: &[(String, String)], rootfs_name: &str) -> Result<()> {
    run_enroot(
        enroot_bin,
        envs,
        vec![
            "remove".to_string(),
            "--force".to_string(),
            rootfs_name.to_string(),
        ],
        "remove temporary prepare rootfs",
    )
}

fn run_enroot<I>(enroot_bin: &str, envs: &[(String, String)], args: I, context: &str) -> Result<()>
where
    I: IntoIterator<Item = String>,
{
    let args_vec = args.into_iter().collect::<Vec<_>>();
    let mut command = Command::new(enroot_bin);
    command.args(&args_vec);
    command.envs(envs.iter().map(|(k, v)| (k, v)));
    let output = command.output().context(format!(
        "failed to execute '{}' while trying to {}",
        enroot_bin, context
    ))?;
    if !output.status.success() {
        bail!(
            "failed to {}: {}",
            context,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// Returns the cache location used for a service's imported base image.
pub fn base_image_path(cache_dir: &Path, service: &RuntimeService) -> PathBuf {
    let key = base_image_cache_key(service);
    cache_dir.join("base").join(format!(
        "{}-{}.sqsh",
        short_hash(&key),
        sanitize_name(&image_label(&service.source))
    ))
}

fn runtime_image_path(plan: &Plan, service: &PlannedService) -> PathBuf {
    match (&service.image, &service.prepare) {
        (ImageSource::LocalSqsh(path), None) => path.clone(),
        (ImageSource::Remote(_), None) => {
            let runtime = RuntimeService {
                name: service.name.clone(),
                runtime_image: PathBuf::new(),
                execution: service.execution.clone(),
                environment: service.environment.clone(),
                volumes: service.volumes.clone(),
                working_dir: service.working_dir.clone(),
                depends_on: service.depends_on.clone(),
                readiness: service.readiness.clone(),
                failure_policy: service.failure_policy.clone(),
                placement: service.placement.clone(),
                slurm: service.slurm.clone(),
                prepare: service.prepare.clone(),
                source: service.image.clone(),
            };
            base_image_path(&plan.cache_dir, &runtime)
        }
        (_, Some(prepare)) => plan.cache_dir.join("prepared").join(format!(
            "{}-{}.sqsh",
            short_hash(&prepared_image_cache_key_from_plan(service, prepare)),
            sanitize_name(&service.name)
        )),
    }
}

fn prepared_image_cache_key_from_plan(
    service: &PlannedService,
    prepare: &PreparedImageSpec,
) -> String {
    let mut parts = vec![
        "prepared".to_string(),
        env!("CARGO_PKG_VERSION").to_string(),
    ];
    match &service.image {
        ImageSource::LocalSqsh(path) => parts.push(path.to_string_lossy().into_owned()),
        ImageSource::Remote(remote) => parts.push(remote.clone()),
    }
    parts.extend(prepare.commands.iter().cloned());
    parts.extend(prepare.mounts.iter().cloned());
    parts.extend(prepare.env.iter().map(|(k, v)| format!("{k}={v}")));
    parts.push(format!("root={}", prepare.root));
    cache_key(&parts.iter().map(String::as_str).collect::<Vec<_>>())
}

fn prepared_image_cache_key(service: &RuntimeService, prepare: &PreparedImageSpec) -> String {
    let mut parts = vec![
        "prepared".to_string(),
        env!("CARGO_PKG_VERSION").to_string(),
    ];
    match &service.source {
        ImageSource::LocalSqsh(path) => parts.push(path.to_string_lossy().into_owned()),
        ImageSource::Remote(remote) => parts.push(remote.clone()),
    }
    parts.extend(prepare.commands.iter().cloned());
    parts.extend(prepare.mounts.iter().cloned());
    parts.extend(prepare.env.iter().map(|(k, v)| format!("{k}={v}")));
    parts.push(format!("root={}", prepare.root));
    cache_key(&parts.iter().map(String::as_str).collect::<Vec<_>>())
}

fn base_image_cache_key(service: &RuntimeService) -> String {
    let image_key = match &service.source {
        ImageSource::LocalSqsh(path) => path.to_string_lossy().into_owned(),
        ImageSource::Remote(remote) => remote.clone(),
    };
    cache_key(&["base", image_key.as_str(), env!("CARGO_PKG_VERSION")])
}

fn temporary_rootfs_name(service: &RuntimeService) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_secs();
    format!("hpc-compose-{}-{}", sanitize_name(&service.name), ts)
}

fn cache_key(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    hex::encode(hasher.finalize())
}

fn short_hash(hash: &str) -> &str {
    &hash[..16]
}

fn sanitize_name(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn image_label(source: &ImageSource) -> String {
    match source {
        ImageSource::LocalSqsh(path) => path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("local-image")
            .to_string(),
        ImageSource::Remote(remote) => remote
            .rsplit('/')
            .next()
            .unwrap_or(remote.as_str())
            .replace(':', "-"),
    }
}

fn create_cache_dirs(plan: &RuntimePlan) -> Result<()> {
    for path in [
        plan.cache_dir.join("base"),
        plan.cache_dir.join("prepared"),
        plan.cache_dir.join("enroot/cache"),
        plan.cache_dir.join("enroot/data"),
        plan.cache_dir.join("enroot/tmp"),
    ] {
        fs::create_dir_all(&path).context(format!(
            "failed to create cache directory {}",
            path.display()
        ))?;
    }
    Ok(())
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    let parent = path.parent().context(format!(
        "path '{}' does not have a parent directory",
        path.display()
    ))?;
    fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;
    Ok(())
}

fn enroot_env(cache_dir: &Path) -> Vec<(String, String)> {
    vec![
        (
            "ENROOT_CACHE_PATH".to_string(),
            cache_dir.join("enroot/cache").display().to_string(),
        ),
        (
            "ENROOT_DATA_PATH".to_string(),
            cache_dir.join("enroot/data").display().to_string(),
        ),
    ]
}

/// Verifies that an external binary is available on the current machine.
pub fn ensure_binary_available(binary: &str, message: &str) -> Result<()> {
    if binary.contains(std::path::MAIN_SEPARATOR) {
        let path = Path::new(binary);
        if path.exists() {
            return Ok(());
        }
        bail!("{message}");
    }

    let path_var = std::env::var_os("PATH").unwrap_or_default();
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(binary);
        if candidate.exists() {
            return Ok(());
        }
    }
    bail!("{message}");
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, OnceLock};

    use super::*;
    use crate::planner::{ImageSource, PreparedImageSpec, ServicePlacement};
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
            slurm: SlurmConfig::default(),
            ordered_services: vec![fake_service(tmpdir.path())],
        };
        let options = PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
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
            slurm: SlurmConfig::default(),
            ordered_services: vec![service],
        };
        let options = PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
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
            slurm: SlurmConfig::default(),
            ordered_services: vec![service],
        };
        let options = PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
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
    fn failed_prepare_cleans_up_by_default() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("enroot.log");
        let fake = write_fake_enroot(tmpdir.path(), &log);

        let mut service = fake_service(tmpdir.path());
        service.prepare.as_mut().expect("prepare").commands = vec!["fail-me".into()];
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            slurm: SlurmConfig::default(),
            ordered_services: vec![service],
        };
        let options = PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
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
            slurm: SlurmConfig::default(),
            ordered_services: vec![service],
        };

        prepare_runtime_plan(
            &plan,
            &PrepareOptions {
                enroot_bin: fake.display().to_string(),
                keep_failed_prep: false,
                force_rebuild: false,
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
            },
        )
        .expect_err("local missing");
        assert!(err.to_string().contains("does not exist"));

        let remote_no_prepare = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache2"),
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
            slurm: SlurmConfig::default(),
            ordered_services: vec![service],
        };
        let err = prepare_runtime_plan(
            &plan,
            &PrepareOptions {
                enroot_bin: fake.display().to_string(),
                keep_failed_prep: true,
                force_rebuild: false,
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
    fn runtime_path_and_command_helpers_cover_remaining_branches() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let plan = Plan {
            name: "demo".into(),
            project_dir: tmpdir.path().to_path_buf(),
            spec_path: compose,
            cache_dir: tmpdir.path().join("cache"),
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
                plan.ordered_services[1].prepare.as_ref().expect("prepare")
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
                    .expect("prepare")
            )
            .len()
                > 10
        );
    }
}
