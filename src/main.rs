use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use hpc_compose::cache::{
    CacheEntryKind, load_manifest_if_exists, prune_all_unused, prune_by_age, scan_cache,
};
use hpc_compose::planner::{ImageSource, build_plan, registry_host_for_remote};
use hpc_compose::preflight::{Options as PreflightOptions, Report, run as run_preflight};
use hpc_compose::prepare::{
    ArtifactAction, PrepareOptions, PrepareSummary, RuntimePlan, base_image_path,
    build_runtime_plan, prepare_runtime_plan,
};
use hpc_compose::render::render_script;
use hpc_compose::spec::ComposeSpec;

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Compile a compose-like spec into a single Slurm job using Enroot"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Validate {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
    },
    Render {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    Prepare {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long, default_value = "enroot")]
        enroot_bin: String,
        #[arg(long)]
        keep_failed_prep: bool,
        #[arg(long)]
        force: bool,
    },
    Preflight {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long)]
        strict: bool,
        #[arg(long, default_value = "enroot")]
        enroot_bin: String,
        #[arg(long, default_value = "sbatch")]
        sbatch_bin: String,
        #[arg(long, default_value = "srun")]
        srun_bin: String,
    },
    Inspect {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
    },
    Submit {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long)]
        script_out: Option<PathBuf>,
        #[arg(long, default_value = "sbatch")]
        sbatch_bin: String,
        #[arg(long, default_value = "srun")]
        srun_bin: String,
        #[arg(long, default_value = "enroot")]
        enroot_bin: String,
        #[arg(long)]
        keep_failed_prep: bool,
        #[arg(long)]
        skip_prepare: bool,
        #[arg(long)]
        force_rebuild: bool,
        #[arg(long)]
        no_preflight: bool,
    },
    Cache {
        #[command(subcommand)]
        command: CacheCommands,
    },
}

#[derive(Debug, Subcommand)]
enum CacheCommands {
    List {
        #[arg(long)]
        cache_dir: Option<PathBuf>,
    },
    Inspect {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long)]
        service: Option<String>,
    },
    Prune {
        #[arg(short = 'f', long)]
        file: Option<PathBuf>,
        #[arg(long)]
        cache_dir: Option<PathBuf>,
        #[arg(long)]
        age: Option<u64>,
        #[arg(long)]
        all_unused: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    run_command(cli.command)
}

fn run_command(command: Commands) -> Result<()> {
    match command {
        Commands::Validate { file } => {
            load_runtime_plan(&file)?;
            println!("spec is valid");
        }
        Commands::Render { file, output } => {
            let script = render_from_path(&file)?;
            if let Some(output_path) = output {
                fs::write(&output_path, script).with_context(|| {
                    format!(
                        "failed to write rendered script to {}",
                        output_path.display()
                    )
                })?;
                println!("{}", output_path.display());
            } else {
                print!("{script}");
            }
        }
        Commands::Prepare {
            file,
            enroot_bin,
            keep_failed_prep,
            force,
        } => {
            let runtime_plan = load_runtime_plan(&file)?;
            let summary = prepare_runtime_plan(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin,
                    keep_failed_prep,
                    force_rebuild: force,
                },
            )?;
            print_prepare_summary(&summary);
        }
        Commands::Preflight {
            file,
            strict,
            enroot_bin,
            sbatch_bin,
            srun_bin,
        } => {
            let runtime_plan = load_runtime_plan(&file)?;
            let report = run_preflight(
                &runtime_plan,
                &PreflightOptions {
                    enroot_bin,
                    sbatch_bin,
                    srun_bin,
                    require_submit_tools: true,
                    skip_prepare: false,
                },
            );
            print_report(&report);
            if report.has_errors() {
                bail!("preflight failed");
            }
            if strict && report.has_warnings() {
                bail!("preflight reported warnings");
            }
        }
        Commands::Inspect { file } => {
            let runtime_plan = load_runtime_plan(&file)?;
            print_plan_inspect(&runtime_plan);
        }
        Commands::Submit {
            file,
            script_out,
            sbatch_bin,
            srun_bin,
            enroot_bin,
            keep_failed_prep,
            skip_prepare,
            force_rebuild,
            no_preflight,
        } => {
            let runtime_plan = load_runtime_plan(&file)?;

            if !no_preflight {
                let report = run_preflight(
                    &runtime_plan,
                    &PreflightOptions {
                        enroot_bin: enroot_bin.clone(),
                        sbatch_bin: sbatch_bin.clone(),
                        srun_bin,
                        require_submit_tools: true,
                        skip_prepare,
                    },
                );
                print_report(&report);
                if report.has_errors() {
                    bail!("preflight failed; fix the reported errors before submitting");
                }
            }

            if !skip_prepare {
                let summary = prepare_runtime_plan(
                    &runtime_plan,
                    &PrepareOptions {
                        enroot_bin,
                        keep_failed_prep,
                        force_rebuild,
                    },
                )?;
                print_prepare_summary(&summary);
            }

            let script = render_script(&runtime_plan)?;
            let script_path = script_out.unwrap_or_else(|| default_script_path(&file));
            fs::write(&script_path, script).with_context(|| {
                format!(
                    "failed to write rendered script to {}",
                    script_path.display()
                )
            })?;

            let output = Command::new(&sbatch_bin)
                .arg(&script_path)
                .output()
                .with_context(|| format!("failed to execute '{sbatch_bin}'"))?;
            if !output.status.success() {
                bail!(
                    "sbatch failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }

            let stdout = String::from_utf8_lossy(&output.stdout);
            print!("{stdout}");
            print_submit_details(&runtime_plan, &script_path, stdout.trim())?;
        }
        Commands::Cache { command } => match command {
            CacheCommands::List { cache_dir } => {
                let cache_dir = cache_dir.unwrap_or_else(default_cache_dir);
                let manifests = scan_cache(&cache_dir)?;
                if manifests.is_empty() {
                    println!("no cache entries found in {}", cache_dir.display());
                } else {
                    println!("cache dir: {}", cache_dir.display());
                    for manifest in manifests {
                        let kind = match manifest.kind {
                            CacheEntryKind::Base => "base",
                            CacheEntryKind::Prepared => "prepared",
                        };
                        println!(
                            "{kind}\t{}\tservices={}\tsource={}",
                            manifest.artifact_path,
                            manifest.service_names.join(","),
                            manifest.source_image
                        );
                    }
                }
            }
            CacheCommands::Inspect { file, service } => {
                let runtime_plan = load_runtime_plan(&file)?;
                print_cache_inspect(&runtime_plan, service.as_deref())?;
            }
            CacheCommands::Prune {
                file,
                cache_dir,
                age,
                all_unused,
            } => {
                if age.is_none() && !all_unused {
                    bail!("cache prune requires either --age DAYS or --all-unused");
                }
                if age.is_some() && all_unused {
                    bail!("cache prune accepts only one strategy at a time");
                }

                if let Some(days) = age {
                    let target = cache_dir.unwrap_or_else(default_cache_dir);
                    let result = prune_by_age(&target, days)?;
                    print_prune_result(&target, &result.removed);
                } else {
                    let file = file.context("--all-unused requires -f/--file so the current plan can define which artifacts are still referenced")?;
                    let runtime_plan = load_runtime_plan(&file)?;
                    let target = cache_dir.unwrap_or_else(|| runtime_plan.cache_dir.clone());
                    let result = prune_all_unused(&target, &runtime_plan)?;
                    print_prune_result(&target, &result.removed);
                }
            }
        },
    }
    Ok(())
}

fn render_from_path(path: &Path) -> Result<String> {
    let runtime = load_runtime_plan(path)?;
    render_script(&runtime)
}

fn load_runtime_plan(path: &Path) -> Result<RuntimePlan> {
    let spec = ComposeSpec::load(path)?;
    let plan = build_plan(path, spec)?;
    Ok(build_runtime_plan(&plan))
}

fn default_script_path(spec_path: &Path) -> PathBuf {
    let parent = spec_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join("hpc-compose.sbatch")
}

fn default_cache_dir() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".cache/hpc-compose")
}

fn print_report(report: &Report) {
    if report.items.is_empty() {
        return;
    }
    let _ = writeln!(io::stderr(), "{}", report.render());
    let _ = io::stderr().flush();
}

fn print_prepare_summary(summary: &PrepareSummary) {
    for service in &summary.services {
        if let Some(base) = &service.base_image {
            println!(
                "{} service '{}' base image {}: {}",
                action_label(base.action),
                service.service_name,
                artifact_role_label("base"),
                base.path.display()
            );
        }
        println!(
            "{} service '{}' runtime image {}: {}",
            action_label(service.runtime_image.action),
            service.service_name,
            artifact_role_label("runtime"),
            service.runtime_image.path.display()
        );
        if let Some(note) = &service.runtime_image.note {
            println!("note  service '{}': {note}", service.service_name);
        }
    }
}

fn action_label(action: ArtifactAction) -> &'static str {
    match action {
        ArtifactAction::Present => "OK",
        ArtifactAction::Reused => "REUSE",
        ArtifactAction::Built => "BUILD",
    }
}

fn artifact_role_label(name: &str) -> &'static str {
    match name {
        "base" => "cache artifact",
        "runtime" => "artifact",
        _ => "artifact",
    }
}

fn print_plan_inspect(plan: &RuntimePlan) {
    println!("name: {}", plan.name);
    println!("runtime mode: pyxis");
    println!("cache dir: {}", plan.cache_dir.display());
    println!("service order: {}", service_names(plan).join(" -> "));

    for service in &plan.ordered_services {
        println!();
        println!("service: {}", service.name);
        println!("source image: {}", source_image_display(&service.source));
        if let ImageSource::Remote(_) = &service.source {
            let base_path = base_image_path(&plan.cache_dir, service);
            println!("base cache artifact: {}", base_path.display());
            println!("base cache state: {}", hit_or_miss(base_path.exists()));
        }
        println!("runtime image: {}", service.runtime_image.display());
        println!("runtime image state: {}", runtime_cache_state(service));
        if let Some(prepare) = &service.prepare {
            println!(
                "prepare commands: {}",
                if prepare.commands.is_empty() {
                    "0".to_string()
                } else {
                    prepare.commands.len().to_string()
                }
            );
            if prepare.force_rebuild {
                println!(
                    "reuse policy: rebuild on submit because x-enroot.prepare.mounts are present"
                );
            } else {
                println!("reuse policy: reuse prepared image when the cached artifact exists");
            }
        } else if matches!(service.source, ImageSource::LocalSqsh(_)) {
            println!("reuse policy: uses local .sqsh directly");
        } else {
            println!("reuse policy: reuse imported base image when the cached artifact exists");
        }
    }
}

fn print_cache_inspect(plan: &RuntimePlan, filter: Option<&str>) -> Result<()> {
    for service in &plan.ordered_services {
        if let Some(filter_name) = filter
            && service.name != filter_name
        {
            continue;
        }

        println!("service: {}", service.name);
        println!("source image: {}", source_image_display(&service.source));

        if let ImageSource::Remote(remote) = &service.source {
            let base_path = base_image_path(&plan.cache_dir, service);
            println!("base artifact: {}", base_path.display());
            println!("base registry: {}", registry_host_for_remote(remote));
            print_manifest_block(&base_path)?;
        }

        println!("runtime artifact: {}", service.runtime_image.display());
        print_manifest_block(&service.runtime_image)?;
        println!(
            "current reuse expectation: {}",
            runtime_cache_state(service)
        );
        if let Some(prepare) = &service.prepare
            && prepare.force_rebuild
        {
            println!("note: this service rebuilds on submit because prepare.mounts are present");
        }
        println!();
    }
    Ok(())
}

fn print_manifest_block(path: &Path) -> Result<()> {
    println!("artifact present: {}", yes_no(path.exists()));
    let manifest_path = hpc_compose::cache::manifest_path_for(path);
    println!("manifest path: {}", manifest_path.display());
    if let Some(manifest) = load_manifest_if_exists(path)? {
        let kind = match manifest.kind {
            CacheEntryKind::Base => "base",
            CacheEntryKind::Prepared => "prepared",
        };
        println!("manifest kind: {kind}");
        println!("manifest cache key: {}", manifest.cache_key);
        println!("manifest source: {}", manifest.source_image);
        println!("manifest services: {}", manifest.service_names.join(","));
        println!("manifest created_at: {}", manifest.created_at);
        println!("manifest last_used_at: {}", manifest.last_used_at);
        if manifest.kind == CacheEntryKind::Prepared {
            println!("prepare root: {}", manifest.prepare_root.unwrap_or(true));
            println!(
                "prepare commands: {}",
                if manifest.prepare_commands.is_empty() {
                    "0".to_string()
                } else {
                    manifest.prepare_commands.join(" | ")
                }
            );
            println!(
                "force rebuild due to mounts: {}",
                yes_no(manifest.force_rebuild_due_to_mounts)
            );
        }
    } else {
        println!("manifest present: no");
    }
    Ok(())
}

fn runtime_cache_state(service: &hpc_compose::prepare::RuntimeService) -> &'static str {
    if let Some(prepare) = &service.prepare {
        if prepare.force_rebuild {
            "rebuild on submit"
        } else if service.runtime_image.exists() {
            "cache hit"
        } else {
            "cache miss"
        }
    } else {
        match &service.source {
            ImageSource::LocalSqsh(path) => {
                if path.exists() {
                    "local image present"
                } else {
                    "local image missing"
                }
            }
            ImageSource::Remote(_) => {
                if service.runtime_image.exists() {
                    "cache hit"
                } else {
                    "cache miss"
                }
            }
        }
    }
}

fn source_image_display(source: &ImageSource) -> String {
    match source {
        ImageSource::LocalSqsh(path) => path.display().to_string(),
        ImageSource::Remote(remote) => remote.clone(),
    }
}

fn hit_or_miss(exists: bool) -> &'static str {
    if exists { "cache hit" } else { "cache miss" }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn service_names(plan: &RuntimePlan) -> Vec<&str> {
    plan.ordered_services
        .iter()
        .map(|service| service.name.as_str())
        .collect()
}

fn print_submit_details(plan: &RuntimePlan, script_path: &Path, sbatch_stdout: &str) -> Result<()> {
    println!("rendered script: {}", script_path.display());
    println!("cache dir: {}", plan.cache_dir.display());

    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    if let Some(job_id) = extract_job_id(sbatch_stdout) {
        for service in &plan.ordered_services {
            println!(
                "log  service '{}': {}",
                service.name,
                submit_dir
                    .join(".hpc-compose")
                    .join(job_id)
                    .join("logs")
                    .join(format!("{}.log", sanitize_service_name(&service.name)))
                    .display()
            );
        }
    } else {
        for service in &plan.ordered_services {
            println!(
                "log  service '{}': {}/.hpc-compose/<job-id>/logs/{}.log",
                service.name,
                submit_dir.display(),
                sanitize_service_name(&service.name)
            );
        }
    }
    Ok(())
}

fn extract_job_id(text: &str) -> Option<&str> {
    text.split_whitespace()
        .rev()
        .find(|token| token.chars().all(|ch| ch.is_ascii_digit()))
}

fn print_prune_result(cache_dir: &Path, removed: &[PathBuf]) {
    println!("cache dir: {}", cache_dir.display());
    if removed.is_empty() {
        println!("removed: 0");
        return;
    }
    println!("removed: {}", removed.len());
    for path in removed {
        println!("pruned: {}", path.display());
    }
}

fn sanitize_service_name(value: &str) -> String {
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    use super::*;
    use hpc_compose::cache::{CacheEntryKind, CacheEntryManifest};
    use hpc_compose::planner::{ExecutionSpec, ImageSource, PreparedImageSpec};
    use hpc_compose::spec::{ServiceSlurmConfig, SlurmConfig};

    fn runtime_service(
        source: ImageSource,
        runtime_image: PathBuf,
        prepare: Option<PreparedImageSpec>,
    ) -> hpc_compose::prepare::RuntimeService {
        hpc_compose::prepare::RuntimeService {
            name: "svc/name".into(),
            runtime_image,
            execution: ExecutionSpec::Shell("echo hi".into()),
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            readiness: None,
            slurm: ServiceSlurmConfig::default(),
            prepare,
            source,
        }
    }

    fn write_script(path: &Path, body: &str) {
        fs::write(path, body).expect("write script");
        let mut perms = fs::metadata(path).expect("meta").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("chmod");
    }

    fn write_fake_enroot(tmpdir: &Path) -> PathBuf {
        let path = tmpdir.join("fake-enroot.sh");
        write_script(
            &path,
            r#"#!/bin/bash
set -euo pipefail
cmd="${1:-}"
shift || true
case "$cmd" in
  import)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output) output="$2"; shift 2 ;;
        *) shift ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  create)
    name=""
    while (($#)); do
      case "$1" in
        -n|--name) name="$2"; shift 2 ;;
        -f|--force) shift ;;
        *) shift ;;
      esac
    done
    mkdir -p "$ENROOT_DATA_PATH/$name"
    ;;
  start) exit 0 ;;
  export)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output) output="$2"; shift 2 ;;
        -f|--force) shift ;;
        *) shift ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  remove) exit 0 ;;
esac
"#,
        );
        path
    }

    fn write_fake_sbatch(tmpdir: &Path, success: bool) -> PathBuf {
        let path = tmpdir.join(if success { "sbatch-ok" } else { "sbatch-fail" });
        let body = if success {
            "#!/bin/bash\nset -euo pipefail\necho 'Submitted batch job 54321'\n"
        } else {
            "#!/bin/bash\nset -euo pipefail\necho 'boom' >&2\nexit 2\n"
        };
        write_script(&path, body);
        path
    }

    fn write_fake_srun(tmpdir: &Path) -> PathBuf {
        let path = tmpdir.join("srun");
        write_script(
            &path,
            "#!/bin/bash\nset -euo pipefail\nif [[ \"${1:-}\" == \"--help\" ]]; then echo 'usage --container-image'; fi\n",
        );
        path
    }

    fn write_compose(tmpdir: &Path, body: &str) -> PathBuf {
        let path = tmpdir.join("compose.yaml");
        fs::write(&path, body).expect("compose");
        path
    }

    fn safe_cache_dir() -> tempfile::TempDir {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tmp/hpc-compose-tests");
        fs::create_dir_all(&root).expect("cache root");
        tempfile::Builder::new()
            .prefix("case-")
            .tempdir_in(root)
            .expect("cache tempdir")
    }

    fn write_valid_compose(tmpdir: &Path, cache_dir: &Path) -> PathBuf {
        fs::create_dir_all(tmpdir.join("app")).expect("app");
        fs::write(tmpdir.join("app/main.py"), "print('hi')\n").expect("main.py");
        write_compose(
            tmpdir,
            &format!(
                r#"
name: demo
x-slurm:
  cache_dir: {}
services:
  app:
    image: python:3.11-slim
    working_dir: /workspace
    volumes:
      - ./app:/workspace
    command:
      - python
      - -m
      - main
    x-enroot:
      prepare:
        commands:
          - pip install click
"#,
                cache_dir.display()
            ),
        )
    }

    #[test]
    fn action_and_label_helpers_cover_all_variants() {
        assert_eq!(action_label(ArtifactAction::Present), "OK");
        assert_eq!(action_label(ArtifactAction::Reused), "REUSE");
        assert_eq!(action_label(ArtifactAction::Built), "BUILD");
        assert_eq!(artifact_role_label("base"), "cache artifact");
        assert_eq!(artifact_role_label("runtime"), "artifact");
        assert_eq!(artifact_role_label("other"), "artifact");
        assert_eq!(hit_or_miss(true), "cache hit");
        assert_eq!(hit_or_miss(false), "cache miss");
        assert_eq!(yes_no(true), "yes");
        assert_eq!(yes_no(false), "no");
    }

    #[test]
    fn sanitize_and_extract_job_id_work() {
        assert_eq!(sanitize_service_name("svc/name.with spaces"), "svc_name_with_spaces");
        assert_eq!(
            extract_job_id("Submitted batch job 12345"),
            Some("12345")
        );
        assert_eq!(extract_job_id("no job id here"), None);
    }

    #[test]
    fn runtime_cache_state_covers_prepare_and_local_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let local_sqsh = tmpdir.path().join("local.sqsh");
        let remote_sqsh = tmpdir.path().join("remote.sqsh");
        std::fs::write(&local_sqsh, "x").expect("local");
        std::fs::write(&remote_sqsh, "x").expect("remote");

        let with_forced_prepare = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            remote_sqsh.clone(),
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: vec!["/host:/mnt".into()],
                env: Vec::new(),
                root: true,
                force_rebuild: true,
            }),
        );
        assert_eq!(runtime_cache_state(&with_forced_prepare), "rebuild on submit");

        let with_cached_prepare = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            remote_sqsh.clone(),
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: Vec::new(),
                env: Vec::new(),
                root: true,
                force_rebuild: false,
            }),
        );
        assert_eq!(runtime_cache_state(&with_cached_prepare), "cache hit");

        let missing_prepare = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            tmpdir.path().join("prepared-missing.sqsh"),
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: Vec::new(),
                env: Vec::new(),
                root: true,
                force_rebuild: false,
            }),
        );
        assert_eq!(runtime_cache_state(&missing_prepare), "cache miss");

        let local_present =
            runtime_service(ImageSource::LocalSqsh(local_sqsh.clone()), local_sqsh.clone(), None);
        assert_eq!(runtime_cache_state(&local_present), "local image present");

        let local_missing = runtime_service(
            ImageSource::LocalSqsh(tmpdir.path().join("missing.sqsh")),
            tmpdir.path().join("missing.sqsh"),
            None,
        );
        assert_eq!(runtime_cache_state(&local_missing), "local image missing");

        let remote_missing = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            tmpdir.path().join("missing-remote.sqsh"),
            None,
        );
        assert_eq!(runtime_cache_state(&remote_missing), "cache miss");
    }

    #[test]
    fn service_names_collect_in_order() {
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: PathBuf::from("/cache"),
            slurm: SlurmConfig::default(),
            ordered_services: vec![
                runtime_service(
                    ImageSource::Remote("docker://redis:7".into()),
                    PathBuf::from("/cache/a.sqsh"),
                    None,
                ),
                hpc_compose::prepare::RuntimeService {
                    name: "worker".into(),
                    ..runtime_service(
                        ImageSource::Remote("docker://python:3.11-slim".into()),
                        PathBuf::from("/cache/b.sqsh"),
                        None,
                    )
                },
            ],
        };
        assert_eq!(service_names(&plan), vec!["svc/name", "worker"]);
    }

    #[test]
    fn path_helpers_return_expected_locations() {
        let path = PathBuf::from("/tmp/project/compose.yaml");
        assert_eq!(
            default_script_path(&path),
            PathBuf::from("/tmp/project/hpc-compose.sbatch")
        );
        assert!(default_cache_dir().ends_with(".cache/hpc-compose"));
        assert_eq!(
            render_from_path(Path::new("/definitely/missing/compose.yaml"))
                .expect_err("missing")
                .to_string(),
            "failed to read spec at /definitely/missing/compose.yaml"
        );
    }

    #[test]
    fn print_helpers_cover_manifest_and_summary_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let runtime_image = tmpdir.path().join("prepared.sqsh");
        std::fs::write(&runtime_image, "x").expect("runtime");
        let local_sqsh = tmpdir.path().join("local.sqsh");
        std::fs::write(&local_sqsh, "x").expect("local");
        let manifest = CacheEntryManifest {
            kind: CacheEntryKind::Prepared,
            artifact_path: runtime_image.display().to_string(),
            service_names: vec!["svc/name".into()],
            cache_key: "key".into(),
            source_image: "docker://redis:7".into(),
            registry: Some("registry-1.docker.io".into()),
            prepare_commands: Vec::new(),
            prepare_env: Vec::new(),
            prepare_root: Some(true),
            prepare_mounts: Vec::new(),
            force_rebuild_due_to_mounts: false,
            created_at: 1,
            last_used_at: 1,
            tool_version: "0.1.0".into(),
        };
        let manifest_path = hpc_compose::cache::manifest_path_for(&runtime_image);
        std::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).expect("manifest"),
        )
        .expect("write manifest");

        let service = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            runtime_image.clone(),
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: vec!["/host:/mnt".into()],
                env: Vec::new(),
                root: true,
                force_rebuild: true,
            }),
        );
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            slurm: SlurmConfig::default(),
            ordered_services: vec![service.clone()],
        };
        let local_plan = RuntimePlan {
            name: "local-demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            slurm: SlurmConfig::default(),
            ordered_services: vec![runtime_service(
                ImageSource::LocalSqsh(local_sqsh.clone()),
                local_sqsh,
                None,
            )],
        };

        print_report(&Report { items: Vec::new() });
        print_report(&Report {
            items: vec![hpc_compose::preflight::Item {
                level: hpc_compose::preflight::Level::Warn,
                message: "warn".into(),
                remediation: None,
            }],
        });
        print_prepare_summary(&PrepareSummary {
            services: vec![hpc_compose::prepare::ServicePrepareResult {
                service_name: service.name.clone(),
                base_image: Some(hpc_compose::prepare::ArtifactStatus {
                    path: tmpdir.path().join("base.sqsh"),
                    action: ArtifactAction::Built,
                    note: None,
                }),
                runtime_image: hpc_compose::prepare::ArtifactStatus {
                    path: runtime_image.clone(),
                    action: ArtifactAction::Reused,
                    note: Some("cached".into()),
                },
            }],
        });
        print_plan_inspect(&plan);
        print_plan_inspect(&local_plan);
        print_cache_inspect(&plan, None).expect("inspect");
        print_cache_inspect(&plan, Some("other")).expect("inspect filtered");
        print_manifest_block(&runtime_image).expect("manifest block");
        print_manifest_block(&tmpdir.path().join("missing.sqsh")).expect("missing manifest block");
        print_prune_result(tmpdir.path(), &[]);
        print_prune_result(tmpdir.path(), &[runtime_image.clone()]);
        print_submit_details(&plan, Path::new("/tmp/job.sbatch"), "no job id").expect("submit details");
        print_submit_details(&plan, Path::new("/tmp/job.sbatch"), "Submitted batch job 99999")
            .expect("submit details with job id");
        assert_eq!(source_image_display(&service.source), "docker://redis:7");
        assert_eq!(
            source_image_display(&ImageSource::LocalSqsh(PathBuf::from("/tmp/local.sqsh"))),
            "/tmp/local.sqsh"
        );
    }

    #[test]
    fn run_command_covers_success_and_error_arms() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let cache_root = safe_cache_dir();
        let cache_dir = cache_root.path().to_path_buf();
        let compose = write_valid_compose(tmpdir.path(), &cache_dir);
        let enroot = write_fake_enroot(tmpdir.path());
        let srun = write_fake_srun(tmpdir.path());
        let sbatch_ok = write_fake_sbatch(tmpdir.path(), true);
        let sbatch_fail = write_fake_sbatch(tmpdir.path(), false);
        let empty_cache = tmpdir.path().join("empty-cache");
        fs::create_dir_all(&empty_cache).expect("empty cache");

        run_command(Commands::Validate {
            file: compose.clone(),
        })
        .expect("validate");
        run_command(Commands::Render {
            file: compose.clone(),
            output: None,
        })
        .expect("render stdout");
        let rendered = tmpdir.path().join("rendered.sbatch");
        run_command(Commands::Render {
            file: compose.clone(),
            output: Some(rendered.clone()),
        })
        .expect("render file");
        assert!(rendered.exists());

        run_command(Commands::Prepare {
            file: compose.clone(),
            enroot_bin: enroot.display().to_string(),
            keep_failed_prep: false,
            force: true,
        })
        .expect("prepare");

        let err = run_command(Commands::Preflight {
            file: compose.clone(),
            strict: true,
            enroot_bin: enroot.display().to_string(),
            sbatch_bin: sbatch_ok.display().to_string(),
            srun_bin: srun.display().to_string(),
        })
        .expect_err("strict warnings");
        assert!(err.to_string().contains("preflight reported warnings"));
        run_command(Commands::Preflight {
            file: compose.clone(),
            strict: false,
            enroot_bin: enroot.display().to_string(),
            sbatch_bin: sbatch_ok.display().to_string(),
            srun_bin: srun.display().to_string(),
        })
        .expect("non-strict preflight");

        run_command(Commands::Inspect {
            file: compose.clone(),
        })
        .expect("inspect");

        let err = run_command(Commands::Submit {
            file: compose.clone(),
            script_out: None,
            sbatch_bin: sbatch_fail.display().to_string(),
            srun_bin: srun.display().to_string(),
            enroot_bin: enroot.display().to_string(),
            keep_failed_prep: false,
            skip_prepare: true,
            force_rebuild: false,
            no_preflight: true,
        })
        .expect_err("sbatch fail");
        assert!(err.to_string().contains("sbatch failed"));

        run_command(Commands::Submit {
            file: compose.clone(),
            script_out: Some(tmpdir.path().join("submit.sbatch")),
            sbatch_bin: sbatch_ok.display().to_string(),
            srun_bin: srun.display().to_string(),
            enroot_bin: enroot.display().to_string(),
            keep_failed_prep: false,
            skip_prepare: true,
            force_rebuild: false,
            no_preflight: false,
        })
        .expect("submit");

        run_command(Commands::Cache {
            command: CacheCommands::List {
                cache_dir: Some(cache_dir.clone()),
            },
        })
        .expect("cache list");
        run_command(Commands::Cache {
            command: CacheCommands::List {
                cache_dir: Some(empty_cache),
            },
        })
        .expect("cache list empty");
        run_command(Commands::Cache {
            command: CacheCommands::Inspect {
                file: compose.clone(),
                service: Some("app".into()),
            },
        })
        .expect("cache inspect");
        let err = run_command(Commands::Cache {
            command: CacheCommands::Prune {
                file: None,
                cache_dir: Some(cache_dir.clone()),
                age: None,
                all_unused: true,
            },
        })
        .expect_err("missing file");
        assert!(err.to_string().contains("--all-unused requires -f/--file"));
        let err = run_command(Commands::Cache {
            command: CacheCommands::Prune {
                file: Some(compose),
                cache_dir: Some(cache_dir.clone()),
                age: Some(7),
                all_unused: true,
            },
        })
        .expect_err("conflicting strategies");
        assert!(err
            .to_string()
            .contains("cache prune accepts only one strategy at a time"));
        run_command(Commands::Cache {
            command: CacheCommands::Prune {
                file: None,
                cache_dir: Some(cache_dir),
                age: Some(999),
                all_unused: false,
            },
        })
        .expect("prune age");
    }
}
