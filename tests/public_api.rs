use hpc_compose::cache::dataset::{
    HF_COMPLETE_MARKER, HF_URI_SCHEME, HfArtifactRef, STAGED_COMPLETE_MARKER, StagedInputAction,
    StagedInputKind, StagedInputProof, StagedInputSpec, dataset_cache_key, ensure_staged_input,
    parse_hf_uri, render_hf_stage_command, staged_input_dir, validate_hf_repo,
    validate_hf_revision,
};
use hpc_compose::cache::{CacheEntryKind, cache_key_for_service, parse_remote_registry};
use hpc_compose::context::{
    BinaryOverrides, ResolveRequest, Settings, SettingsProfile, ValueSource,
    discover_settings_path, load_settings, load_settings_if_exists, repo_adjacent_settings_path,
    repo_root_or_cwd, resolve, write_settings,
};
use hpc_compose::job::{
    CleanupMode, CleanupReport, DeepCleanupDetails, MAX_NOTE_LEN, MAX_TAG_LEN, MAX_TAGS_PER_RECORD,
    apply_tag_changes, build_cleanup_report, build_deep_cleanup_report, run_cleanup_report,
    run_deep_cleanup_report, validate_note_text, validate_tag,
};
use hpc_compose::manpages::{check_manpages, render_manpages, write_manpages};
use hpc_compose::planner::{
    ExecutionSpec, ImageSource, PreparedImageSpec, ServicePlacement, build_plan,
    cache_path_policy_issue, registry_host_for_remote, runtime_root_policy_issue,
};
use hpc_compose::render::{log_file_name_for_service, render_script};
use hpc_compose::rendezvous::{env_token as rendezvous_env_token, validate_name};
use hpc_compose::runtime_plan::{RuntimeService, build_runtime_plan};
use hpc_compose::spec::{ComposeSpec, ServiceFailurePolicy, ServiceSlurmConfig};
use schemars::JsonSchema;
use std::any::{type_name, type_name_of_val};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn public_context_and_manpage_apis_work_from_integration_tests() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let repo_root = tmpdir.path().join("repo");
    let nested = repo_root.join("nested/work");
    fs::create_dir_all(repo_root.join(".git")).expect("git dir");
    fs::create_dir_all(&nested).expect("nested dir");

    let compose = repo_root.join("compose.yaml");
    fs::write(
        &compose,
        format!(
            "name: public-api\nservices:\n  app:\n    image: redis:7\nx-slurm:\n  cache_dir: {}\n",
            repo_root.join("cache").display()
        ),
    )
    .expect("compose");

    let settings_path = repo_adjacent_settings_path(&nested);
    let mut settings = Settings {
        default_profile: Some("dev".to_string()),
        defaults: hpc_compose::context::SettingsDefaults {
            compose_file: Some("compose.yaml".to_string()),
            env: BTreeMap::from([("CACHE_HINT".to_string(), "shared".to_string())]),
            binaries: BinaryOverrides {
                srun: Some("/opt/slurm/bin/srun".to_string()),
                ..BinaryOverrides::default()
            },
            ..Default::default()
        },
        ..Settings::default()
    };
    settings
        .profiles
        .insert("dev".to_string(), SettingsProfile::default());
    write_settings(&settings_path, &settings).expect("write settings");

    assert_eq!(discover_settings_path(&nested), Some(settings_path.clone()));
    assert_eq!(repo_root_or_cwd(&nested), repo_root);
    assert_eq!(
        std::any::type_name_of_val(&repo_root_or_cwd),
        "hpc_compose::context::repo_root_or_cwd"
    );
    assert!(
        load_settings_if_exists(&settings_path)
            .expect("optional")
            .is_some()
    );
    let loaded = load_settings(&settings_path).expect("load settings");
    assert_eq!(loaded.default_profile.as_deref(), Some("dev"));

    let original_cwd = env::current_dir().expect("cwd");
    env::set_current_dir(&nested).expect("set cwd");
    let request = ResolveRequest::from_current_dir().expect("request");
    env::set_current_dir(original_cwd).expect("restore cwd");
    assert_eq!(
        fs::canonicalize(&request.cwd).expect("request cwd"),
        fs::canonicalize(&nested).expect("nested cwd")
    );

    let resolved = resolve(&ResolveRequest {
        cwd: nested.clone(),
        profile: None,
        settings_file: Some(settings_path.clone()),
        compose_file_override: None,
        binary_overrides: BinaryOverrides::default(),
        huggingface_cli_bin: None,
    })
    .expect("resolve");
    assert_eq!(resolved.compose_file.value, compose);
    assert_eq!(resolved.compose_file.source, ValueSource::Defaults);
    assert_eq!(
        resolved.interpolation_vars.get("CACHE_HINT"),
        Some(&"shared".to_string())
    );

    let pages = render_manpages();
    assert!(!pages.is_empty());
    let man_dir = tmpdir.path().join("man/man1");
    write_manpages(&man_dir).expect("write manpages");
    check_manpages(&man_dir).expect("check manpages");
    assert!(man_dir.join("hpc-compose.1").exists());
}

#[test]
fn public_job_annotation_policy_paths_remain_compatible() {
    let _: fn(&str) -> anyhow::Result<()> = validate_tag;
    let _: fn(&str) -> anyhow::Result<String> = validate_note_text;

    assert_eq!(MAX_TAGS_PER_RECORD, 32);
    assert_eq!(MAX_TAG_LEN, 64);
    assert_eq!(MAX_NOTE_LEN, 4096);
    assert_eq!(
        validate_tag("").expect_err("empty tag").to_string(),
        "tag must not be empty"
    );
    assert_eq!(
        validate_tag("bad tag")
            .expect_err("unsupported tag")
            .to_string(),
        "tag 'bad tag' contains unsupported characters; use only letters, digits, '.', '_', and '-'"
    );

    let mut tags = vec!["zeta".to_string()];
    apply_tag_changes(&mut tags, &["alpha".to_string()], &[]).expect("tag change");
    assert_eq!(tags, vec!["alpha".to_string(), "zeta".to_string()]);
    assert_eq!(
        validate_note_text("  stable loss\n").expect("normalized note"),
        "stable loss"
    );
    assert_eq!(
        validate_note_text(" \n\t")
            .expect_err("empty note")
            .to_string(),
        "note text must not be empty"
    );
}

#[test]
fn public_resolved_context_secret_values_signature_remains_compatible() {
    let _: fn(&hpc_compose::context::ResolvedContext) -> std::collections::BTreeSet<String> =
        hpc_compose::context::ResolvedContext::secret_values;
}

#[test]
fn public_rendezvous_vocabulary_paths_remain_compatible() {
    let _: fn(&str) -> anyhow::Result<()> = validate_name;
    let _: fn(&str) -> String = rendezvous_env_token;

    validate_name("model-server_1.0").expect("valid rendezvous name");
    for value in [".", ".."] {
        assert_eq!(
            validate_name(value)
                .expect_err("reserved path component")
                .to_string(),
            "rendezvous name must not be '.' or '..'"
        );
    }
    assert_eq!(
        validate_name(" \t").expect_err("empty name").to_string(),
        "rendezvous name must not be empty"
    );
    assert_eq!(
        validate_name("bad/name")
            .expect_err("unsupported name")
            .to_string(),
        "rendezvous name must contain only ASCII letters, digits, '.', '_', or '-'"
    );

    assert_eq!(rendezvous_env_token(""), "_");
    assert_eq!(rendezvous_env_token("é"), "__");
    assert_eq!(rendezvous_env_token("🙂"), "____");
    assert_eq!(rendezvous_env_token("a-b"), rendezvous_env_token("a.b"));
    assert_eq!(rendezvous_env_token("a.b"), rendezvous_env_token("a_b"));
}

#[test]
fn public_huggingface_dataset_api_paths_remain_compatible() {
    assert_eq!(HF_URI_SCHEME, "hf://");
    assert_eq!(HF_COMPLETE_MARKER, ".hpc-compose-hf-complete");
    assert_eq!(STAGED_COMPLETE_MARKER, ".hpc-compose-staged-complete.json");

    validate_hf_repo("org/model").expect("valid repo");
    validate_hf_revision("abc1234").expect("valid revision");
    assert!(validate_hf_repo("org/too/many").is_err());
    assert!(validate_hf_revision("main").is_err());

    let parsed: HfArtifactRef = parse_hf_uri("hf://org/model@abc1234", StagedInputKind::Model)
        .expect("parse public hf reference");
    assert_eq!(parsed.repo, "org/model");
    assert_eq!(parsed.revision, "abc1234");
    assert_eq!(parsed.kind, StagedInputKind::Model);

    let spec: StagedInputSpec = parsed.staged_input_spec();
    assert_eq!(spec.uri, "hf://org/model");
    assert_eq!(spec.revision.as_deref(), Some("abc1234"));
    assert_eq!(spec.kind.as_dir_segment(), "models");
    let key = dataset_cache_key(&spec);
    assert_eq!(key.len(), 16);
    assert_eq!(
        staged_input_dir(PathBuf::from("/shared/cache").as_path(), spec.kind, &key),
        PathBuf::from("/shared/cache/models").join(&key)
    );

    let proof = StagedInputProof {
        content_digest: Some("sha256:abc".into()),
    };
    assert_eq!(proof.content_digest.as_deref(), Some("sha256:abc"));
    assert_eq!(StagedInputAction::Built, StagedInputAction::Built);
    assert_ne!(StagedInputAction::Built, StagedInputAction::Reused);

    let cache = tempfile::tempdir().expect("public staged-input cache");
    let (staged_dir, action) = ensure_staged_input(cache.path(), &spec, |dest| {
        fs::write(dest.join("config.json"), b"{}").expect("public materializer payload");
        Ok(StagedInputProof {
            content_digest: Some("sha256:public".into()),
        })
    })
    .expect("public ensure_staged_input");
    assert_eq!(action, StagedInputAction::Built);
    assert!(staged_dir.join("config.json").is_file());

    let (reused_dir, reused_action) = ensure_staged_input(cache.path(), &spec, |_dest| {
        panic!("public ensure_staged_input must not rematerialize a completed entry")
    })
    .expect("public ensure_staged_input reuse");
    assert_eq!(reused_dir, staged_dir);
    assert_eq!(reused_action, StagedInputAction::Reused);

    let command =
        render_hf_stage_command(&parsed, "/shared/cache/models/key", "/opt/huggingface-cli");
    assert!(command.starts_with(
        "echo 'Staging in HuggingFace model' 'org/model'@'abc1234' '->' '/shared/cache/models/key'\n"
    ));
    assert!(command.contains(
        "'/opt/huggingface-cli' download 'org/model' --revision 'abc1234' --local-dir \"$hf_tmp\""
    ));
}

#[test]
fn public_plan_runtime_plan_and_render_paths_remain_callable() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose_path = tmpdir.path().join("compose.yaml");
    fs::write(
        &compose_path,
        format!(
            "name: public-pipeline\nservices:\n  app:\n    image: redis:7\n    command: [redis-server]\nx-slurm:\n  cache_dir: {}\n",
            tmpdir.path().join("cache").display()
        ),
    )
    .expect("write compose");

    let spec = ComposeSpec::load(&compose_path).expect("load spec");
    let plan = build_plan(&compose_path, spec).expect("build plan");
    let runtime_plan = build_runtime_plan(&plan);
    let script = render_script(&runtime_plan).expect("render script");

    assert_eq!(runtime_plan.name, "public-pipeline");
    assert_eq!(runtime_plan.ordered_services.len(), 1);
    assert!(
        runtime_plan.ordered_services[0]
            .runtime_image
            .starts_with(tmpdir.path().join("cache/base"))
    );
    assert!(script.starts_with("#!/bin/bash\n"));
    assert!(script.contains("#SBATCH --job-name=public-pipeline"));
    assert!(script.contains("redis-server"));
    assert_eq!(
        log_file_name_for_service("api.worker-1"),
        "api_x2e_worker_x2d_1.log"
    );
}

#[test]
fn public_registry_and_path_policy_helpers_remain_compatible() {
    assert_eq!(
        registry_host_for_remote("docker://redis:7"),
        "registry-1.docker.io"
    );
    assert_eq!(
        registry_host_for_remote("docker://myhost:5000/app:latest"),
        "myhost:5000"
    );
    assert_eq!(
        parse_remote_registry(&ImageSource::Remote(
            "docker://registry.example#team/app:1".into()
        )),
        Some("registry.example".into())
    );
    assert_eq!(
        parse_remote_registry(&ImageSource::LocalSqsh(PathBuf::from("/images/app.sqsh"))),
        None
    );
    assert_eq!(
        cache_path_policy_issue(Path::new("/tmp/hpc-compose")),
        Some(
            "x-slurm.cache_dir resolves to '/tmp/hpc-compose', which is typically node-local and not shared; choose a shared filesystem path instead"
                .to_string()
        )
    );
    assert_eq!(
        runtime_root_policy_issue(Path::new("/tmp/runs")),
        Some(
            "x-slurm.runtime_root resolves to '/tmp/runs', which is typically node-local and not shared; choose a shared filesystem path so per-job logs and state stay visible from compute nodes"
                .to_string()
        )
    );
}

#[test]
fn legacy_cache_key_for_service_retains_exact_descriptive_bytes() {
    let service = RuntimeService {
        name: "legacy-key".into(),
        runtime_image: PathBuf::from("/shared/cache/prepared/legacy-key.sqsh"),
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
            commands: vec!["echo first".into(), "echo second".into()],
            mounts: vec!["/host input:/container input:ro".into()],
            env: vec![
                ("ALPHA".into(), "one".into()),
                ("BETA".into(), "two".into()),
            ],
            root: true,
            force_rebuild: true,
        }),
        source: ImageSource::Remote("docker://registry.example/acme/app:1".into()),
    };

    // This public compatibility helper returns a legacy descriptive string,
    // not the hashed key persisted by current prepare/runtime-plan code.
    assert_eq!(
        cache_key_for_service(&service, CacheEntryKind::Base),
        format!(
            "base:docker://registry.example/acme/app:1:{}",
            env!("CARGO_PKG_VERSION")
        )
    );
    assert_eq!(
        cache_key_for_service(&service, CacheEntryKind::Prepared),
        format!(
            "prepared|{}|docker://registry.example/acme/app:1|echo first|echo second|/host input:/container input:ro|ALPHA=one|BETA=two|root=true",
            env!("CARGO_PKG_VERSION")
        )
    );

    let mut without_prepare = service.clone();
    without_prepare.prepare = None;
    assert_eq!(
        cache_key_for_service(&without_prepare, CacheEntryKind::Prepared),
        ""
    );
    for kind in [
        CacheEntryKind::Dataset,
        CacheEntryKind::Model,
        CacheEntryKind::Source,
        CacheEntryKind::Unknown,
    ] {
        assert_eq!(cache_key_for_service(&service, kind), "");
    }
}

#[test]
fn public_cleanup_api_retains_type_schema_and_function_provenance() {
    let _: fn(&Path, CleanupMode, bool, bool) -> anyhow::Result<CleanupReport> =
        build_cleanup_report;
    let _: fn(&CleanupReport) -> anyhow::Result<()> = run_cleanup_report;
    let _: fn(&Path, &Path, CleanupMode, bool, bool) -> anyhow::Result<CleanupReport> =
        build_deep_cleanup_report;
    let _: fn(&CleanupReport) -> anyhow::Result<()> = run_deep_cleanup_report;

    assert_eq!(
        type_name::<CleanupReport>(),
        "hpc_compose::job::record::CleanupReport"
    );
    assert_eq!(
        CleanupReport::schema_id(),
        "hpc_compose::job::record::CleanupReport"
    );
    assert_eq!(
        type_name::<DeepCleanupDetails>(),
        "hpc_compose::job::deep_clean::DeepCleanupDetails"
    );
    assert_eq!(
        DeepCleanupDetails::schema_id(),
        "hpc_compose::job::deep_clean::DeepCleanupDetails"
    );
    assert_eq!(
        type_name_of_val(&build_cleanup_report),
        "hpc_compose::job::record::build_cleanup_report"
    );
    assert_eq!(
        type_name_of_val(&build_deep_cleanup_report),
        "hpc_compose::job::deep_clean::build_deep_cleanup_report"
    );
}
