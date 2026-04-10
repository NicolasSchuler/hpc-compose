use hpc_compose::context::{
    BinaryOverrides, ResolveRequest, Settings, SettingsProfile, ValueSource,
    discover_settings_path, load_settings, load_settings_if_exists, repo_adjacent_settings_path,
    repo_root_or_cwd, resolve, write_settings,
};
use hpc_compose::manpages::{check_manpages, render_manpages, write_manpages};
use std::collections::BTreeMap;
use std::env;
use std::fs;

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
