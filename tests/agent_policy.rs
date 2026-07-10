use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::process::Command;

use clap::Command as ClapCommand;
use hpc_compose::cli::build_cli_command;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Policy {
    schema_version: u32,
    effect_tags: BTreeMap<String, String>,
    authorization_tiers: BTreeMap<String, Tier>,
    #[serde(default)]
    global_overrides: Vec<GlobalOverride>,
    commands: Vec<CommandPolicy>,
}

#[derive(Debug, Deserialize)]
struct Tier {
    order: u8,
}

#[derive(Debug, Deserialize)]
struct CommandPolicy {
    path: String,
    authorization_tier: String,
    effects: Vec<String>,
    #[serde(default)]
    overrides: Vec<Override>,
}

#[derive(Debug, Deserialize)]
struct Override {
    flags: Vec<String>,
    authorization_tier: Option<String>,
    #[serde(default)]
    add_effects: Vec<String>,
    #[serde(default)]
    remove_effects: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct GlobalOverride {
    flag: String,
    #[serde(default)]
    add_effects: Vec<String>,
    #[serde(default)]
    remove_effects: Vec<String>,
}

#[derive(Debug, Clone)]
struct Decision {
    tier: String,
    effects: BTreeSet<String>,
}

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn load_policy() -> Policy {
    toml::from_str(
        &fs::read_to_string(repo_root().join("agent-command-policy.toml"))
            .expect("read agent-command-policy.toml"),
    )
    .expect("parse agent-command-policy.toml")
}

fn build_cli_command_for_test() -> ClapCommand {
    std::thread::Builder::new()
        .name("build-agent-policy-cli".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(build_cli_command)
        .expect("spawn CLI command builder")
        .join()
        .expect("CLI command builder should not panic")
}

fn collect_public_command_paths(command: &ClapCommand, prefix: &[String], paths: &mut Vec<String>) {
    for subcommand in command.get_subcommands() {
        if subcommand.is_hide_set() {
            continue;
        }
        let mut path = prefix.to_vec();
        path.push(subcommand.get_name().to_string());
        paths.push(path.join(" "));
        collect_public_command_paths(subcommand, &path, paths);
    }
}

fn command_at_path<'a>(root: &'a ClapCommand, path: &str) -> &'a ClapCommand {
    path.split_whitespace().fold(root, |command, part| {
        command
            .get_subcommands()
            .find(|subcommand| subcommand.get_name() == part)
            .unwrap_or_else(|| panic!("missing Clap path {path:?} at component {part:?}"))
    })
}

fn long_flags(command: &ClapCommand) -> BTreeSet<String> {
    command
        .get_arguments()
        .filter(|argument| !argument.is_hide_set())
        .filter_map(|argument| argument.get_long().map(|flag| format!("--{flag}")))
        .collect()
}

fn policy_entry<'a>(policy: &'a Policy, path: &str) -> &'a CommandPolicy {
    policy
        .commands
        .iter()
        .find(|entry| entry.path == path)
        .unwrap_or_else(|| panic!("policy missing command path {path:?}"))
}

fn decide(policy: &Policy, path: &str, flags: &[&str]) -> Decision {
    let command = policy_entry(policy, path);
    let present = flags.iter().copied().collect::<BTreeSet<_>>();
    let mut decision = Decision {
        tier: command.authorization_tier.clone(),
        effects: command.effects.iter().cloned().collect(),
    };
    for override_ in &command.overrides {
        if override_
            .flags
            .iter()
            .all(|flag| present.contains(flag.as_str()))
        {
            if let Some(tier) = &override_.authorization_tier {
                decision.tier.clone_from(tier);
            }
            for effect in &override_.remove_effects {
                decision.effects.remove(effect);
            }
            decision
                .effects
                .extend(override_.add_effects.iter().cloned());
        }
    }
    for override_ in &policy.global_overrides {
        if present.contains(override_.flag.as_str()) {
            for effect in &override_.remove_effects {
                decision.effects.remove(effect);
            }
            decision
                .effects
                .extend(override_.add_effects.iter().cloned());
        }
    }
    decision
}

fn assert_no_mutation(decision: &Decision, context: &str) {
    for effect in [
        "local-write",
        "local-delete",
        "executes-user-code",
        "network-or-ssh",
        "scheduler-submit",
        "scheduler-cancel",
    ] {
        assert!(
            !decision.effects.contains(effect),
            "{context} unexpectedly retains {effect}: {decision:?}"
        );
    }
}

#[test]
fn policy_covers_each_public_clap_path_exactly_once() {
    let policy = load_policy();
    let mut public_paths = Vec::new();
    collect_public_command_paths(&build_cli_command_for_test(), &[], &mut public_paths);
    let public_paths = public_paths.into_iter().collect::<BTreeSet<_>>();
    let policy_paths = policy
        .commands
        .iter()
        .map(|entry| entry.path.clone())
        .collect::<BTreeSet<_>>();

    assert_eq!(
        policy.commands.len(),
        policy_paths.len(),
        "duplicate policy path"
    );
    assert_eq!(
        policy_paths, public_paths,
        "policy and public Clap paths drifted"
    );
}

#[test]
fn policy_effects_tiers_and_override_flags_are_valid() {
    let policy = load_policy();
    assert_eq!(policy.schema_version, 1);
    let known_effects = policy.effect_tags.keys().cloned().collect::<BTreeSet<_>>();
    let cli = build_cli_command_for_test();
    let global_flags = long_flags(&cli);

    for override_ in &policy.global_overrides {
        assert!(
            global_flags.contains(&override_.flag),
            "global policy override {} is not a public global Clap flag",
            override_.flag
        );
    }
    for command in &policy.commands {
        assert!(
            policy
                .authorization_tiers
                .contains_key(&command.authorization_tier),
            "{} has unknown tier {}",
            command.path,
            command.authorization_tier
        );
        assert!(
            command
                .effects
                .iter()
                .all(|effect| known_effects.contains(effect)),
            "{} has an unknown effect",
            command.path
        );
        let flags = long_flags(command_at_path(&cli, &command.path));
        for override_ in &command.overrides {
            for flag in &override_.flags {
                assert!(
                    flags.contains(flag),
                    "policy override {flag} does not exist on Clap path {} (available: {flags:?})",
                    command.path
                );
            }
            if let Some(tier) = &override_.authorization_tier {
                assert!(
                    policy.authorization_tiers.contains_key(tier),
                    "{} override has unknown tier {tier}",
                    command.path
                );
            }
            assert!(
                override_
                    .add_effects
                    .iter()
                    .chain(&override_.remove_effects)
                    .all(|effect| known_effects.contains(effect)),
                "{} override has an unknown effect",
                command.path
            );
        }
    }

    let mut orders = policy
        .authorization_tiers
        .values()
        .map(|tier| tier.order)
        .collect::<Vec<_>>();
    orders.sort_unstable();
    assert_eq!(orders, vec![1, 2, 3, 4, 5]);
}

#[test]
fn effect_sensitive_flags_are_classified_or_covered_by_the_base() {
    let policy = load_policy();
    let cli = build_cli_command_for_test();
    let cases = [
        ("--out", "local-write"),
        ("--script-out", "local-write"),
        ("--fix", "local-write"),
        ("--submit", "scheduler-submit"),
        ("--preemption", "scheduler-cancel"),
        ("--fs-probes", "scheduler-submit"),
        ("--run", "executes-user-code"),
        ("--open", "network-or-ssh"),
        ("--remote", "network-or-ssh"),
        ("--show-values", "sensitive-output"),
        ("--show-script", "sensitive-output"),
        ("--follow", "polls"),
        ("--rightsize", "scheduler-read"),
        ("--stop-when", "scheduler-cancel"),
    ];

    for command in &policy.commands {
        let flags = long_flags(command_at_path(&cli, &command.path));
        for (flag, effect) in cases {
            if !flags.contains(flag) || command.effects.iter().any(|item| item == effect) {
                continue;
            }
            assert!(
                command.overrides.iter().any(|override_| {
                    override_.flags.iter().any(|item| item == flag)
                        && override_.add_effects.iter().any(|item| item == effect)
                }),
                "{} exposes effect-sensitive {flag} without base or override effect {effect}",
                command.path
            );
        }
    }
}

#[test]
fn every_public_dry_run_removes_submit_and_cancel_effects() {
    let policy = load_policy();
    let cli = build_cli_command_for_test();
    for command in &policy.commands {
        let clap_command = command_at_path(&cli, &command.path);
        if !long_flags(clap_command).contains("--dry-run") {
            continue;
        }
        assert!(
            command
                .overrides
                .iter()
                .any(|override_| override_.flags == ["--dry-run"]),
            "{} exposes --dry-run but has no exact policy override",
            command.path
        );
        let decision = decide(&policy, &command.path, &["--dry-run"]);
        assert!(
            !decision.effects.contains("scheduler-submit")
                && !decision.effects.contains("scheduler-cancel"),
            "{} --dry-run retains scheduler mutation effects: {decision:?}",
            command.path
        );
    }
}

#[test]
fn write_and_remote_overrides_preserve_their_real_effects() {
    let policy = load_policy();

    for path in ["up", "notebook", "germinate"] {
        let decision = decide(&policy, path, &["--dry-run"]);
        assert_eq!(decision.tier, "scoped-local-mutation", "{path} --dry-run");
        assert!(decision.effects.contains("local-write"), "{path} --dry-run");
        assert!(
            !decision.effects.contains("scheduler-submit"),
            "{path} --dry-run"
        );
        assert!(
            !decision.effects.contains("scheduler-cancel"),
            "{path} --dry-run"
        );
    }

    let remote = decide(&policy, "up", &["--dry-run", "--remote"]);
    assert_eq!(remote.tier, "explicit-runtime-or-external-mutation");
    assert!(remote.effects.contains("local-write"));
    assert!(remote.effects.contains("network-or-ssh"));
    assert!(!remote.effects.contains("scheduler-submit"));
    assert!(!remote.effects.contains("scheduler-cancel"));

    let report_out = decide(&policy, "doctor cluster-report", &["--out"]);
    assert_eq!(report_out.tier, "scoped-local-mutation");
    assert!(report_out.effects.contains("local-write"));

    let workspace_status = decide(&policy, "workspace status", &[]);
    assert_eq!(workspace_status.tier, "scoped-local-mutation");
    assert!(workspace_status.effects.contains("local-write"));
}

#[test]
fn forward_safety_scenarios_preserve_agent_boundaries() {
    let policy = load_policy();

    // Single-batch and multi-service authoring use the same static JSON loop.
    for scenario in ["single-batch", "multi-service"] {
        for path in ["validate", "lint", "plan", "inspect"] {
            let decision = decide(&policy, path, &["--offline", "--format"]);
            assert_no_mutation(&decision, &format!("{scenario}: {path}"));
            assert_eq!(decision.tier, "automatic-read-only");
        }
    }

    // Unknown-site distributed work may inspect bundled guidance and scheduler facts,
    // but it does not get implicit permission to provision, execute, or submit.
    for path in ["docs", "examples recommend", "doctor cluster-report"] {
        let decision = decide(&policy, path, &[]);
        assert_no_mutation(&decision, &format!("unknown-site distributed: {path}"));
    }

    // A remote dry-run still stages over SSH. Global --offline blocks that
    // transport at runtime, but it does not turn the requested operation into an
    // automatic read-only action; Slurm mutation must remain absent either way.
    let remote_dry_run = decide(
        &policy,
        "up",
        &["--remote", "--dry-run", "--format", "--offline"],
    );
    assert_eq!(remote_dry_run.tier, "explicit-runtime-or-external-mutation");
    assert!(remote_dry_run.effects.contains("local-write"));
    assert!(!remote_dry_run.effects.contains("network-or-ssh"));
    assert!(!remote_dry_run.effects.contains("scheduler-submit"));
    assert!(!remote_dry_run.effects.contains("scheduler-cancel"));

    // Redacted JSON is safe to ingest; script/value/log/debug surfaces are not.
    assert!(
        !decide(&policy, "plan", &["--offline", "--format"])
            .effects
            .contains("sensitive-output")
    );
    assert!(
        decide(&policy, "plan", &["--verbose"])
            .effects
            .contains("sensitive-output")
    );
    for (path, flags) in [
        ("plan", vec!["--show-script"]),
        ("config", vec!["--show-values"]),
        ("context", vec!["--show-values"]),
        ("logs", vec![]),
        ("debug", vec![]),
    ] {
        assert!(
            decide(&policy, path, &flags)
                .effects
                .contains("sensitive-output"),
            "secret-bearing scenario must guard {path} {flags:?}"
        );
    }

    // Recovery observation remains non-mutating; export and cleanup retain their
    // separate local-write/destructive boundaries.
    for path in ["status", "debug", "checkpoints"] {
        let decision = decide(&policy, path, &[]);
        assert!(!decision.effects.contains("scheduler-cancel"));
        assert!(!decision.effects.contains("local-delete"));
    }
    assert_eq!(
        decide(&policy, "artifacts", &[]).tier,
        "scoped-local-mutation"
    );

    // Sweep resume is still a submission. Only its dry-run loses mutation effects.
    let resume = decide(&policy, "sweep submit", &["--resume"]);
    assert_eq!(resume.tier, "explicit-quota");
    assert!(resume.effects.contains("scheduler-submit"));
    let resume_dry_run = decide(&policy, "sweep submit", &["--resume", "--dry-run"]);
    assert_no_mutation(&resume_dry_run, "sweep resume dry-run");

    // An agent cannot turn a cleanup confirmation flag into authorization.
    for path in [
        "clean",
        "cache prune",
        "down",
        "cancel",
        "rendezvous prune",
        "sweep stop",
        "workspace release",
    ] {
        let decision = decide(&policy, path, &["--yes"]);
        assert_eq!(
            decision.tier, "explicit-destructive",
            "unauthorized cleanup was downgraded: {path}"
        );
        assert!(
            decision.effects.contains("local-delete")
                || decision.effects.contains("scheduler-cancel"),
            "destructive command lacks a destructive effect: {path}"
        );
    }
}

#[test]
fn generated_agent_assets_are_current() {
    let status = Command::new("python3")
        .current_dir(repo_root())
        .args(["scripts/generate_agent_assets.py", "--check"])
        .status()
        .expect("run agent asset generator");
    assert!(status.success(), "generated agent assets are stale");
}
