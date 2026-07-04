//! Progressive tutorial lessons for authoring hpc-compose specs.

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::init::render_template_body;

const DEFAULT_LESSON_ID: &str = "progressive-complexity";

/// A shipped spec-evolution lesson.
#[derive(Debug, Clone, Copy)]
pub struct EvolveLesson {
    id: &'static str,
    title: &'static str,
    description: &'static str,
    steps: &'static [EvolveStep],
}

impl EvolveLesson {
    /// Returns the stable lesson identifier used by the CLI.
    #[must_use]
    pub fn id(&self) -> &'static str {
        self.id
    }

    /// Returns the human-readable lesson title.
    #[must_use]
    pub fn title(&self) -> &'static str {
        self.title
    }

    /// Returns the lesson summary shown in discovery output.
    #[must_use]
    pub fn description(&self) -> &'static str {
        self.description
    }

    /// Returns the ordered tutorial steps.
    #[must_use]
    pub fn steps(&self) -> &'static [EvolveStep] {
        self.steps
    }
}

/// One valid spec snapshot in a lesson.
#[derive(Debug, Clone, Copy)]
pub struct EvolveStep {
    id: &'static str,
    title: &'static str,
    summary: &'static str,
    concepts: &'static [&'static str],
    source_templates: &'static [&'static str],
    body: &'static str,
}

impl EvolveStep {
    /// Returns the stable step identifier used by `--until`.
    #[must_use]
    pub fn id(&self) -> &'static str {
        self.id
    }

    /// Returns the human-readable step title.
    #[must_use]
    pub fn title(&self) -> &'static str {
        self.title
    }

    /// Returns the short transition summary.
    #[must_use]
    pub fn summary(&self) -> &'static str {
        self.summary
    }

    /// Returns the concepts introduced by this step.
    #[must_use]
    pub fn concepts(&self) -> &'static [&'static str] {
        self.concepts
    }

    /// Returns related built-in template ids that inspired this step.
    #[must_use]
    pub fn source_templates(&self) -> &'static [&'static str] {
        self.source_templates
    }
}

/// Validation facts captured after a candidate step plans successfully.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct EvolveValidationSummary {
    pub service_count: usize,
    pub services: Vec<String>,
    pub allocation_nodes: u32,
    pub placement_modes: Vec<String>,
}

/// One accepted evolve step in a run report.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct EvolveAcceptedStep {
    pub id: String,
    pub title: String,
    pub validation: EvolveValidationSummary,
}

/// Machine-readable output for a completed evolve run.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct EvolveRunReport {
    pub lesson_id: String,
    pub lesson_title: String,
    pub app_name: String,
    pub cache_dir: Option<String>,
    pub output_path: std::path::PathBuf,
    pub accepted_steps: Vec<EvolveAcceptedStep>,
    pub skipped_steps: Vec<String>,
    pub final_step: Option<String>,
    pub next_commands: Vec<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum EvolvePromptAction {
    Accept,
    Skip,
    Quit,
    Help,
}

const PROGRESSIVE_STEPS: &[EvolveStep] = &[
    EvolveStep {
        id: "minimal",
        title: "Minimal batch spec",
        summary: "Start with one service and one single-node Slurm allocation.",
        concepts: &["top-level x-slurm resources", "one service", "safe plan"],
        source_templates: &["minimal-batch"],
        body: r#"
name: progressive-complexity

x-slurm:
  job_name: progressive-complexity
  time: "00:10:00"
  mem: 4G
  cpus_per_task: 1

services:
  app:
    image: python:3.11-slim
    command: python -c "print('Hello from Slurm!')"
"#,
    },
    EvolveStep {
        id: "second-service",
        title: "Add a dependent service",
        summary: "Split the workflow into an app service and a dependent worker service.",
        concepts: &[
            "multiple services",
            "startup ordering",
            "service_started dependency",
        ],
        source_templates: &["app-redis-worker"],
        body: r#"
name: progressive-complexity

x-slurm:
  job_name: progressive-complexity
  time: "00:10:00"
  mem: 4G
  cpus_per_task: 1

services:
  app:
    image: python:3.11-slim
    command:
      - /bin/sh
      - -lc
      - |
        python - <<'PY'
        import time
        print("app started")
        time.sleep(20)
        PY

  worker:
    image: python:3.11-slim
    depends_on:
      app:
        condition: service_started
    command: python -c "print('worker started after app launch')"
"#,
    },
    EvolveStep {
        id: "readiness",
        title: "Gate on readiness",
        summary: "Make the dependency wait for a concrete readiness check instead of launch.",
        concepts: &[
            "readiness",
            "service_healthy dependency",
            "localhost inside one node",
        ],
        source_templates: &["app-redis-worker"],
        body: r#"
name: progressive-complexity

x-slurm:
  job_name: progressive-complexity
  time: "00:10:00"
  mem: 4G
  cpus_per_task: 1

services:
  app:
    image: python:3.11-slim
    command:
      - /bin/sh
      - -lc
      - |
        python - <<'PY'
        import http.server
        import socketserver

        class Handler(http.server.SimpleHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ready\n")

        with socketserver.TCPServer(("127.0.0.1", 8000), Handler) as server:
            print("app ready on 127.0.0.1:8000")
            server.serve_forever()
        PY
    readiness:
      type: http
      url: http://127.0.0.1:8000/
      status_code: 200
      timeout_seconds: 30

  worker:
    image: python:3.11-slim
    depends_on:
      app:
        condition: service_healthy
    command:
      - /bin/sh
      - -lc
      - |
        python - <<'PY'
        import urllib.request
        print(urllib.request.urlopen("http://127.0.0.1:8000/", timeout=5).read().decode().strip())
        PY
"#,
    },
    EvolveStep {
        id: "failure-policy",
        title: "Add a bounded failure policy",
        summary: "Let a transient worker failure restart without permitting an infinite crash loop.",
        concepts: &[
            "restart_on_failure",
            "retry budget",
            "rolling crash-loop window",
        ],
        source_templates: &["restart-policy"],
        body: r#"
name: progressive-complexity

x-slurm:
  job_name: progressive-complexity
  time: "00:10:00"
  mem: 4G
  cpus_per_task: 1

services:
  app:
    image: python:3.11-slim
    command:
      - /bin/sh
      - -lc
      - |
        python - <<'PY'
        import http.server
        import socketserver

        class Handler(http.server.SimpleHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ready\n")

        with socketserver.TCPServer(("127.0.0.1", 8000), Handler) as server:
            print("app ready on 127.0.0.1:8000")
            server.serve_forever()
        PY
    readiness:
      type: http
      url: http://127.0.0.1:8000/
      status_code: 200
      timeout_seconds: 30

  worker:
    image: python:3.11-slim
    depends_on:
      app:
        condition: service_healthy
    command:
      - /bin/sh
      - -lc
      - |
        python - <<'PY'
        import pathlib
        import sys
        import urllib.request

        state = pathlib.Path("/hpc-compose/job/evolve-worker-attempts.txt")
        attempts = int(state.read_text()) if state.exists() else 0
        attempts += 1
        state.write_text(f"{attempts}\n")

        print(urllib.request.urlopen("http://127.0.0.1:8000/", timeout=5).read().decode().strip())
        print(f"worker attempt {attempts}")
        if attempts == 1:
            print("simulating one transient failure")
            sys.exit(42)
        print("worker completed")
        PY
    x-slurm:
      failure_policy:
        mode: restart_on_failure
        max_restarts: 3
        backoff_seconds: 2
        window_seconds: 60
        max_restarts_in_window: 2
"#,
    },
    EvolveStep {
        id: "multi-node-placement",
        title: "Add multi-node placement",
        summary: "Reserve two nodes and pin services to explicit non-overlapping allocation node ranges.",
        concepts: &[
            "multi-node allocation",
            "placement.node_range",
            "non-overlap",
        ],
        source_templates: &["multi-node-mpi", "ray-head-workers"],
        body: r#"
name: progressive-complexity

x-slurm:
  job_name: progressive-complexity
  time: "00:10:00"
  nodes: 2
  mem: 4G
  cpus_per_task: 1

services:
  app:
    image: python:3.11-slim
    command:
      - /bin/sh
      - -lc
      - |
        echo "app service nodes=$$HPC_COMPOSE_SERVICE_NODELIST"
        sleep 30
    readiness:
      type: sleep
      seconds: 1
    x-slurm:
      placement:
        node_range: "0"

  worker:
    image: python:3.11-slim
    depends_on:
      app:
        condition: service_healthy
    command:
      - /bin/sh
      - -lc
      - |
        python - <<'PY'
        import pathlib
        import sys

        state = pathlib.Path("/hpc-compose/job/evolve-worker-attempts.txt")
        attempts = int(state.read_text()) if state.exists() else 0
        attempts += 1
        state.write_text(f"{attempts}\n")

        print("worker running on its placed node")
        print(f"worker attempt {attempts}")
        if attempts == 1:
            print("simulating one transient failure")
            sys.exit(42)
        print("worker completed")
        PY
    x-slurm:
      placement:
        node_range: "1"
      failure_policy:
        mode: restart_on_failure
        max_restarts: 3
        backoff_seconds: 2
        window_seconds: 60
        max_restarts_in_window: 2
"#,
    },
];

const LESSONS: &[EvolveLesson] = &[EvolveLesson {
    id: DEFAULT_LESSON_ID,
    title: "Spec Metamorphosis: Progressive Complexity",
    description: "Build one valid spec from a minimal batch job into a multi-service, readiness-gated, failure-aware, multi-node workflow.",
    steps: PROGRESSIVE_STEPS,
}];

/// Returns the default lesson id used by `hpc-compose evolve`.
#[must_use]
pub fn default_lesson_id() -> &'static str {
    DEFAULT_LESSON_ID
}

/// Returns all shipped evolve lessons.
#[must_use]
pub fn lessons() -> &'static [EvolveLesson] {
    LESSONS
}

/// Resolves a shipped lesson by id.
///
/// # Errors
///
/// Returns an error when the id is not a shipped lesson.
pub fn resolve_lesson(id: &str) -> Result<&'static EvolveLesson> {
    LESSONS
        .iter()
        .find(|lesson| lesson.id == id)
        .with_context(|| {
            format!(
                "unknown evolve lesson '{}'; available lessons: {}",
                id,
                LESSONS
                    .iter()
                    .map(EvolveLesson::id)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
}

/// Renders one evolve step with the requested application name and cache dir.
///
/// # Errors
///
/// Returns an error when the shipped step body cannot be parsed or serialized.
pub fn render_step(step: &EvolveStep, app_name: &str, cache_dir: Option<&str>) -> Result<String> {
    render_template_body(step.body, step.id, app_name, cache_dir)
}

#[cfg(test)]
pub(crate) fn validate_source_templates() -> Result<()> {
    for lesson in LESSONS {
        for step in lesson.steps {
            for template in step.source_templates {
                crate::init::resolve_template(template)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn select_steps_until(
    lesson: &EvolveLesson,
    until: Option<&str>,
) -> Result<&'static [EvolveStep]> {
    let Some(until) = until else {
        return Ok(lesson.steps);
    };
    let index = lesson
        .steps
        .iter()
        .position(|step| step.id == until)
        .with_context(|| {
            format!(
                "unknown step '{}' for lesson '{}'; available steps: {}",
                until,
                lesson.id,
                lesson
                    .steps
                    .iter()
                    .map(EvolveStep::id)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
    Ok(&lesson.steps[..=index])
}

pub(crate) fn parse_prompt_action(input: &str) -> Result<EvolvePromptAction> {
    match input.trim().to_ascii_lowercase().as_str() {
        "" | "y" | "yes" | "a" | "accept" => Ok(EvolvePromptAction::Accept),
        "s" | "skip" => Ok(EvolvePromptAction::Skip),
        "q" | "quit" => Ok(EvolvePromptAction::Quit),
        "?" | "h" | "help" => Ok(EvolvePromptAction::Help),
        other => bail!(
            "unknown evolve response '{}'; press Enter/y/a to accept, s to skip, q to quit, or ? for help",
            other
        ),
    }
}

pub(crate) fn compact_line_diff(previous: &str, candidate: &str, max_lines: usize) -> String {
    if previous == candidate {
        return "(no changes)\n".to_string();
    }
    let previous_lines = previous.lines().collect::<Vec<_>>();
    let candidate_lines = candidate.lines().collect::<Vec<_>>();
    let ops = diff_ops(&previous_lines, &candidate_lines);
    let changed = ops
        .iter()
        .enumerate()
        .filter_map(|(index, op)| (!matches!(op, DiffOp::Equal(_))).then_some(index))
        .collect::<Vec<_>>();

    let mut lines = vec!["--- previous".to_string(), "+++ candidate".to_string()];
    let mut skipped = false;
    for (index, op) in ops.iter().enumerate() {
        let near_change = changed
            .iter()
            .any(|changed_index| index.abs_diff(*changed_index) <= 2);
        match op {
            DiffOp::Equal(line) if near_change => {
                skipped = false;
                lines.push(format!("  {line}"));
            }
            DiffOp::Equal(_) => {
                if !skipped {
                    lines.push("  ...".to_string());
                    skipped = true;
                }
            }
            DiffOp::Remove(line) => {
                skipped = false;
                lines.push(format!("- {line}"));
            }
            DiffOp::Add(line) => {
                skipped = false;
                lines.push(format!("+ {line}"));
            }
        }
    }

    if max_lines > 0 && lines.len() > max_lines {
        lines.truncate(max_lines.saturating_sub(1));
        lines.push("... diff truncated ...".to_string());
    }
    lines.push(String::new());
    lines.join("\n")
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum DiffOp<'a> {
    Equal(&'a str),
    Remove(&'a str),
    Add(&'a str),
}

fn diff_ops<'a>(previous: &[&'a str], candidate: &[&'a str]) -> Vec<DiffOp<'a>> {
    let mut lcs = vec![vec![0usize; candidate.len() + 1]; previous.len() + 1];
    for i in (0..previous.len()).rev() {
        for j in (0..candidate.len()).rev() {
            lcs[i][j] = if previous[i] == candidate[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }

    let mut ops = Vec::new();
    let mut i = 0;
    let mut j = 0;
    while i < previous.len() && j < candidate.len() {
        if previous[i] == candidate[j] {
            ops.push(DiffOp::Equal(previous[i]));
            i += 1;
            j += 1;
        } else if lcs[i + 1][j] >= lcs[i][j + 1] {
            ops.push(DiffOp::Remove(previous[i]));
            i += 1;
        } else {
            ops.push(DiffOp::Add(candidate[j]));
            j += 1;
        }
    }
    while i < previous.len() {
        ops.push(DiffOp::Remove(previous[i]));
        i += 1;
    }
    while j < candidate.len() {
        ops.push(DiffOp::Add(candidate[j]));
        j += 1;
    }
    ops
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use serde_norway::Value;

    use super::*;
    use crate::init::resolve_template;
    use crate::planner::{ServicePlacementMode, build_plan};
    use crate::prepare::build_runtime_plan;
    use crate::spec::{ComposeSpec, DependencyCondition, ServiceFailureMode};

    fn rendered_step(id: &str) -> String {
        let lesson = resolve_lesson(default_lesson_id()).expect("lesson");
        let step = lesson
            .steps()
            .iter()
            .find(|step| step.id() == id)
            .expect("step");
        render_step(step, "custom-app", None).expect("render")
    }

    fn plan_for_step(id: &str) -> crate::planner::Plan {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join(format!("{id}.yaml"));
        fs::write(&path, rendered_step(id)).expect("write");
        let spec =
            ComposeSpec::load_with_interpolation_vars(&path, &BTreeMap::new()).expect("load spec");
        build_plan(&path, spec).expect("build plan")
    }

    #[test]
    fn default_lesson_and_step_ids_are_stable() {
        let lesson = resolve_lesson(default_lesson_id()).expect("lesson");
        assert_eq!(lesson.id(), "progressive-complexity");
        assert_eq!(
            lesson
                .steps()
                .iter()
                .map(EvolveStep::id)
                .collect::<Vec<_>>(),
            vec![
                "minimal",
                "second-service",
                "readiness",
                "failure-policy",
                "multi-node-placement"
            ]
        );
        assert!(resolve_lesson("missing").is_err());
        assert!(select_steps_until(lesson, Some("readiness")).is_ok());
        assert!(select_steps_until(lesson, Some("missing")).is_err());
    }

    #[test]
    fn lesson_ids_step_ids_and_sources_are_unique_and_resolvable() {
        let mut lesson_ids = std::collections::BTreeSet::new();
        for lesson in lessons() {
            assert!(lesson_ids.insert(lesson.id()));
            let mut step_ids = std::collections::BTreeSet::new();
            for step in lesson.steps() {
                assert!(step_ids.insert(step.id()));
                for template in step.source_templates() {
                    resolve_template(template).expect("source template");
                }
            }
        }
        validate_source_templates().expect("sources");
    }

    #[test]
    fn steps_render_with_and_without_cache_dir_and_rewrite_names() {
        let lesson = resolve_lesson(default_lesson_id()).expect("lesson");
        for step in lesson.steps() {
            let without_cache = render_step(step, "custom-app", None).expect("render no cache");
            assert!(!without_cache.contains("cache_dir:"));

            let with_cache =
                render_step(step, "custom-app", Some("/shared/cache")).expect("render cache");
            let value: Value = serde_norway::from_str(&with_cache).expect("yaml");
            let root = value.as_mapping().expect("root");
            assert_eq!(
                root.get(Value::String("name".into()))
                    .and_then(Value::as_str),
                Some("custom-app")
            );
            let slurm = root
                .get(Value::String("x-slurm".into()))
                .and_then(Value::as_mapping)
                .expect("x-slurm");
            assert_eq!(
                slurm
                    .get(Value::String("job_name".into()))
                    .and_then(Value::as_str),
                Some("custom-app")
            );
            assert_eq!(
                slurm
                    .get(Value::String("cache_dir".into()))
                    .and_then(Value::as_str),
                Some("/shared/cache")
            );
        }
    }

    #[test]
    fn every_step_loads_plans_and_builds_runtime_plan() {
        let lesson = resolve_lesson(default_lesson_id()).expect("lesson");
        for step in lesson.steps() {
            let tmpdir = tempfile::tempdir().expect("tmpdir");
            let path = tmpdir.path().join(format!("{}.yaml", step.id()));
            fs::write(
                &path,
                render_step(step, "custom-app", None).expect("render"),
            )
            .expect("write");
            let spec = ComposeSpec::load_with_interpolation_vars(&path, &BTreeMap::new())
                .unwrap_or_else(|err| panic!("load {}: {err}", step.id()));
            let plan =
                build_plan(&path, spec).unwrap_or_else(|err| panic!("plan {}: {err}", step.id()));
            let runtime = build_runtime_plan(&plan);
            assert_eq!(runtime.ordered_services.len(), plan.ordered_services.len());
        }
    }

    #[test]
    fn semantic_progression_matches_expected_shape() {
        let minimal = plan_for_step("minimal");
        assert_eq!(minimal.ordered_services.len(), 1);
        assert_eq!(minimal.slurm.allocation_nodes(), 1);

        let second = plan_for_step("second-service");
        assert_eq!(second.ordered_services.len(), 2);
        let worker = second
            .ordered_services
            .iter()
            .find(|service| service.name == "worker")
            .expect("worker");
        assert_eq!(worker.depends_on.len(), 1);
        assert_eq!(
            worker.depends_on[0].condition,
            DependencyCondition::ServiceStarted
        );

        let readiness = plan_for_step("readiness");
        let app = readiness
            .ordered_services
            .iter()
            .find(|service| service.name == "app")
            .expect("app");
        assert!(app.readiness.is_some());
        let worker = readiness
            .ordered_services
            .iter()
            .find(|service| service.name == "worker")
            .expect("worker");
        assert_eq!(
            worker.depends_on[0].condition,
            DependencyCondition::ServiceHealthy
        );

        let failure = plan_for_step("failure-policy");
        let worker = failure
            .ordered_services
            .iter()
            .find(|service| service.name == "worker")
            .expect("worker");
        assert_eq!(
            worker.failure_policy.mode,
            ServiceFailureMode::RestartOnFailure
        );
        assert_eq!(worker.failure_policy.max_restarts, 3);
        assert_eq!(worker.failure_policy.window_seconds, 60);
        assert_eq!(worker.failure_policy.max_restarts_in_window, 2);

        let multi = plan_for_step("multi-node-placement");
        assert_eq!(multi.slurm.allocation_nodes(), 2);
        let app = multi
            .ordered_services
            .iter()
            .find(|service| service.name == "app")
            .expect("app");
        let worker = multi
            .ordered_services
            .iter()
            .find(|service| service.name == "worker")
            .expect("worker");
        assert_eq!(app.placement.mode, ServicePlacementMode::PrimaryNode);
        assert_eq!(worker.placement.mode, ServicePlacementMode::Partitioned);
        assert_eq!(app.placement.node_indices.as_deref(), Some(&[0][..]));
        assert_eq!(worker.placement.node_indices.as_deref(), Some(&[1][..]));
    }

    #[test]
    fn compact_diff_handles_new_changed_and_truncated_output() {
        let new_file = compact_line_diff("", "a\nb\n", 20);
        assert!(new_file.contains("+ a"));
        assert!(new_file.contains("+ b"));

        let changed = compact_line_diff("a\nb\nc\nd\n", "a\nb2\nc\nd\n", 20);
        assert!(changed.contains("  a"));
        assert!(changed.contains("- b"));
        assert!(changed.contains("+ b2"));
        assert!(changed.contains("  c"));

        let truncated = compact_line_diff("", "1\n2\n3\n4\n5\n", 4);
        assert!(truncated.contains("diff truncated"));
    }

    #[test]
    fn prompt_parser_accepts_supported_controls() {
        assert_eq!(
            parse_prompt_action("").expect("empty"),
            EvolvePromptAction::Accept
        );
        assert_eq!(
            parse_prompt_action("y").expect("yes"),
            EvolvePromptAction::Accept
        );
        assert_eq!(
            parse_prompt_action("a").expect("accept"),
            EvolvePromptAction::Accept
        );
        assert_eq!(
            parse_prompt_action("s").expect("skip"),
            EvolvePromptAction::Skip
        );
        assert_eq!(
            parse_prompt_action("q").expect("quit"),
            EvolvePromptAction::Quit
        );
        assert_eq!(
            parse_prompt_action("?").expect("help"),
            EvolvePromptAction::Help
        );
        assert!(parse_prompt_action("nope").is_err());
    }
}
