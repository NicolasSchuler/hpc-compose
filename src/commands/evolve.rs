use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use hpc_compose::cli::OutputFormat;
use hpc_compose::evolve::{
    EvolveAcceptedStep, EvolveLesson, EvolvePromptAction, EvolveRunReport, EvolveStep,
    EvolveValidationSummary, compact_line_diff, default_lesson_id, lessons, parse_prompt_action,
    render_step, resolve_lesson, select_steps_until,
};
use hpc_compose::init::{next_commands, write_initialized_template};
use hpc_compose::planner::{ServicePlacementMode, build_plan};
use hpc_compose::prepare::build_runtime_plan;
use hpc_compose::spec::ComposeSpec;
use hpc_compose::term;
use serde::Serialize;

use crate::output::common as output_common;

#[allow(clippy::too_many_arguments)]
pub(crate) fn command(
    lesson: Option<String>,
    list_lessons: bool,
    describe_lesson: Option<String>,
    name: Option<String>,
    cache_dir: Option<String>,
    output_path: PathBuf,
    force: bool,
    yes: bool,
    until: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    if list_lessons {
        return print_lesson_list(format);
    }
    if let Some(lesson_id) = describe_lesson {
        let lesson = resolve_lesson(&lesson_id)?;
        return print_lesson_description(lesson, format);
    }

    let format = output_common::resolve_output_format(format);
    if format == OutputFormat::Json && !yes {
        bail!("hpc-compose evolve --format json requires --yes for noninteractive execution");
    }

    let lesson_id = lesson.unwrap_or_else(|| default_lesson_id().to_string());
    let lesson = resolve_lesson(&lesson_id)?;
    let steps = select_steps_until(lesson, until.as_deref())?;
    let app_name = name.unwrap_or_else(|| lesson.id().to_string());
    let cache_dir = normalize_cache_dir(cache_dir)?;
    let output_path = prepare_output_path(&output_path, force)?;

    let report = if yes {
        run_noninteractive(
            lesson,
            steps,
            &app_name,
            cache_dir.as_deref(),
            &output_path,
            force,
        )?
    } else {
        let mut stdin = io::stdin().lock();
        let mut stdout = io::stdout();
        run_interactive(
            lesson,
            steps,
            &app_name,
            cache_dir.as_deref(),
            &output_path,
            force,
            &mut stdin,
            &mut stdout,
        )?
    };

    match format {
        OutputFormat::Text => print_run_report(&report),
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&crate::output::contract::EvolveOutput::new(report))
                .context("failed to serialize evolve run output")?
        ),
    }
    Ok(())
}

fn normalize_cache_dir(cache_dir: Option<String>) -> Result<Option<String>> {
    match cache_dir {
        Some(value) if value.trim().is_empty() => bail!(
            "--cache-dir cannot be empty; choose a path visible from both the login node and the compute nodes"
        ),
        other => Ok(other),
    }
}

fn prepare_output_path(output_path: &Path, force: bool) -> Result<PathBuf> {
    let output_path = crate::path_util::absolute_path_cwd(output_path)?;
    if output_path.exists() && !force {
        bail!(
            "refusing to overwrite {}; pass --force to replace it",
            output_path.display()
        );
    }
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;
    }
    Ok(output_path)
}

fn run_noninteractive(
    lesson: &EvolveLesson,
    steps: &[EvolveStep],
    app_name: &str,
    cache_dir: Option<&str>,
    output_path: &Path,
    force: bool,
) -> Result<EvolveRunReport> {
    let mut accepted_steps = Vec::new();
    let mut wrote_once = false;
    for step in steps {
        let rendered = render_step(step, app_name, cache_dir)?;
        let validation = validate_candidate(output_path, &rendered)?;
        write_initialized_template(output_path, &rendered, force || wrote_once)?;
        wrote_once = true;
        accepted_steps.push(EvolveAcceptedStep {
            id: step.id().to_string(),
            title: step.title().to_string(),
            validation,
        });
    }
    Ok(build_report(
        lesson,
        app_name,
        cache_dir,
        output_path,
        accepted_steps,
        Vec::new(),
    ))
}

#[allow(clippy::too_many_arguments)]
fn run_interactive(
    lesson: &EvolveLesson,
    steps: &[EvolveStep],
    app_name: &str,
    cache_dir: Option<&str>,
    output_path: &Path,
    force: bool,
    input: &mut impl BufRead,
    output: &mut impl Write,
) -> Result<EvolveRunReport> {
    writeln!(
        output,
        "{}",
        term::styled_section_header("hpc-compose evolve")
    )
    .ok();
    writeln!(
        output,
        "{} {}",
        term::styled_bold("lesson:"),
        lesson.title()
    )
    .ok();
    writeln!(output, "{}", term::styled_dim(lesson.description())).ok();
    writeln!(output).ok();
    writeln!(output, "Controls: Enter/y/a accept, s skip, q quit, ? help").ok();

    let mut previous = if output_path.exists() {
        fs::read_to_string(output_path).unwrap_or_default()
    } else {
        String::new()
    };
    let mut accepted_steps = Vec::new();
    let mut skipped_steps = Vec::new();
    let mut wrote_once = false;

    for (index, step) in steps.iter().enumerate() {
        let rendered = render_step(step, app_name, cache_dir)?;
        let validation = validate_candidate(output_path, &rendered)?;
        print_step_preview(
            output,
            step,
            index + 1,
            steps.len(),
            &previous,
            &validation,
            &rendered,
        )?;

        loop {
            write!(output, "Accept this step? [Y/a/s/q/?] ").ok();
            output.flush().ok();
            let mut line = String::new();
            input
                .read_line(&mut line)
                .context("failed to read evolve response")?;
            match parse_prompt_action(&line) {
                Ok(EvolvePromptAction::Accept) => {
                    write_initialized_template(output_path, &rendered, force || wrote_once)?;
                    writeln!(output, "wrote {}", output_path.display()).ok();
                    accepted_steps.push(EvolveAcceptedStep {
                        id: step.id().to_string(),
                        title: step.title().to_string(),
                        validation,
                    });
                    previous = rendered;
                    wrote_once = true;
                    break;
                }
                Ok(EvolvePromptAction::Skip) => {
                    writeln!(output, "skipped {}", step.id()).ok();
                    skipped_steps.push(step.id().to_string());
                    break;
                }
                Ok(EvolvePromptAction::Quit) => {
                    writeln!(output, "stopped").ok();
                    return Ok(build_report(
                        lesson,
                        app_name,
                        cache_dir,
                        output_path,
                        accepted_steps,
                        skipped_steps,
                    ));
                }
                Ok(EvolvePromptAction::Help) => {
                    print_prompt_help(output);
                }
                Err(err) => {
                    writeln!(output, "{err}").ok();
                }
            }
        }
    }

    Ok(build_report(
        lesson,
        app_name,
        cache_dir,
        output_path,
        accepted_steps,
        skipped_steps,
    ))
}

fn print_step_preview(
    output: &mut impl Write,
    step: &EvolveStep,
    number: usize,
    total: usize,
    previous: &str,
    validation: &EvolveValidationSummary,
    rendered: &str,
) -> Result<()> {
    writeln!(output).ok();
    writeln!(
        output,
        "{} {number}/{total}: {}",
        term::styled_bold("Step"),
        step.title()
    )
    .ok();
    writeln!(output, "{}", step.summary()).ok();
    writeln!(output, "concepts: {}", step.concepts().join(", ")).ok();
    writeln!(
        output,
        "validates: services={} nodes={} placements={}",
        validation.service_count,
        validation.allocation_nodes,
        validation.placement_modes.join(",")
    )
    .ok();
    writeln!(output, "{}", term::styled_section_header("Diff:")).ok();
    write!(output, "{}", compact_line_diff(previous, rendered, 90))
        .context("failed to write evolve diff")?;
    Ok(())
}

fn print_prompt_help(output: &mut impl Write) {
    writeln!(
        output,
        "Enter/y/a accepts and writes the candidate spec; s skips it; q quits after the last accepted valid spec; ? shows this help."
    )
    .ok();
}

fn build_report(
    lesson: &EvolveLesson,
    app_name: &str,
    cache_dir: Option<&str>,
    output_path: &Path,
    accepted_steps: Vec<EvolveAcceptedStep>,
    skipped_steps: Vec<String>,
) -> EvolveRunReport {
    let final_step = accepted_steps.last().map(|step| step.id.clone());
    let next_commands = final_step
        .as_ref()
        .map(|_| next_commands(output_path))
        .unwrap_or_default();
    EvolveRunReport {
        lesson_id: lesson.id().to_string(),
        lesson_title: lesson.title().to_string(),
        app_name: app_name.to_string(),
        cache_dir: cache_dir.map(str::to_string),
        output_path: output_path.to_path_buf(),
        accepted_steps,
        skipped_steps,
        final_step,
        next_commands,
    }
}

fn print_run_report(report: &EvolveRunReport) {
    if report.accepted_steps.is_empty() {
        println!("no spec written");
        if !report.skipped_steps.is_empty() {
            println!("skipped steps: {}", report.skipped_steps.join(", "));
        }
        return;
    }
    println!(
        "accepted {} step(s); final step: {}",
        report.accepted_steps.len(),
        report.final_step.as_deref().unwrap_or("<none>")
    );
    println!("output: {}", report.output_path.display());
    if !report.skipped_steps.is_empty() {
        println!("skipped steps: {}", report.skipped_steps.join(", "));
    }
    for command in &report.next_commands {
        println!("{command}");
    }
}

fn validate_candidate(output_path: &Path, rendered: &str) -> Result<EvolveValidationSummary> {
    let parent = output_path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;
    let temp_path = parent.join(format!(
        ".hpc-compose-evolve-{}-{}.tmp.yaml",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    fs::write(&temp_path, rendered).context(format!(
        "failed to write validation temp {}",
        temp_path.display()
    ))?;

    let result = (|| {
        let spec = ComposeSpec::load(&temp_path)?;
        let plan = build_plan(&temp_path, spec)?;
        let runtime_plan = build_runtime_plan(&plan);
        Ok(EvolveValidationSummary {
            service_count: runtime_plan.ordered_services.len(),
            services: runtime_plan
                .ordered_services
                .iter()
                .map(|service| service.name.clone())
                .collect(),
            allocation_nodes: runtime_plan.slurm.allocation_nodes(),
            placement_modes: runtime_plan
                .ordered_services
                .iter()
                .map(|service| placement_mode_label(service.placement.mode).to_string())
                .collect(),
        })
    })();
    let remove_result = fs::remove_file(&temp_path);
    match (result, remove_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Ok(_), Err(err)) => Err(err).context(format!(
            "failed to remove validation temp {}",
            temp_path.display()
        )),
        (Err(err), Ok(())) => Err(err),
        (Err(err), Err(remove_err)) => Err(err).context(format!(
            "also failed to remove validation temp {}: {remove_err}",
            temp_path.display()
        )),
    }
}

fn placement_mode_label(mode: ServicePlacementMode) -> &'static str {
    match mode {
        ServicePlacementMode::PrimaryNode => "primary",
        ServicePlacementMode::Partitioned => "partitioned",
        ServicePlacementMode::Distributed => "distributed",
    }
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct LessonListOutput {
    pub(crate) schema_version: u32,
    lessons: Vec<LessonDescriptionOutput>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct LessonDescriptionOutput {
    pub(crate) schema_version: u32,
    id: String,
    title: String,
    description: String,
    step_count: usize,
    steps: Vec<StepDescriptionOutput>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct StepDescriptionOutput {
    id: String,
    title: String,
    summary: String,
    concepts: Vec<String>,
    source_templates: Vec<String>,
}

fn print_lesson_list(format: Option<OutputFormat>) -> Result<()> {
    match output_common::resolve_output_format(format) {
        OutputFormat::Text => {
            println!("lessons:");
            for lesson in lessons() {
                println!(
                    "  {}\t{} ({} steps)",
                    term::styled_bold(lesson.id()),
                    lesson.description(),
                    lesson.steps().len()
                );
            }
            Ok(())
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&LessonListOutput {
                    schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
                    lessons: lessons().iter().map(describe_lesson_output).collect(),
                })
                .context("failed to serialize evolve lesson list")?
            );
            Ok(())
        }
    }
}

fn print_lesson_description(lesson: &EvolveLesson, format: Option<OutputFormat>) -> Result<()> {
    match output_common::resolve_output_format(format) {
        OutputFormat::Text => {
            println!("{}", term::styled_label("lesson", lesson.id()));
            println!("{}", term::styled_label("title", lesson.title()));
            println!(
                "{}",
                term::styled_label("description", lesson.description())
            );
            println!(
                "{}",
                term::styled_label("steps", &lesson.steps().len().to_string())
            );
            for (index, step) in lesson.steps().iter().enumerate() {
                println!(
                    "  {}. {} - {}",
                    index + 1,
                    term::styled_bold(step.id()),
                    step.title()
                );
                println!("     {}", term::styled_dim(step.summary()));
                println!("     concepts: {}", step.concepts().join(", "));
            }
            Ok(())
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&describe_lesson_output(lesson))
                    .context("failed to serialize evolve lesson description")?
            );
            Ok(())
        }
    }
}

fn describe_lesson_output(lesson: &EvolveLesson) -> LessonDescriptionOutput {
    LessonDescriptionOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        id: lesson.id().to_string(),
        title: lesson.title().to_string(),
        description: lesson.description().to_string(),
        step_count: lesson.steps().len(),
        steps: lesson
            .steps()
            .iter()
            .map(|step| StepDescriptionOutput {
                id: step.id().to_string(),
                title: step.title().to_string(),
                summary: step.summary().to_string(),
                concepts: step
                    .concepts()
                    .iter()
                    .map(|item| (*item).to_string())
                    .collect(),
                source_templates: step
                    .source_templates()
                    .iter()
                    .map(|item| (*item).to_string())
                    .collect(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_temp_file_is_removed_on_success_and_failure() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let output = tmpdir.path().join("compose.yaml");
        let lesson = resolve_lesson(default_lesson_id()).expect("lesson");
        let rendered =
            render_step(&lesson.steps()[0], "custom-app", None).expect("render valid step");
        validate_candidate(&output, &rendered).expect("valid candidate");
        assert_no_validation_temps(tmpdir.path());

        let err = validate_candidate(&output, "not: [valid").expect_err("invalid candidate");
        assert!(err.to_string().contains("failed to parse YAML"));
        assert_no_validation_temps(tmpdir.path());
    }

    #[test]
    fn skipped_steps_still_allow_later_acceptance_to_validate() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let output = tmpdir.path().join("compose.yaml");
        let lesson = resolve_lesson(default_lesson_id()).expect("lesson");
        let report = run_interactive(
            lesson,
            select_steps_until(lesson, Some("readiness")).expect("steps"),
            "custom-app",
            None,
            &output,
            false,
            &mut std::io::Cursor::new(b"\ns\n\n"),
            &mut Vec::new(),
        )
        .expect("interactive");
        assert_eq!(
            report
                .accepted_steps
                .iter()
                .map(|step| step.id.as_str())
                .collect::<Vec<_>>(),
            vec!["minimal", "readiness"]
        );
        assert_eq!(report.skipped_steps, vec!["second-service"]);
        let final_yaml = fs::read_to_string(output).expect("final spec");
        assert!(final_yaml.contains("condition: service_healthy"));
    }

    fn assert_no_validation_temps(path: &Path) {
        for entry in fs::read_dir(path).expect("read tmpdir") {
            let entry = entry.expect("entry");
            let name = entry.file_name();
            let name = name.to_string_lossy();
            assert!(
                !name.starts_with(".hpc-compose-evolve-"),
                "unexpected validation temp file {name}"
            );
        }
    }
}
