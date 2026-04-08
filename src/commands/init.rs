use std::io;
use std::path::PathBuf;

use anyhow::Result;
use clap_complete::Shell;
use hpc_compose::cli::build_cli_command;
use hpc_compose::init::{
    next_commands, prompt_for_init, render_template, write_initialized_template,
};

use crate::output;

#[allow(clippy::too_many_arguments)]
pub(crate) fn init(
    template: Option<String>,
    list_templates: bool,
    describe_template: Option<String>,
    name: Option<String>,
    cache_dir: Option<String>,
    output_path: PathBuf,
    force: bool,
) -> Result<()> {
    if list_templates {
        output::print_template_list();
        return Ok(());
    }
    if let Some(template_name) = describe_template {
        output::print_template_description(&template_name)?;
        return Ok(());
    }
    let answers = output::resolve_init_answers(template, name, cache_dir, prompt_for_init)?;
    let rendered = render_template(
        &answers.template_name,
        &answers.app_name,
        &answers.cache_dir,
    )?;
    let path = write_initialized_template(&output_path, &rendered, force)?;
    println!("wrote {}", path.display());
    for command in next_commands(&path) {
        println!("{command}");
    }
    Ok(())
}

pub(crate) fn completions(shell: Shell) -> Result<()> {
    let mut cmd = build_cli_command();
    clap_complete::generate(shell, &mut cmd, "hpc-compose", &mut io::stdout());
    Ok(())
}
