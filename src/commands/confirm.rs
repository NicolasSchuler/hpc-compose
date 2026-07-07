use std::fs;
use std::io::{self, BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

pub(crate) fn confirm_destructive_action(action: &str, yes: bool) -> Result<()> {
    confirm_destructive_action_with_details(action, &[], yes)
}

pub(crate) fn confirm_destructive_action_with_details(
    action: &str,
    details: &[String],
    yes: bool,
) -> Result<()> {
    let mut stdin = io::stdin().lock();
    let mut stderr = io::stderr();
    confirm_destructive_action_with_io(
        action,
        details,
        yes,
        io::stdin().is_terminal(),
        &mut stdin,
        &mut stderr,
    )
}

pub(crate) fn estimate_paths_bytes(paths: &[PathBuf]) -> u64 {
    paths
        .iter()
        .map(|path| estimate_path_bytes(path))
        .fold(0_u64, u64::saturating_add)
}

fn confirm_destructive_action_with_io(
    action: &str,
    details: &[String],
    yes: bool,
    stdin_is_terminal: bool,
    input: &mut impl BufRead,
    output: &mut impl Write,
) -> Result<()> {
    if yes {
        return Ok(());
    }
    write_confirmation_details(output, details)?;
    if !stdin_is_terminal {
        bail!("{action} requires --yes when stdin is not a terminal");
    }

    write!(output, "{action}. Continue? [y/N] ").context("failed to write confirmation prompt")?;
    output
        .flush()
        .context("failed to flush confirmation prompt")?;

    let mut answer = String::new();
    input
        .read_line(&mut answer)
        .context("failed to read confirmation response")?;
    match answer.trim().to_ascii_lowercase().as_str() {
        "y" | "yes" => Ok(()),
        _ => bail!("aborted: {action}"),
    }
}

fn estimate_path_bytes(path: &Path) -> u64 {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    if !metadata.is_dir() {
        return 0;
    }
    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| estimate_path_bytes(&entry.path()))
        .fold(0_u64, u64::saturating_add)
}

fn write_confirmation_details(output: &mut impl Write, details: &[String]) -> Result<()> {
    if details.is_empty() {
        return Ok(());
    }
    writeln!(output, "destructive action preview:")
        .context("failed to write confirmation details")?;
    for detail in details {
        writeln!(output, "  - {detail}").context("failed to write confirmation details")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn yes_bypasses_confirmation() {
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();
        confirm_destructive_action_with_io(
            "delete files",
            &[],
            true,
            false,
            &mut input,
            &mut output,
        )
        .expect("--yes should bypass prompt");
        assert!(output.is_empty());
    }

    #[test]
    fn non_terminal_without_yes_is_rejected() {
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();
        let err = confirm_destructive_action_with_io(
            "delete files",
            &[],
            false,
            false,
            &mut input,
            &mut output,
        )
        .expect_err("non-tty destructive action should require --yes");
        assert!(err.to_string().contains("requires --yes"));
    }

    #[test]
    fn terminal_accepts_yes_and_rejects_default() {
        let mut input = Cursor::new(b"yes\n".to_vec());
        let mut output = Vec::new();
        confirm_destructive_action_with_io(
            "delete files",
            &[],
            false,
            true,
            &mut input,
            &mut output,
        )
        .expect("yes should accept");
        assert!(String::from_utf8(output).expect("utf8").contains("[y/N]"));

        let mut input = Cursor::new(b"\n".to_vec());
        let mut output = Vec::new();
        let err = confirm_destructive_action_with_io(
            "delete files",
            &[],
            false,
            true,
            &mut input,
            &mut output,
        )
        .expect_err("default response should abort");
        assert!(err.to_string().contains("aborted"));
    }

    #[test]
    fn details_are_printed_before_prompt_and_non_tty_rejection() {
        let details = vec!["job id: 12345".to_string(), "purge paths: 2".to_string()];
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();
        let err = confirm_destructive_action_with_io(
            "delete files",
            &details,
            false,
            false,
            &mut input,
            &mut output,
        )
        .expect_err("non-tty destructive action should require --yes");
        let output = String::from_utf8(output).expect("utf8");
        assert!(err.to_string().contains("requires --yes"));
        assert!(output.contains("destructive action preview"));
        assert!(output.contains("job id: 12345"));
        assert!(output.contains("purge paths: 2"));
    }
}
