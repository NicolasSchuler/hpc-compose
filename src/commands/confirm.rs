use std::io::{self, BufRead, IsTerminal, Write};

use anyhow::{Context, Result, bail};

pub(crate) fn confirm_destructive_action(action: &str, yes: bool) -> Result<()> {
    let mut stdin = io::stdin().lock();
    let mut stderr = io::stderr();
    confirm_destructive_action_with_io(
        action,
        yes,
        io::stdin().is_terminal(),
        &mut stdin,
        &mut stderr,
    )
}

fn confirm_destructive_action_with_io(
    action: &str,
    yes: bool,
    stdin_is_terminal: bool,
    input: &mut impl BufRead,
    output: &mut impl Write,
) -> Result<()> {
    if yes {
        return Ok(());
    }
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn yes_bypasses_confirmation() {
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();
        confirm_destructive_action_with_io("delete files", true, false, &mut input, &mut output)
            .expect("--yes should bypass prompt");
        assert!(output.is_empty());
    }

    #[test]
    fn non_terminal_without_yes_is_rejected() {
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();
        let err = confirm_destructive_action_with_io(
            "delete files",
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
        confirm_destructive_action_with_io("delete files", false, true, &mut input, &mut output)
            .expect("yes should accept");
        assert!(String::from_utf8(output).expect("utf8").contains("[y/N]"));

        let mut input = Cursor::new(b"\n".to_vec());
        let mut output = Vec::new();
        let err = confirm_destructive_action_with_io(
            "delete files",
            false,
            true,
            &mut input,
            &mut output,
        )
        .expect_err("default response should abort");
        assert!(err.to_string().contains("aborted"));
    }
}
