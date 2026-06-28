//! Shared SSH connection-multiplexing helpers for the laptop-facing commands
//! (`reach`, `pull`, `experiment`). Emitting identical ControlMaster options
//! everywhere keeps a per-session OTP/2FA prompt down to a single
//! authentication that is then reused across tunnels and transfers.

/// ssh options that establish (and reuse) one master connection, so a login
/// node requiring an OTP authenticates once within `ControlPersist`. Kept as an
/// array so it can be spread into `Command` args verbatim.
pub(crate) const CONTROL_MASTER_SSH_OPTS: [&str; 6] = [
    "-o",
    "ControlMaster=auto",
    "-o",
    "ControlPath=~/.ssh/cm-%r@%h:%p",
    "-o",
    "ControlPersist=10m",
];

/// ssh options that neutralize an interactive Host alias for non-interactive
/// automation. A `~/.ssh/config` block tailored for human use (e.g.
/// `RemoteCommand tmux new -A ...` + `RequestTTY yes`) otherwise hijacks the
/// commands `up --remote` runs (`mkdir`, the version probe, the delegated `up`):
/// OpenSSH refuses with "Cannot execute command-line and remote command" when a
/// config `RemoteCommand` collides with a command on the line. Forcing
/// `RemoteCommand=none` / `RequestTTY=no` makes delegation work through such an
/// alias instead of failing cryptically.
pub(crate) const NONINTERACTIVE_SSH_OPTS: [&str; 4] =
    ["-o", "RemoteCommand=none", "-o", "RequestTTY=no"];

/// One-line explanation shown beneath an emitted ssh/rsync command.
pub(crate) const OTP_MULTIPLEX_NOTE: &str = "The ControlMaster options reuse one authenticated connection, so a login node that requires \
     an OTP/2FA only prompts on the first connection within ControlPersist.";

/// The ControlMaster options as a single space-joined string, for embedding in
/// a printed `ssh`/`rsync -e` command.
pub(crate) fn control_master_opts_str() -> String {
    CONTROL_MASTER_SSH_OPTS.join(" ")
}

/// Renders an `ssh -N -L` port-forward command with connection multiplexing.
pub(crate) fn ssh_forward_command(
    local_port: u16,
    remote_port: u16,
    compute: &str,
    login: &str,
) -> String {
    format!(
        "ssh -N {opts} -L {local_port}:{compute}:{remote_port} {login}",
        opts = control_master_opts_str(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssh_forward_command_includes_multiplexing_and_forward() {
        let command = ssh_forward_command(8000, 8000, "gpu042", "login01");
        assert!(command.starts_with("ssh -N "));
        assert!(command.contains("ControlMaster=auto"));
        assert!(command.contains("ControlPersist=10m"));
        assert!(command.ends_with("-L 8000:gpu042:8000 login01"));
    }

    #[test]
    fn control_master_opts_str_joins_the_array() {
        assert_eq!(
            control_master_opts_str(),
            "-o ControlMaster=auto -o ControlPath=~/.ssh/cm-%r@%h:%p -o ControlPersist=10m"
        );
    }
}
