#!/usr/bin/env python3
"""Run a command under a fresh pseudo-terminal, capture its output, and exit with
the command's status.

The dev-cluster e2e uses this to drive the crossterm `watch` TUI non-interactively:
`watch` only renders its alternate-screen UI on a TTY, so a plain `podman exec`
(no -t) would silently fall back to line mode. Allocating a PTY here makes the
child see a real terminal, so the harness can prove the TUI entered AND restored
the alternate screen.

Usage: pty-run.py [--timeout SECONDS] [--out FILE] -- CMD [ARGS...]
Captured bytes go to --out (default: stdout, written raw). Exit code mirrors the
child; 124 on timeout (matching coreutils `timeout`), 127 if CMD is not found.
"""
import argparse
import fcntl
import os
import pty
import select
import signal
import struct
import sys
import termios
import time


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--out", default="-")
    parser.add_argument("--rows", type=int, default=40)
    parser.add_argument("--cols", type=int, default=120)
    parser.add_argument("cmd", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    cmd = args.cmd[1:] if args.cmd and args.cmd[0] == "--" else args.cmd
    if not cmd:
        print("pty-run: no command given", file=sys.stderr)
        return 2

    # pty.fork() gives the child a controlling terminal (stdin/stdout/stderr all
    # wired to the new pty slave), which is exactly what crossterm needs.
    pid, master = pty.fork()
    if pid == 0:
        try:
            os.execvp(cmd[0], cmd)
        except FileNotFoundError:
            os._exit(127)
        except OSError:
            os._exit(126)

    # Give the pty a real window size; a default 0x0 makes a TUI render nothing.
    try:
        winsize = struct.pack("HHHH", args.rows, args.cols, 0, 0)
        fcntl.ioctl(master, termios.TIOCSWINSZ, winsize)
    except OSError:
        pass

    captured = bytearray()
    deadline = time.monotonic() + args.timeout
    status = 0
    timed_out = False
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            timed_out = True
            break
        readable, _, _ = select.select([master], [], [], min(remaining, 1.0))
        if master in readable:
            try:
                chunk = os.read(master, 65536)
            except OSError:
                chunk = b""
            if not chunk:
                break
            captured.extend(chunk)
        waited_pid, status = os.waitpid(pid, os.WNOHANG)
        if waited_pid == pid:
            # Child exited; drain anything still buffered in the pty.
            try:
                while True:
                    chunk = os.read(master, 65536)
                    if not chunk:
                        break
                    captured.extend(chunk)
            except OSError:
                pass
            break

    if timed_out:
        try:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
        except (ProcessLookupError, ChildProcessError):
            pass

    sink = sys.stdout.buffer if args.out == "-" else open(args.out, "wb")
    try:
        sink.write(bytes(captured))
        sink.flush()
    finally:
        if sink is not sys.stdout.buffer:
            sink.close()

    if timed_out:
        return 124
    if os.WIFEXITED(status):
        return os.WEXITSTATUS(status)
    if os.WIFSIGNALED(status):
        return 128 + os.WTERMSIG(status)
    return 1


if __name__ == "__main__":
    sys.exit(main())
