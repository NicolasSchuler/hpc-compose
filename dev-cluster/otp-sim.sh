#!/usr/bin/env bash
# OTP/2FA login simulation for the dev-cluster SSH login-node stand-in.
#
# Real cluster login nodes require a one-time passcode (OTP/2FA) per SSH session.
# hpc-compose copes by reusing a single authenticated connection via SSH
# ControlMaster multiplexing (see src/commands/runtime/ssh_hint.rs), so a whole
# laptop session prompts once. The default dev-cluster sshd is key-only, which
# can't exercise that property. This toggle makes the stand-in require an
# interactive (OTP-style) second factor and counts how often it actually fires,
# so a harness can prove a multi-command session authenticates exactly once.
#
#   otp-sim enable    require publickey + keyboard-interactive (OTP); reset count
#   otp-sim disable   restore the default key-only sshd
#   otp-sim reset     zero the OTP-prompt counter
#   otp-sim count     print how many OTP/2FA authentications have occurred
#
# How the count works: with AuthenticationMethods requiring keyboard-interactive,
# every genuine SSH authentication runs the PAM auth stack once. A pam_exec hook
# in that stack bumps a counter file. Connections multiplexed over an existing
# ControlMaster socket open a new channel WITHOUT re-authenticating, so they never
# bump the counter -- which is exactly the one-OTP-per-session property. The
# pam_permit that follows satisfies the step with no typed response, so the client
# stays non-interactive (zero prompts) while still exercising a real second factor.
set -euo pipefail

DROPIN=/etc/ssh/sshd_config.d/00-otp-sim.conf
PAM=/etc/pam.d/sshd
PAM_BAK=/etc/pam.d/sshd.otp-sim-bak
COUNTER=/usr/local/bin/otp-sim-prompt.sh
LOG=/var/log/otp-prompts.log

restart_sshd() {
  pkill -x sshd 2>/dev/null || true
  # Wait for the listener to exit so the fresh daemon can bind the port.
  for _ in $(seq 1 20); do
    pgrep -x sshd >/dev/null 2>&1 || break
    sleep 0.3
  done
  /usr/sbin/sshd
}

case "${1:-}" in
  enable)
    cat > "$COUNTER" <<'EOS'
#!/bin/sh
# Bumped once per genuine SSH authentication (the OTP/2FA prompt).
echo "otp-auth user=${PAM_USER:-?} rhost=${PAM_RHOST:-?}" >> /var/log/otp-prompts.log
exit 0
EOS
    chmod 755 "$COUNTER"
    # Preserve the distro PAM sshd stack once so `disable` restores it faithfully,
    # then swap in a counter + permit stack for the keyboard-interactive step.
    [ -f "$PAM_BAK" ] || cp "$PAM" "$PAM_BAK"
    cat > "$PAM" <<EOP
auth    required    pam_exec.so $COUNTER
auth    required    pam_permit.so
account required    pam_permit.so
session required    pam_permit.so
EOP
    # Require BOTH a key AND the interactive step. PermitRootLogin must be `yes`
    # (prohibit-password disables keyboard-interactive for root). This drop-in
    # sorts before devcluster.conf, so its values win (first match in sshd_config).
    cat > "$DROPIN" <<'EOC'
PermitRootLogin yes
PubkeyAuthentication yes
PasswordAuthentication no
KbdInteractiveAuthentication yes
UsePAM yes
AuthenticationMethods publickey,keyboard-interactive
EOC
    : > "$LOG"
    restart_sshd
    echo "otp-sim: enabled (publickey + keyboard-interactive; counter reset)"
    ;;
  disable)
    rm -f "$DROPIN" "$COUNTER" "$LOG"
    [ -f "$PAM_BAK" ] && mv -f "$PAM_BAK" "$PAM"
    restart_sshd
    echo "otp-sim: disabled (key-only sshd restored)"
    ;;
  reset)
    : > "$LOG"
    ;;
  count)
    if [ -f "$LOG" ]; then wc -l < "$LOG" | tr -d ' '; else echo 0; fi
    ;;
  *)
    echo "usage: otp-sim {enable|disable|reset|count}" >&2
    exit 2
    ;;
esac
