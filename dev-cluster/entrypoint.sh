#!/usr/bin/env bash
# Boot a single-node Slurm cluster (controller + worker in one container) for
# local hpc-compose development, then stay alive so callers can `exec` into it.
#
# The boot path is covered by scripts/devcluster_e2e.sh and the CI
# dev-cluster-e2e job; see dev-cluster/README.md for scope and caveats.
set -euo pipefail

log() { printf '[devcluster] %s\n' "$*"; }

# --- 1. munge authentication ------------------------------------------------
# munged needs a shared key (0400, owned by the munge user) and its runtime dirs.
if [[ ! -s /etc/munge/munge.key ]]; then
  log "generating munge key"
  dd if=/dev/urandom bs=1 count=1024 of=/etc/munge/munge.key status=none
fi
chown munge:munge /etc/munge/munge.key
chmod 400 /etc/munge/munge.key
install -d -o munge -g munge -m 0755 /run/munge /var/lib/munge /var/log/munge
log "starting munged"
runuser -u munge -- /usr/sbin/munged

# --- 2. auto-detect node resources -----------------------------------------
# Advertising the container's real CPUs/RAM (minus a small margin) guarantees
# the node registers as `idle` regardless of how large the engine VM is.
cpus="$(nproc)"
mem_total_mb="$(( $(awk '/MemTotal/ {print $2}' /proc/meminfo) / 1024 ))"
if (( mem_total_mb > 1024 )); then
  mem="$(( mem_total_mb - 512 ))"
else
  mem=512
fi
log "node resources: CPUs=${cpus} RealMemory=${mem}MB"

sed -e "s/@CPUS@/${cpus}/g" -e "s/@MEM@/${mem}/g" \
  /etc/slurm/slurm.conf.tmpl > /etc/slurm/slurm.conf

# --- 3. spool / log dirs ----------------------------------------------------
install -d -m 0755 /var/spool/slurmctld /var/spool/slurmd /var/log/slurm
# Host-backend dev runs need the cache dir to exist and be writable.
install -d -m 0777 "${CACHE_DIR:-/var/cache/hpc-compose}"
# slurmd creates its cgroup scope under /sys/fs/cgroup/system.slice but does not
# mkdir -p the parent; rootless engines don't pre-create system.slice the way
# systemd does. Create it when the cgroup fs is writable (privileged container).
if [[ -w /sys/fs/cgroup ]]; then
  mkdir -p /sys/fs/cgroup/system.slice 2>/dev/null || true
fi
# Make the container's own hostname resolvable to itself for SlurmctldHost.
if ! grep -q 'hpc-compose-dev' /etc/hosts; then
  echo "127.0.0.1 hpc-compose-dev" >> /etc/hosts
fi

# --- 4. accounting: mariadb + slurmdbd --------------------------------------
# Gives `sacct` real data so `up`/`status`/`stats`/`score` track jobs to a
# terminal state instead of degrading to "unknown".
install -d -o mysql -g mysql -m 0755 /var/lib/mysql /run/mysqld
if [[ ! -d /var/lib/mysql/mysql ]]; then
  log "initializing mariadb data dir"
  mariadb-install-db --user=mysql --datadir=/var/lib/mysql \
    --auth-root-authentication-method=normal >/dev/null 2>&1 || true
fi
log "starting mariadb"
mariadbd --user=mysql --datadir=/var/lib/mysql \
  --socket=/run/mysqld/mysqld.sock >/var/log/slurm/mariadb.log 2>&1 &
for _ in $(seq 1 60); do
  mariadb-admin --socket=/run/mysqld/mysqld.sock ping >/dev/null 2>&1 && break
  sleep 0.5
done
log "provisioning slurm accounting database"
mariadb --socket=/run/mysqld/mysqld.sock <<'SQL' || true
CREATE DATABASE IF NOT EXISTS slurm_acct_db;
CREATE USER IF NOT EXISTS 'slurm'@'%' IDENTIFIED BY 'slurm';
GRANT ALL ON slurm_acct_db.* TO 'slurm'@'%';
FLUSH PRIVILEGES;
SQL
install -m 0600 -o root -g root /etc/slurm/slurmdbd.conf.in /etc/slurm/slurmdbd.conf
log "starting slurmdbd"
if ! slurmdbd; then
  log "slurmdbd failed to start; recent log:"
  tail -n 30 /var/log/slurm/slurmdbd.log 2>/dev/null || true
  exit 1
fi
for _ in $(seq 1 30); do
  sacctmgr -i show cluster >/dev/null 2>&1 && break
  sleep 0.5
done
# Register the cluster with the dbd so accounting rows are attributed.
sacctmgr -i add cluster hpc-compose-dev >/dev/null 2>&1 || true

# --- 5. daemons -------------------------------------------------------------
# slurmctld/slurmd daemonize and return; surface their log on a startup failure
# so `podman logs` shows the cause instead of the container silently exiting.
log "starting slurmctld"
if ! slurmctld; then
  log "slurmctld failed to start; recent log:"
  tail -n 30 /var/log/slurm/slurmctld.log 2>/dev/null || true
  exit 1
fi
# Give the controller a moment to open its listening socket before slurmd joins.
for _ in $(seq 1 20); do
  scontrol ping >/dev/null 2>&1 && break
  sleep 0.5
done
log "starting slurmd"
if ! slurmd; then
  log "slurmd failed to start; recent log:"
  tail -n 30 /var/log/slurm/slurmd.log 2>/dev/null || true
  exit 1
fi

# Resume the node in case it registered in an UNKNOWN/down state.
sleep 1
scontrol update nodename=hpc-compose-dev state=resume 2>/dev/null || true
sinfo || true

# --- 5b. sshd: SSH-reachable login-node stand-in for the remote-submit e2e ---
# Key-only root login. No credentials are baked into the image; the remote e2e
# harness injects its ephemeral public key into /root/.ssh/authorized_keys after
# boot. Harmless when unused (the default exec-based harness never connects).
install -d -m 0755 /run/sshd
install -d -m 0700 /root/.ssh
mkdir -p /etc/ssh/sshd_config.d
cat >/etc/ssh/sshd_config.d/devcluster.conf <<'SSHD'
PermitRootLogin prohibit-password
PubkeyAuthentication yes
PasswordAuthentication no
SSHD
log "starting sshd"
/usr/sbin/sshd || log "sshd failed to start (remote-submit e2e unavailable)"

log "cluster ready — exec hpc-compose against it, e.g.:"
log "  hpc-compose up -f <compose.yaml>"

# --- 6. stay alive ----------------------------------------------------------
exec tail -F /var/log/slurm/slurmctld.log /var/log/slurm/slurmd.log
