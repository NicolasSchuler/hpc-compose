# syntax=docker/dockerfile:1
#
# A single-node Slurm cluster with hpc-compose preinstalled, for running compose
# specs against a REAL scheduler on a laptop (no fake-Slurm shim). Build context
# is the repo root so the binary is compiled for the container's Linux/arch:
#
#   docker build -f dev-cluster/Dockerfile -t hpc-compose-devcluster .
#
# See dev-cluster/README.md for the full flow and the host-backend caveat.

# --- Stage 1: build hpc-compose for the container target --------------------
# Copy only the crate's build inputs (src + compile-time include_str! roots) so
# edits to dev-cluster/ config don't bust the cargo cache layer.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples
COPY schema ./schema
RUN cargo build --release --locked --bin hpc-compose

# --- Stage 2: Slurm controller + worker + accounting + hpc-compose ----------
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
# slurm-wlm: slurmctld/slurmd/client commands. slurmdbd + mariadb-server give
# `sacct` accounting so `up`/`stats`/`score` track jobs to a terminal state.
# munge: auth. Extra tools let host-backend example services actually run.
# openssh-server + rsync make the node an SSH-reachable login-node stand-in for
# the remote-submit e2e (host -> ssh/rsync -> this node); ssh-keygen -A
# pre-generates host keys so sshd can start at boot.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      slurm-wlm slurmdbd munge mariadb-server \
      ca-certificates procps iproute2 tini \
      python3 curl redis-server socat \
      openssh-server rsync \
 && rm -rf /var/lib/apt/lists/* \
 && ssh-keygen -A

COPY --from=build /src/target/release/hpc-compose /usr/local/bin/hpc-compose
COPY dev-cluster/slurm.conf.tmpl /etc/slurm/slurm.conf.tmpl
COPY dev-cluster/cgroup.conf /etc/slurm/cgroup.conf
# slurmdbd refuses a world-readable config; the entrypoint installs this to
# /etc/slurm/slurmdbd.conf with 0600 perms at boot.
COPY dev-cluster/slurmdbd.conf /etc/slurm/slurmdbd.conf.in
COPY dev-cluster/entrypoint.sh /usr/local/bin/devcluster-entrypoint.sh
# OTP/2FA login simulation toggle for the SSH login-node stand-in, used by the
# one-OTP ControlMaster e2e (scripts/devcluster_otp_e2e.sh). Inert until enabled.
COPY dev-cluster/otp-sim.sh /usr/local/bin/otp-sim
RUN chmod +x /usr/local/bin/devcluster-entrypoint.sh /usr/local/bin/otp-sim

# tini reaps the daemons started by the entrypoint.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/devcluster-entrypoint.sh"]
