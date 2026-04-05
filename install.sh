#!/bin/sh

set -eu

REPO="${HPC_COMPOSE_REPO:-NicolasSchuler/hpc-compose}"
INSTALL_DIR="${HPC_COMPOSE_INSTALL_DIR:-${HOME}/.local/bin}"
BINARY_NAME="hpc-compose"

usage() {
  cat <<'EOF'
Install the latest hpc-compose release for the current platform.

Environment overrides:
  HPC_COMPOSE_INSTALL_DIR  Install destination (default: ~/.local/bin)
  HPC_COMPOSE_VERSION      Release tag to install, for example v0.1.11
  HPC_COMPOSE_REPO         Alternate GitHub repo in owner/name form
EOF
}

log() {
  printf '%s\n' "$*" >&2
}

fail() {
  log "error: $*"
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

detect_os() {
  if [ -n "${HPC_COMPOSE_OS:-}" ]; then
    printf '%s' "${HPC_COMPOSE_OS}"
  else
    uname -s
  fi
}

detect_arch() {
  if [ -n "${HPC_COMPOSE_ARCH:-}" ]; then
    printf '%s' "${HPC_COMPOSE_ARCH}"
  else
    uname -m
  fi
}

resolve_target() {
  os="$(detect_os)"
  arch="$(detect_arch)"

  case "${os}:${arch}" in
    Linux:x86_64 | Linux:amd64)
      printf '%s' 'x86_64-unknown-linux-musl'
      ;;
    Linux:aarch64 | Linux:arm64)
      printf '%s' 'aarch64-unknown-linux-musl'
      ;;
    Darwin:x86_64)
      printf '%s' 'x86_64-apple-darwin'
      ;;
    Darwin:arm64 | Darwin:aarch64)
      printf '%s' 'aarch64-apple-darwin'
      ;;
    *)
      fail "unsupported platform: ${os} ${arch}"
      ;;
  esac
}

resolve_version() {
  if [ -n "${HPC_COMPOSE_VERSION:-}" ]; then
    printf '%s' "${HPC_COMPOSE_VERSION}"
    return 0
  fi

  latest_url="https://github.com/${REPO}/releases/latest"
  latest_location="$(curl -fsSIL -o /dev/null -w '%{url_effective}' "${latest_url}")" \
    || fail "failed to resolve the latest release tag"
  latest_tag="${latest_location##*/}"

  [ -n "${latest_tag}" ] || fail "could not determine the latest release tag"
  printf '%s' "${latest_tag}"
}

verify_checksum() {
  archive_path="$1"
  checksum_path="$2"

  if command -v shasum >/dev/null 2>&1; then
    (
      cd "${TMP_WORKDIR}"
      shasum -a 256 -c "$(basename "${checksum_path}")"
    ) >/dev/null
    return 0
  fi

  if command -v sha256sum >/dev/null 2>&1; then
    (
      cd "${TMP_WORKDIR}"
      sha256sum -c "$(basename "${checksum_path}")"
    ) >/dev/null
    return 0
  fi

  if command -v openssl >/dev/null 2>&1; then
    expected="$(cut -d ' ' -f 1 "${checksum_path}")"
    actual="$(openssl dgst -sha256 "${archive_path}" | sed 's/^.*= //')"
    [ "${expected}" = "${actual}" ] || fail "checksum verification failed"
    return 0
  fi

  fail "no checksum tool available; need shasum, sha256sum, or openssl"
}

main() {
  case "${1:-}" in
    "" ) ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      fail "unexpected argument: $1"
      ;;
  esac

  need_cmd curl
  need_cmd tar
  need_cmd install
  need_cmd mktemp

  target="$(resolve_target)"
  version="$(resolve_version)"
  asset="${BINARY_NAME}-${version}-${target}.tar.gz"
  asset_url="https://github.com/${REPO}/releases/download/${version}/${asset}"
  checksum_url="${asset_url}.sha256"

  tmp_base="${TMPDIR:-/tmp}"
  TMP_WORKDIR="$(mktemp -d "${tmp_base%/}/hpc-compose-install.XXXXXX")"
  trap 'rm -rf "${TMP_WORKDIR}"' EXIT INT TERM HUP

  archive_path="${TMP_WORKDIR}/${asset}"
  checksum_path="${archive_path}.sha256"

  log "Downloading ${asset}"
  curl -fsSL "${asset_url}" -o "${archive_path}" || fail "failed to download ${asset_url}"
  curl -fsSL "${checksum_url}" -o "${checksum_path}" || fail "failed to download ${checksum_url}"
  verify_checksum "${archive_path}" "${checksum_path}"

  mkdir -p "${INSTALL_DIR}"
  tar -xzf "${archive_path}" -C "${TMP_WORKDIR}"
  install -m 0755 "${TMP_WORKDIR}/${BINARY_NAME}" "${INSTALL_DIR}/${BINARY_NAME}"

  log "Installed ${BINARY_NAME} ${version} to ${INSTALL_DIR}/${BINARY_NAME}"
  case ":${PATH:-}:" in
    *:"${INSTALL_DIR}":*) ;;
    *)
      log "Add ${INSTALL_DIR} to PATH if it is not already there."
      ;;
  esac
}

main "$@"
