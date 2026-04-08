#!/bin/sh

set -eu

REPO="${HPC_COMPOSE_REPO:-NicolasSchuler/hpc-compose}"
INSTALL_DIR="${HPC_COMPOSE_INSTALL_DIR:-${HOME}/.local/bin}"
BINARY_NAME="hpc-compose"
BASE_URL="${HPC_COMPOSE_BASE_URL:-}"

usage() {
  cat <<'EOF'
Install the latest hpc-compose release for the current platform.

Environment overrides:
  HPC_COMPOSE_INSTALL_DIR  Install destination (default: ~/.local/bin)
  HPC_COMPOSE_MAN_DIR      Manpage root (default: <prefix>/share/man when install dir ends in /bin, otherwise ~/.local/share/man)
  HPC_COMPOSE_VERSION      Release tag to install, for example v0.1.12
  HPC_COMPOSE_REPO         Alternate GitHub repo in owner/name form
  HPC_COMPOSE_BASE_URL     Alternate base URL serving release assets directly
EOF
}

log() {
  printf '%s\n' "$*" >&2
}

warn() {
  log "warning: $*"
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
  if [ -n "${BASE_URL}" ] && [ -z "${HPC_COMPOSE_VERSION:-}" ]; then
    fail "HPC_COMPOSE_VERSION is required when HPC_COMPOSE_BASE_URL is set"
  fi

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

  expected="$(cut -d ' ' -f 1 "${checksum_path}")"
  [ -n "${expected}" ] || fail "checksum file is empty: ${checksum_path}"

  if command -v shasum >/dev/null 2>&1; then
    actual="$(shasum -a 256 "${archive_path}" | cut -d ' ' -f 1)"
  elif command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "${archive_path}" | cut -d ' ' -f 1)"
  elif command -v openssl >/dev/null 2>&1; then
    actual="$(openssl dgst -sha256 "${archive_path}" | sed 's/^.*= //')"
  else
    fail "no checksum tool available; need shasum, sha256sum, or openssl"
  fi

  expected_lc="$(printf '%s' "${expected}" | tr 'A-F' 'a-f')"
  actual_lc="$(printf '%s' "${actual}" | tr 'A-F' 'a-f')"
  [ "${expected_lc}" = "${actual_lc}" ] || fail "checksum verification failed"
}

resolve_man_dir() {
  if [ -n "${HPC_COMPOSE_MAN_DIR:-}" ]; then
    printf '%s' "${HPC_COMPOSE_MAN_DIR}"
    return 0
  fi

  install_dir="${INSTALL_DIR%/}"
  if [ "$(basename "${install_dir}")" = "bin" ]; then
    printf '%s' "$(dirname "${install_dir}")/share/man"
  else
    printf '%s' "${HOME}/.local/share/man"
  fi
}

install_manpages() {
  src_dir="${TMP_WORKDIR}/share/man/man1"
  [ -d "${src_dir}" ] || return 0

  man_dir="$(resolve_man_dir)"
  if ! mkdir -p "${man_dir}/man1"; then
    warn "failed to create ${man_dir}/man1; skipping manpage installation"
    return 0
  fi
  installed=0
  for page in "${src_dir}"/*.1; do
    [ -f "${page}" ] || continue
    if ! install -m 0644 "${page}" "${man_dir}/man1/$(basename "${page}")"; then
      warn "failed to install manpages into ${man_dir}/man1; leaving the binary installed"
      return 0
    fi
    installed=1
  done

  if [ "${installed}" -eq 1 ]; then
    log "Installed manpages to ${man_dir}/man1"
  fi
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
  if [ -n "${BASE_URL}" ]; then
    asset_url="${BASE_URL%/}/${asset}"
  else
    asset_url="https://github.com/${REPO}/releases/download/${version}/${asset}"
  fi
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
  install_manpages

  log "Installed ${BINARY_NAME} ${version} to ${INSTALL_DIR}/${BINARY_NAME}"
  case ":${PATH:-}:" in
    *:"${INSTALL_DIR}":*) ;;
    *)
      log "Add ${INSTALL_DIR} to PATH if it is not already there."
      ;;
  esac
}

main "$@"
