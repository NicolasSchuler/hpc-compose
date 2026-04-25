set shell := ["bash", "-euo", "pipefail", "-c"]

_require-tools *tools:
    @missing=0; for tool in {{tools}}; do if ! command -v "$tool" >/dev/null 2>&1; then echo "missing required tool: $tool" >&2; missing=1; fi; done; exit "$missing"

_require-cargo-subcommands:
    @cargo deny --version >/dev/null 2>&1 || { echo "missing required cargo subcommand: cargo-deny" >&2; exit 1; }
    @cargo llvm-cov --version >/dev/null 2>&1 || { echo "missing required cargo subcommand: cargo-llvm-cov" >&2; exit 1; }

check:
    cargo fmt --all -- --check
    cargo clippy --all-targets --locked -- -D warnings
    cargo test --locked

docs-check: (_require-tools "mdbook" "lychee" "pa11y-ci" "curl" "python3")
    mdbook build docs
    RUSTFLAGS="-D warnings" cargo doc --locked --no-deps
    cargo run --locked --features manpage-bin --bin gen-manpages -- --check
    shopt -s globstar nullglob; lychee --no-progress --fallback-extensions md --exclude-path 'target/mdbook/404\.html$' README.md CONTRIBUTING.md SECURITY.md CODE_OF_CONDUCT.md docs/src/**/*.md target/mdbook/**/*.html
    python3 -m http.server 3000 --directory target/mdbook >/tmp/hpc-compose-docs-http.log 2>&1 & server_pid=$!; trap 'kill "$server_pid"' EXIT; for _ in $(seq 1 30); do if curl -fsS http://127.0.0.1:3000/ >/dev/null; then break; fi; sleep 1; done; pa11y-ci --config .pa11yci.json

examples-check: (_require-tools "shellcheck")
    cargo build --locked
    for f in examples/*.yaml; do echo "Validating $f"; env -u CACHE_DIR cargo run --locked -- validate -f "$f"; done
    shellcheck install.sh scripts/cluster_smoke.sh
    tmpdir="$(mktemp -d)"; trap 'rm -rf "$tmpdir"' EXIT; for f in examples/*.yaml; do echo "Shellchecking rendered $f"; out="$tmpdir/$(basename "$f" .yaml).sbatch"; env -u CACHE_DIR cargo run --locked -- render -f "$f" --output "$out"; shellcheck -e SC2034 -x -s bash "$out"; done

release-check: _require-cargo-subcommands
    cargo test --locked --test release_metadata
    cargo deny check
    cargo llvm-cov --workspace --locked --no-report
    cargo llvm-cov report --json --summary-only --locked --ignore-filename-regex 'commands/|output/mod\.rs|watch_ui\.rs|term\.rs|progress\.rs|manpages\.rs|main\.rs|cli/|job/model\.rs' --fail-under-lines 95 --fail-under-regions 93 --fail-under-functions 91

ci: check docs-check examples-check release-check
