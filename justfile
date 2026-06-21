set shell := ["bash", "-euo", "pipefail", "-c"]

MDBOOK_VERSION := "0.5.2"
LYCHEE_VERSION := "0.23.0"
PA11Y_CI_VERSION := "4.0.1"
ACTIONLINT_VERSION := "1.7.12"
TYPOS_VERSION := "1.28.4"
MARKDOWNLINT_CLI2_VERSION := "0.14.0"

_require-tools *tools:
    @missing=0; for tool in {{tools}}; do if ! command -v "$tool" >/dev/null 2>&1; then echo "missing required tool: $tool" >&2; missing=1; fi; done; exit "$missing"

_require-cargo-subcommands:
    @cargo deny --version >/dev/null 2>&1 || { echo "missing required cargo subcommand: cargo-deny" >&2; exit 1; }
    @cargo llvm-cov --version >/dev/null 2>&1 || { echo "missing required cargo subcommand: cargo-llvm-cov" >&2; exit 1; }

bootstrap-docs-tools: (_require-tools "cargo" "npm")
    cargo install mdbook --locked --version "{{MDBOOK_VERSION}}"
    cargo install lychee --locked --version "{{LYCHEE_VERSION}}"
    cargo install typos-cli --locked --version "{{TYPOS_VERSION}}"
    npm install --global "pa11y-ci@{{PA11Y_CI_VERSION}}" "markdownlint-cli2@{{MARKDOWNLINT_CLI2_VERSION}}"

clean:
    rm -rf target .tmp coverage htmlcov tarpaulin-report.html lcov.info *.profraw *.profdata

# Trim stale build artifacts without a full `cargo clean` (keeps the warm debug cache).
# Drops the duplicate coverage/mutants target trees unconditionally (a `cargo llvm-cov`
# run rebuilds the whole workspace into target/llvm-cov-target), then sweeps main-tree
# artifacts untouched for >N days via cargo-sweep when it is installed.
cache-sweep days="3":
    rm -rf target/llvm-cov-target target/mutants
    @if command -v cargo-sweep >/dev/null 2>&1; then \
        cargo sweep --time {{days}}; \
    else \
        echo "cargo-sweep not installed; removed duplicate trees only."; \
        echo "Install with: cargo install cargo-sweep"; \
    fi

# Drop artifacts not built by a currently-installed toolchain. Run after `rustup update`,
# which orphans the entire previous toolchain's artifacts that `--time` alone won't catch.
cache-sweep-installed:
    @if command -v cargo-sweep >/dev/null 2>&1; then \
        cargo sweep --installed; \
    else \
        echo "cargo-sweep not installed. Install with: cargo install cargo-sweep"; \
    fi

workflow-check: (_require-tools "actionlint")
    actionlint -color

check: workflow-check
    cargo fmt --all -- --check
    cargo clippy --all-targets --locked -- -D warnings
    cargo test --locked

docs-check: (_require-tools "mdbook" "lychee" "pa11y-ci" "typos" "markdownlint-cli2" "curl" "python3")
    mdbook build docs
    RUSTFLAGS="-D warnings" cargo doc --locked --no-deps
    cargo run --locked --features manpage-bin --bin gen-manpages -- --check
    typos docs/src README.md CONTRIBUTING.md SECURITY.md CODE_OF_CONDUCT.md
    markdownlint-cli2
    shopt -s globstar nullglob; lychee --no-progress --fallback-extensions md --exclude '^https://github\.com/NicolasSchuler/hpc-compose/edit/main/' --exclude-path 'target/mdbook/404\.html$' README.md CONTRIBUTING.md SECURITY.md CODE_OF_CONDUCT.md docs/src/**/*.md target/mdbook/**/*.html
    python3 scripts/gen_pa11y_urls.py
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
    cargo llvm-cov report --json --summary-only --locked --ignore-filename-regex '(^|/)commands/mod\.rs$|(^|/)cli/commands\.rs$|(^|/)main\.rs$|(^|/)job/model\.rs$' --fail-under-lines 87 --fail-under-regions 87 --fail-under-functions 85

# `release-check` runs `cargo llvm-cov`, which leaves a full duplicate workspace build in
# target/llvm-cov-target; sweep it (and prune >3d main-tree artifacts) once the suite passes.
ci: check docs-check examples-check release-check cache-sweep
