#!/usr/bin/env python3
"""Generate hpc-compose's agent policy, safety docs, and published LLM assets."""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import quote, urlparse


ROOT = Path(__file__).resolve().parents[1]
POLICY_SOURCE = ROOT / "agent-command-policy.toml"
POLICY_JSON = ROOT / "agent-command-policy.json"
DOCS_POLICY_JSON = ROOT / "docs/src/agent-command-policy.json"
HUMAN_SAFETY = ROOT / "docs/src/agent-command-safety.md"
SKILL_SAFETY = ROOT / "skills/hpc-compose/references/command-safety.md"
SKILL_VERSION = ROOT / "skills/hpc-compose/VERSION"
LLMS_INDEX = ROOT / "llms.txt"
DOCS_SOURCE = ROOT / "docs/src"
MAX_LLMS_BYTES = 12 * 1024
MAX_ESSENTIAL_CONTEXT_BYTES = 320 * 1024
MAX_FULL_CONTEXT_BYTES = 400 * 1024
INCLUDE_RE = re.compile(r"\{\{#include\s+([^}\s]+)\s*\}\}")
LINK_RE = re.compile(r"\[[^\]]+\]\(([^)\s]+)\)")


class GenerationError(RuntimeError):
    """Raised when a checked-in input or generated artifact is invalid."""


def load_toml(path: Path) -> dict[str, object]:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise GenerationError(f"cannot read {path.relative_to(ROOT)}: {error}") from error


def crate_version() -> str:
    manifest = load_toml(ROOT / "Cargo.toml")
    package = manifest.get("package")
    if not isinstance(package, dict) or not isinstance(package.get("version"), str):
        raise GenerationError("Cargo.toml must define [package].version")
    return package["version"]


def string_list(value: object, context: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise GenerationError(f"{context} must be an array of strings")
    return value


def validate_policy(policy: dict[str, object]) -> None:
    if policy.get("schema_version") != 1:
        raise GenerationError("agent-command-policy.toml schema_version must be 1")
    if not isinstance(policy.get("policy_version"), str) or not re.fullmatch(
        r"[0-9]+\.[0-9]+\.[0-9]+", policy["policy_version"]
    ):
        raise GenerationError("policy_version must be a semantic X.Y.Z version")
    for key in ("title", "description"):
        if not isinstance(policy.get(key), str) or not policy[key].strip():
            raise GenerationError(f"policy {key} must be a non-empty string")

    effects = policy.get("effect_tags")
    tiers = policy.get("authorization_tiers")
    commands = policy.get("commands")
    if not isinstance(effects, dict) or not effects:
        raise GenerationError("policy must define [effect_tags]")
    if not isinstance(tiers, dict) or not tiers:
        raise GenerationError("policy must define [authorization_tiers]")
    if not isinstance(commands, list) or not commands:
        raise GenerationError("policy must define [[commands]]")

    known_effects = set(effects)
    expected_effects = {
        "local-read",
        "local-write",
        "local-delete",
        "executes-user-code",
        "network-or-ssh",
        "scheduler-read",
        "scheduler-submit",
        "scheduler-cancel",
        "polls",
        "sensitive-output",
    }
    if known_effects != expected_effects:
        raise GenerationError(
            f"effect tags differ from the contract: {sorted(known_effects ^ expected_effects)}"
        )

    expected_tiers = {
        "automatic-read-only": 1,
        "scoped-local-mutation": 2,
        "explicit-runtime-or-external-mutation": 3,
        "explicit-quota": 4,
        "explicit-destructive": 5,
    }
    if {name: tier.get("order") for name, tier in tiers.items() if isinstance(tier, dict)} != expected_tiers:
        raise GenerationError("authorization tiers differ from the ordered contract")

    orders: list[int] = []
    for name, tier in tiers.items():
        if not isinstance(tier, dict) or not isinstance(tier.get("order"), int):
            raise GenerationError(f"authorization tier {name!r} must define an integer order")
        orders.append(tier["order"])
    if sorted(orders) != list(range(1, len(orders) + 1)):
        raise GenerationError("authorization tier orders must be consecutive starting at 1")

    paths: list[str] = []
    for command in commands:
        if not isinstance(command, dict):
            raise GenerationError("each [[commands]] entry must be a table")
        path = command.get("path")
        tier = command.get("authorization_tier")
        if not isinstance(path, str) or not path or path.strip() != path:
            raise GenerationError("each command path must be a non-empty normalized string")
        if tier not in tiers:
            raise GenerationError(f"command {path!r} uses unknown tier {tier!r}")
        validate_effects(command.get("effects"), known_effects, f"command {path!r}")
        validate_overrides(command.get("overrides", []), tiers, known_effects, path)
        paths.append(path)

    if paths != sorted(paths):
        raise GenerationError("command policy entries must be sorted by path")
    duplicates = sorted({path for path in paths if paths.count(path) > 1})
    if duplicates:
        raise GenerationError(f"duplicate command policy paths: {duplicates}")

    validate_overrides(
        policy.get("global_overrides", []), tiers, known_effects, "global", global_scope=True
    )


def validate_effects(value: object, known: set[str], context: str) -> None:
    effects = string_list(value, f"{context}.effects")
    unknown = set(effects) - known
    if unknown:
        raise GenerationError(f"{context} uses unknown effects: {sorted(unknown)}")
    if len(effects) != len(set(effects)):
        raise GenerationError(f"{context}.effects contains duplicates")


def validate_overrides(
    value: object,
    tiers: dict[str, object],
    known_effects: set[str],
    context: str,
    *,
    global_scope: bool = False,
) -> None:
    if not isinstance(value, list):
        raise GenerationError(f"{context}.overrides must be an array")
    conditions: list[tuple[str, ...]] = []
    for override in value:
        if not isinstance(override, dict):
            raise GenerationError(f"{context} override must be a table")
        if global_scope:
            flags = [override.get("flag")]
        else:
            flags = string_list(override.get("flags"), f"{context} override flags")
        if not flags or not all(isinstance(flag, str) and flag.startswith("--") for flag in flags):
            raise GenerationError(f"{context} override flags must be long flags")
        normalized_flags = tuple(flags)
        if len(normalized_flags) != len(set(normalized_flags)):
            raise GenerationError(f"{context} override flags contain duplicates")
        if normalized_flags in conditions:
            raise GenerationError(f"{context} contains duplicate override condition {normalized_flags}")
        conditions.append(normalized_flags)
        tier = override.get("authorization_tier")
        if tier is not None and tier not in tiers:
            raise GenerationError(f"{context} override uses unknown tier {tier!r}")
        for key in ("add_effects", "remove_effects"):
            effects = string_list(override.get(key, []), f"{context} override {key}")
            unknown = set(effects) - known_effects
            if unknown:
                raise GenerationError(
                    f"{context} override {key} uses unknown effects: {sorted(unknown)}"
                )
        additions = set(string_list(override.get("add_effects", []), f"{context} add_effects"))
        removals = set(
            string_list(override.get("remove_effects", []), f"{context} remove_effects")
        )
        overlap = additions & removals
        if overlap:
            raise GenerationError(
                f"{context} override both adds and removes effects: {sorted(overlap)}"
            )


def canonical_policy(policy: dict[str, object]) -> dict[str, object]:
    tiers = policy["authorization_tiers"]
    assert isinstance(tiers, dict)
    ordered_tiers = [
        {"id": name, **tier}
        for name, tier in sorted(tiers.items(), key=lambda item: item[1]["order"])
    ]
    return {
        "schema_version": policy["schema_version"],
        "policy_version": policy["policy_version"],
        "cli_version": crate_version(),
        "title": policy["title"],
        "description": policy["description"],
        "effect_tags": policy["effect_tags"],
        "authorization_tiers": ordered_tiers,
        "global_overrides": policy.get("global_overrides", []),
        "commands": policy["commands"],
    }


def json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=False, ensure_ascii=False) + "\n").encode()


def tier_rows(policy: dict[str, object]) -> list[tuple[str, dict[str, object]]]:
    tiers = policy["authorization_tiers"]
    assert isinstance(tiers, dict)
    return sorted(tiers.items(), key=lambda item: item[1]["order"])


def format_effects(effects: list[str]) -> str:
    return ", ".join(f"`{effect}`" for effect in effects) if effects else "None"


def override_summary(overrides: list[dict[str, object]]) -> str:
    if not overrides:
        return "—"
    parts: list[str] = []
    for override in overrides:
        flags = " + ".join(f"`{flag}`" for flag in override["flags"])
        tier = override.get("authorization_tier")
        changes: list[str] = []
        if tier:
            changes.append(f"tier `{tier}`")
        if override.get("add_effects"):
            changes.append("adds " + format_effects(override["add_effects"]))
        if override.get("remove_effects"):
            changes.append("removes " + format_effects(override["remove_effects"]))
        parts.append(f"{flags}: {', '.join(changes)}")
    return "<br>".join(parts)


def render_human_safety(policy: dict[str, object]) -> bytes:
    commands = policy["commands"]
    assert isinstance(commands, list)
    lines = [
        "# Command Safety for Agents",
        "",
        "<!-- Generated by scripts/generate_agent_assets.py; edit agent-command-policy.toml. -->",
        "",
        "This page is generated from the canonical `agent-command-policy.toml` at the repository root. The published [machine-readable policy](agent-command-policy.json) records the `cli_version` of the source snapshot that generated it; compare that field with the installed binary before treating it as matched. `llms.txt` is only a discovery map; this policy is the authorization contract.",
        "",
        "## Authorization tiers",
        "",
        "| Order | Tier | Rule |",
        "| ---: | --- | --- |",
    ]
    for name, tier in tier_rows(policy):
        lines.append(f"| {tier['order']} | `{name}` | {tier['description']} |")
    lines.extend(
        [
            "",
            "Use the highest applicable tier after applying every matching flag override. Approval for one command is scoped to that invocation; it does not authorize later submissions, cancels, deletions, SSH, or runtime execution.",
            "",
            "## Sensitive output is independent",
            "",
            "A command tagged `sensitive-output` needs a separate data-handling check even when it is read-only. Agents must not echo or ingest unredacted output from `plan --show-script`, `render`, `plan --verbose`, `--show-values`, `logs`, or `debug`. Prefer `hpc-compose --offline plan --format json` and `hpc-compose --offline explain --format json`. When a user explicitly requests a script file, write it to an owner-only destination and do not read it back into the conversation.",
            "",
            "Global `--offline` removes network/SSH and scheduler effects, but it does not make local writes, deletes, runtime execution, or sensitive output safe automatically.",
            "",
            "## Effect tags",
            "",
            "| Tag | Meaning |",
            "| --- | --- |",
        ]
    )
    effects = policy["effect_tags"]
    assert isinstance(effects, dict)
    for name, description in effects.items():
        lines.append(f"| `{name}` | {description} |")
    lines.extend(
        [
            "",
            "## Complete command classification",
            "",
            "| Command path | Base tier | Base effects | Flag-sensitive overrides |",
            "| --- | --- | --- | --- |",
        ]
    )
    for command in commands:
        lines.append(
            f"| `hpc-compose {command['path']}` | `{command['authorization_tier']}` | "
            f"{format_effects(command['effects'])} | {override_summary(command.get('overrides', []))} |"
        )
    lines.extend(
        [
            "",
            "## High-risk examples",
            "",
            "- `when` and `germinate` submit automatically after their conditions or canary setup; only `germinate --dry-run` removes submission effects.",
            "- `preflight --fs-probes`, `doctor mpi-smoke --submit`, and `doctor fabric-smoke --submit` consume a small allocation.",
            "- `test --preemption` submits, signals, and requeues a job and therefore uses the destructive tier.",
            "- `sweep observe --watch --stop-when` and `sweep stop` can cancel trials; `sweep submit --resume` can submit previously unattempted trials.",
            "- `workspace allocate` and `workspace extend` mutate external site storage; `workspace release`, cache pruning, rendezvous pruning, cleanup, and cancellation are destructive.",
            "- Local runtime modes (`up --local`, `run --local`, `notebook --local`, `dev`, `tmux`, and `test --local`) execute workload code and are never automatic read-only operations.",
            "",
            "## Related Docs",
            "",
            "- [Set Up With an AI Agent](ai-agent-setup.md)",
            "- [CLI Reference](cli-reference.md)",
            "- [JSON Output Stability](json-output-stability.md)",
            "",
        ]
    )
    return "\n".join(lines).encode()


def render_skill_safety(policy: dict[str, object]) -> bytes:
    commands = policy["commands"]
    assert isinstance(commands, list)
    lines = [
        "# Command safety",
        "",
        "<!-- Generated by scripts/generate_agent_assets.py; edit agent-command-policy.toml. -->",
        "",
        "Apply the highest matching authorization tier. `sensitive-output` is an independent guard. If a command or flag combination is absent, stop and inspect `hpc-compose --offline docs command safety` plus the installed binary's help.",
        "",
    ]
    for name, tier in tier_rows(policy):
        members = [
            f"`{command['path']}`"
            for command in commands
            if command["authorization_tier"] == name
        ]
        lines.extend(
            [
                f"## {tier['order']}. `{name}`",
                "",
                tier["description"],
                "",
                ", ".join(members) + ".",
                "",
            ]
        )
    lines.extend(
        [
            "## Complete flag overrides",
            "",
            "Apply every matching row in policy order. An unchanged tier does not remove an independent sensitive-output guard.",
            "",
            "| Scope | Flags | Resulting tier | Effect changes | Meaning |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    global_overrides = policy.get("global_overrides", [])
    assert isinstance(global_overrides, list)
    for override in global_overrides:
        changes = override_changes(override)
        lines.append(
            f"| Global | `{override['flag']}` | unchanged | {changes} | {override['description']} |"
        )
    for command in commands:
        for override in command.get("overrides", []):
            flags = " + ".join(f"`{flag}`" for flag in override["flags"])
            tier = override.get("authorization_tier")
            lines.append(
                f"| `{command['path']}` | {flags} | "
                f"{f'`{tier}`' if tier else 'unchanged'} | {override_changes(override)} | "
                f"{override['description']} |"
            )
    lines.extend(
        [
            "",
            "## Sensitive-output rule",
            "",
            "Do not echo or ingest unredacted scripts, values, logs, or debug output. Prefer `hpc-compose --offline plan --format json` and `hpc-compose --offline explain --format json`. If the user explicitly asks for a script file, write it owner-only and do not read it back.",
            "",
        ]
    )
    return "\n".join(lines).encode()


def override_changes(override: dict[str, object]) -> str:
    changes: list[str] = []
    additions = override.get("add_effects", [])
    removals = override.get("remove_effects", [])
    if additions:
        assert isinstance(additions, list)
        changes.append("adds " + format_effects(additions))
    if removals:
        assert isinstance(removals, list)
        changes.append("removes " + format_effects(removals))
    return "; ".join(changes) or "none"


def validate_llms_index() -> list[tuple[str, str]]:
    try:
        raw = LLMS_INDEX.read_bytes()
        text = raw.decode("utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise GenerationError(f"cannot read llms.txt: {error}") from error
    if len(raw) > MAX_LLMS_BYTES:
        raise GenerationError(
            f"llms.txt is {len(raw)} bytes; limit is {MAX_LLMS_BYTES} bytes"
        )
    lines = text.splitlines()
    if not lines or not lines[0].startswith("# ") or lines[0].startswith("## "):
        raise GenerationError("llms.txt must begin with exactly one H1")
    if sum(1 for line in lines if line.startswith("# ") and not line.startswith("## ")) != 1:
        raise GenerationError("llms.txt must contain exactly one H1")
    first_h2 = next((index for index, line in enumerate(lines) if line.startswith("## ")), len(lines))
    if not any(line.startswith("> ") for line in lines[1:first_h2]):
        raise GenerationError("llms.txt must include a blockquote summary before its H2 lists")
    if "## Optional" not in lines:
        raise GenerationError("llms.txt must include an Optional section")

    links: list[tuple[str, str]] = []
    section = ""
    for line in lines:
        if line.startswith("## "):
            section = line[3:].strip()
            continue
        match = LINK_RE.search(line)
        if match:
            if not line.startswith("- ") or not section:
                raise GenerationError("llms.txt links must appear in H2 file lists")
            links.append((section, match.group(1)))
    urls = [url for _, url in links]
    duplicates = sorted({url for url in urls if urls.count(url) > 1})
    if duplicates:
        raise GenerationError(f"llms.txt contains duplicate URLs: {duplicates}")
    return links


def expand_includes(path: Path, stack: tuple[Path, ...] = ()) -> str:
    resolved = path.resolve()
    if resolved in stack:
        cycle = " -> ".join(item.name for item in (*stack, resolved))
        raise GenerationError(f"mdBook include cycle: {cycle}")
    try:
        text = resolved.read_text(encoding="utf-8")
    except OSError as error:
        raise GenerationError(f"cannot read included file {resolved}: {error}") from error

    def replace(match: re.Match[str]) -> str:
        target = match.group(1)
        if ":" in target:
            raise GenerationError(
                f"unsupported ranged/anchored mdBook include in {resolved.relative_to(ROOT)}: {target}"
            )
        include_path = (resolved.parent / target).resolve()
        try:
            include_path.relative_to(ROOT)
        except ValueError as error:
            raise GenerationError(f"include escapes repository root: {target}") from error
        if not include_path.is_file():
            raise GenerationError(
                f"unresolved mdBook include in {resolved.relative_to(ROOT)}: {target}"
            )
        return expand_includes(include_path, (*stack, resolved)).rstrip("\n")

    expanded = INCLUDE_RE.sub(replace, text)
    if "{{#include" in expanded:
        raise GenerationError(f"unresolved mdBook include syntax in {resolved.relative_to(ROOT)}")
    return expanded.rstrip() + "\n"


def raw_markdown() -> dict[Path, bytes]:
    result: dict[Path, bytes] = {}
    for source in sorted(DOCS_SOURCE.rglob("*.md")):
        relative = source.relative_to(DOCS_SOURCE)
        result[Path("raw") / relative] = expand_includes(source).encode()
    return result


def context_bytes(links: list[tuple[str, str]], raw: dict[Path, bytes], *, full: bool) -> bytes:
    chunks = [
        "# hpc-compose documentation context\n\n"
        "Generated from the version-matched clean Markdown pages selected by llms.txt. "
        "The command policy, not this context file, defines authorization.\n"
    ]
    seen: set[Path] = set()
    for section, url in links:
        if section == "Optional" and not full:
            continue
        parsed = urlparse(url)
        marker = "/raw/"
        if marker not in parsed.path:
            continue
        relative = Path("raw") / parsed.path.split(marker, 1)[1]
        if relative in seen:
            continue
        content = raw.get(relative)
        if content is None:
            raise GenerationError(f"llms.txt references missing generated Markdown: {url}")
        seen.add(relative)
        chunks.append(f"\n---\n\n<!-- source: /{relative.as_posix()} -->\n\n")
        chunks.append(content.decode())
    output = "".join(chunks).encode()
    limit = MAX_FULL_CONTEXT_BYTES if full else MAX_ESSENTIAL_CONTEXT_BYTES
    label = "llms-ctx-full.txt" if full else "llms-ctx.txt"
    if len(output) > limit:
        raise GenerationError(f"{label} is {len(output)} bytes; limit is {limit} bytes")
    return output


def checked_outputs(policy: dict[str, object]) -> dict[Path, bytes]:
    return {
        POLICY_JSON: json_bytes(canonical_policy(policy)),
        DOCS_POLICY_JSON: json_bytes(canonical_policy(policy)),
        HUMAN_SAFETY: render_human_safety(policy),
        SKILL_SAFETY: render_skill_safety(policy),
        SKILL_VERSION: f"{crate_version()}\n".encode(),
    }


def site_outputs(policy: dict[str, object]) -> dict[Path, bytes]:
    links = validate_llms_index()
    raw = raw_markdown()
    outputs = dict(raw)
    version = crate_version()
    outputs[Path("llms.txt")] = LLMS_INDEX.read_bytes()
    outputs[Path("llms-ctx.txt")] = context_bytes(links, raw, full=False)
    outputs[Path("llms-ctx-full.txt")] = context_bytes(links, raw, full=True)
    policy_bytes = json_bytes(canonical_policy(policy))
    outputs[Path("agent-command-policy.json")] = policy_bytes
    outputs[Path(f"agent-command-policy-v{version}.json")] = policy_bytes
    for source in [
        ROOT / "schema/hpc-compose.schema.json",
        ROOT / "schema/hpc-compose-settings.schema.json",
    ]:
        outputs[Path("schema") / source.name] = source.read_bytes()
    for source in sorted((ROOT / "schema/outputs").glob("*.schema.json")):
        outputs[Path("schema/outputs") / source.name] = source.read_bytes()
    for _, url in links:
        parsed = urlparse(url)
        prefix = "/hpc-compose/"
        if parsed.netloc == "nicolasschuler.github.io" and parsed.path.startswith(prefix):
            relative = Path(parsed.path.removeprefix(prefix))
            if relative not in outputs:
                raise GenerationError(f"llms.txt references an unpublished site artifact: {url}")
    return outputs


def write_or_check(outputs: dict[Path, bytes], *, check: bool, base: Path | None = None) -> None:
    stale: list[str] = []
    for path, expected in outputs.items():
        destination = (base / path) if base is not None else path
        if check:
            try:
                actual = destination.read_bytes()
            except OSError:
                actual = b""
            if actual != expected:
                stale.append(str(destination.relative_to(ROOT) if destination.is_relative_to(ROOT) else destination))
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(expected)
    if stale:
        raise GenerationError("stale or missing generated files:\n  " + "\n  ".join(stale))


def remove_stale_site_files(site_dir: Path, expected: dict[Path, bytes]) -> None:
    expected_paths = {(site_dir / relative).resolve() for relative in expected}
    for directory in (site_dir / "raw", site_dir / "schema/outputs"):
        if not directory.exists():
            continue
        for path in directory.rglob("*"):
            if path.is_file() and path.resolve() not in expected_paths:
                path.unlink()


def fetch_site(base_url: str, expected: dict[Path, bytes]) -> None:
    failures: list[str] = []
    for relative, content in expected.items():
        encoded_path = "/".join(quote(part) for part in relative.parts)
        url = f"{base_url.rstrip('/')}/{encoded_path}"
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                actual = response.read()
        except (OSError, urllib.error.URLError) as error:
            failures.append(f"{url}: {error}")
            continue
        if actual != content:
            failures.append(f"{url}: served bytes differ from generated artifact")
    if failures:
        raise GenerationError("published site verification failed:\n  " + "\n  ".join(failures))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail when generated files are stale")
    parser.add_argument(
        "--site-dir",
        type=Path,
        help="Also generate or check Pages-root raw Markdown, contexts, schemas, and policy",
    )
    parser.add_argument(
        "--base-url",
        help="Fetch and byte-check every generated site artifact from this locally served URL",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        policy = load_toml(POLICY_SOURCE)
        validate_policy(policy)
        validate_llms_index()
        write_or_check(checked_outputs(policy), check=args.check)
        if args.site_dir is not None:
            site_dir = args.site_dir.resolve()
            outputs = site_outputs(policy)
            if not args.check:
                remove_stale_site_files(site_dir, outputs)
            write_or_check(outputs, check=args.check, base=site_dir)
        elif args.base_url is not None:
            outputs = site_outputs(policy)
        if args.base_url is not None:
            fetch_site(args.base_url, outputs)
    except GenerationError as error:
        print(f"agent asset generation failed: {error}", file=sys.stderr)
        return 1
    mode = "checked" if args.check else "generated"
    suffix = f" and site assets in {args.site_dir}" if args.site_dir else ""
    if args.base_url:
        suffix += f" fetched from {args.base_url}"
    print(f"{mode} agent assets{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
