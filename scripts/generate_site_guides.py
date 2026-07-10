#!/usr/bin/env python3
"""Generate validated cluster-site guides from JSON facts and one template."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from string import Template
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
FACTS_DIR = ROOT / "docs" / "site-guides" / "sites"
TEMPLATE_PATH = ROOT / "docs" / "site-guides" / "template.md"
MAX_VERIFICATION_AGE_DAYS = 180


class ValidationError(ValueError):
    """Raised when a site fact file is incomplete or inconsistent."""


def require(mapping: dict[str, Any], key: str, kind: type, context: str) -> Any:
    value = mapping.get(key)
    if not isinstance(value, kind) or (kind in (str, list, dict) and not value):
        raise ValidationError(f"{context}.{key} must be a non-empty {kind.__name__}")
    return value


def require_records(mapping: dict[str, Any], key: str, fields: tuple[str, ...], context: str) -> list[dict[str, Any]]:
    records = require(mapping, key, list, context)
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValidationError(f"{context}.{key}[{index}] must be an object")
        for field in fields:
            require(record, field, str, f"{context}.{key}[{index}]")
    return records


def validate(data: dict[str, Any], source: Path) -> None:
    context = source.relative_to(ROOT).as_posix()
    if data.get("schema_version") != 1:
        raise ValidationError(f"{context}.schema_version must equal 1")
    for key in ("slug", "title", "output"):
        require(data, key, str, context)

    verification = require(data, "verification", dict, context)
    for key in ("verified_on", "state", "note"):
        require(verification, key, str, f"{context}.verification")
    try:
        verified_on = date.fromisoformat(verification["verified_on"])
    except ValueError as error:
        raise ValidationError(
            f"{context}.verification.verified_on must use ISO YYYY-MM-DD"
        ) from error
    age_days = (date.today() - verified_on).days
    if age_days < 0:
        raise ValidationError(
            f"{context}.verification.verified_on cannot be in the future"
        )
    if age_days > MAX_VERIFICATION_AGE_DAYS:
        raise ValidationError(
            f"{context}.verification is stale ({age_days} days old; "
            f"maximum {MAX_VERIFICATION_AGE_DAYS}); re-check the primary sources"
        )
    if verification["state"] not in {"current", "transitional", "uncertain"}:
        raise ValidationError(f"{context}.verification.state has unsupported value")

    sources = require_records(data, "sources", ("id", "title", "url", "covers"), context)
    urls = [record["url"] for record in sources]
    if len(urls) != len(set(urls)):
        raise ValidationError(f"{context}.sources contains duplicate URLs")
    if any(not url.startswith("https://") for url in urls):
        raise ValidationError(f"{context}.sources URLs must use https")

    access = require(data, "access", dict, context)
    for key in ("login_host", "login_command"):
        require(access, key, str, f"{context}.access")
    require(access, "requirements", list, f"{context}.access")

    scheduler = require(data, "scheduler", dict, context)
    require_records(scheduler, "accounts", ("name", "hpc_compose", "evidence"), f"{context}.scheduler")
    require_records(scheduler, "partitions", ("name", "access", "limits", "hpc_compose"), f"{context}.scheduler")
    require_records(scheduler, "qos", ("name", "hpc_compose", "evidence"), f"{context}.scheduler")
    require_records(scheduler, "gres", ("name", "request", "notes"), f"{context}.scheduler")

    require_records(data, "runtimes", ("name", "hpc_compose", "verification", "status"), context)
    modules = require(data, "modules", dict, context)
    for key in ("discovery_commands", "setup_example", "notes"):
        require(modules, key, list, f"{context}.modules")
    require_records(data, "storage", ("name", "visibility", "lifetime", "hpc_compose_role", "avoid"), context)

    provisioning = require_records(data, "provisioning", ("title",), context)
    for index, record in enumerate(provisioning):
        require(record, "commands", list, f"{context}.provisioning[{index}]")
        require(record, "notes", list, f"{context}.provisioning[{index}]")

    cache_policy = require(data, "cache_policy", dict, context)
    for key in ("cache", "temporary", "durability", "beeond"):
        require(cache_policy, key, str, f"{context}.cache_policy")

    probes = require_records(data, "smoke_probes", ("name", "expected"), context)
    for index, probe in enumerate(probes):
        if not isinstance(probe.get("consumes_allocation"), bool):
            raise ValidationError(f"{context}.smoke_probes[{index}].consumes_allocation must be boolean")
        require(probe, "commands", list, f"{context}.smoke_probes[{index}]")

    require(data, "restrictions", list, context)
    require_records(data, "common_failures", ("symptom", "interpretation", "checks"), context)
    support = require(data, "support", dict, context)
    require(support, "url", str, f"{context}.support")
    require(support, "when", list, f"{context}.support")
    require(support, "include", list, f"{context}.support")

    output = ROOT / data["output"]
    try:
        output.relative_to(ROOT / "docs" / "src")
    except ValueError as error:
        raise ValidationError(f"{context}.output must live below docs/src") from error


def bullets(items: list[str]) -> str:
    return "\n".join(f"- {item}" for item in items)


def code_block(commands: list[str]) -> str:
    return "```bash\n" + "\n".join(commands) + "\n```"


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    lines.extend("| " + " | ".join(cell.replace("\n", " ") for cell in row) + " |" for row in rows)
    return "\n".join(lines)


def render(data: dict[str, Any], source: Path, template: Template) -> str:
    verification = data["verification"]
    verification_md = (
        f'<div class="callout warning" role="note" aria-label="Site facts require re-verification">\n\n'
        f"**Verified {verification['verified_on']} · {verification['state'].title()} site state.** "
        f"{verification['note']}\n\n</div>"
    )

    sources_md = "\n".join(
        f"- [{item['title']}]({item['url']}) — {item['covers']}." for item in data["sources"]
    )

    access = data["access"]
    access_md = (
        f"Published login endpoint: `{access['login_host']}`.\n\n"
        + code_block([access["login_command"]])
        + "\n\n"
        + bullets(access["requirements"])
    )

    scheduler = data["scheduler"]
    scheduler_parts = [
        "### Accounts and QOS",
        "",
        markdown_table(
            ["Kind", "Published value", "hpc-compose mapping", "Evidence / limit"],
            [["Account", item["name"], f"`{item['hpc_compose']}`", item["evidence"]] for item in scheduler["accounts"]]
            + [["QOS", item["name"], f"`{item['hpc_compose']}`", item["evidence"]] for item in scheduler["qos"]],
        ),
        "",
        "### Partitions",
        "",
        markdown_table(
            ["Partition", "Access", "Published legacy limits", "hpc-compose mapping"],
            [[f"`{item['name']}`", item["access"], item["limits"], f"`{item['hpc_compose']}`"] for item in scheduler["partitions"]],
        ),
        "",
        "### GPU GRES",
        "",
        markdown_table(
            ["Resource", "Request", "Notes"],
            [[item["name"], f"`{item['request']}`", item["notes"]] for item in scheduler["gres"]],
        ),
    ]

    runtimes_md = markdown_table(
        ["Runtime path", "hpc-compose", "Verify", "Published state"],
        [[item["name"], f"`{item['hpc_compose']}`", item["verification"], item["status"]] for item in data["runtimes"]],
    )

    modules = data["modules"]
    modules_md = (
        "### Module discovery\n\n"
        + code_block(modules["discovery_commands"])
        + "\n\nA reproducible host-runtime setup belongs in the spec:\n\n"
        + "```yaml\nx-slurm:\n  setup:\n"
        + "\n".join(f"    - {command}" for command in modules["setup_example"])
        + "\n```\n\n"
        + bullets(modules["notes"])
    )

    storage_md = markdown_table(
        ["Storage", "Visibility", "Lifetime", "Good hpc-compose role", "Avoid"],
        [[f"`{item['name']}`", item["visibility"], item["lifetime"], item["hpc_compose_role"], item["avoid"]] for item in data["storage"]],
    )

    provisioning_parts: list[str] = []
    for index, item in enumerate(data["provisioning"], start=1):
        provisioning_parts.extend(
            [f"### {index}. {item['title']}", "", code_block(item["commands"]), "", bullets(item["notes"]), ""]
        )

    cache = data["cache_policy"]
    cache_md = bullets(
        [
            f"**Shared cache:** {cache['cache']}",
            f"**Node-local temporary data:** {cache['temporary']}",
            f"**Durability:** {cache['durability']}",
            f"**BeeOND:** {cache['beeond']}",
        ]
    )

    probe_parts: list[str] = []
    for index, probe in enumerate(data["smoke_probes"], start=1):
        if probe["consumes_allocation"]:
            cost = "**Allocation-consuming:** yes. This submits or runs work through Slurm and can consume quota."
        else:
            cost = "**Allocation-consuming:** no."
        probe_parts.extend(
            [f"### {index}. {probe['name']}", "", cost, "", code_block(probe["commands"]), "", f"Expected signal: {probe['expected']}.", ""]
        )

    failure_md = markdown_table(
        ["Symptom", "Interpretation", "Check next"],
        [[item["symptom"], item["interpretation"], item["checks"]] for item in data["common_failures"]],
    )

    support = data["support"]
    support_md = (
        f"Use the [NHR@KIT Support Portal]({support['url']}) when:\n\n"
        + bullets(support["when"])
        + "\n\nInclude enough redacted evidence to reproduce the boundary:\n\n"
        + bullets(support["include"])
    )

    values = {
        "source_path": source.relative_to(ROOT).as_posix(),
        "title": data["title"],
        "verification": verification_md,
        "sources": sources_md,
        "access": access_md,
        "scheduler": "\n".join(scheduler_parts),
        "runtimes": runtimes_md,
        "modules": modules_md,
        "storage": storage_md,
        "provisioning": "\n".join(provisioning_parts).rstrip(),
        "cache_policy": cache_md,
        "smoke_probes": "\n".join(probe_parts).rstrip(),
        "restrictions": bullets(data["restrictions"]),
        "common_failures": failure_md,
        "support": support_md,
    }
    try:
        rendered = template.substitute(values)
    except KeyError as error:
        raise ValidationError(f"unresolved template placeholder: {error.args[0]}") from error
    if not rendered.endswith("\n"):
        rendered += "\n"
    return rendered


def load_sites() -> list[tuple[Path, dict[str, Any]]]:
    sites: list[tuple[Path, dict[str, Any]]] = []
    for source in sorted(FACTS_DIR.glob("*.json")):
        try:
            data = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValidationError(f"cannot read {source.relative_to(ROOT)}: {error}") from error
        if not isinstance(data, dict):
            raise ValidationError(f"{source.relative_to(ROOT)} must contain a JSON object")
        validate(data, source)
        sites.append((source, data))
    if not sites:
        raise ValidationError("no site fact files found")
    return sites


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail when generated pages are stale")
    args = parser.parse_args()

    try:
        template = Template(TEMPLATE_PATH.read_text(encoding="utf-8"))
        sites = load_sites()
        stale: list[str] = []
        for source, data in sites:
            output = ROOT / data["output"]
            expected = render(data, source, template)
            if args.check:
                actual = output.read_text(encoding="utf-8") if output.exists() else ""
                if actual != expected:
                    stale.append(output.relative_to(ROOT).as_posix())
            else:
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text(expected, encoding="utf-8")
                print(f"generated {output.relative_to(ROOT)}")
        if stale:
            print("stale generated site guides:", file=sys.stderr)
            for path in stale:
                print(f"  {path}", file=sys.stderr)
            print("run: python3 scripts/generate_site_guides.py", file=sys.stderr)
            return 1
    except (OSError, ValidationError) as error:
        print(f"site-guide generation failed: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
