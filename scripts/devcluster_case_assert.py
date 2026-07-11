#!/usr/bin/env python3
"""Validate stable JSON contracts emitted by opt-in dev-cluster cases."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


class ContractError(RuntimeError):
    """Raised when a dev-cluster result does not satisfy its contract."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ContractError(message)


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ContractError(f"could not read JSON result {path}: {error}") from error
    require(isinstance(value, dict), "top-level JSON result must be an object")
    return value


def check_preemption(value: dict[str, Any], _job_id: str | None) -> None:
    require(value.get("schema_version") == 1, "schema_version must be 1")
    require(value.get("mode") == "preemption", "mode must be preemption")
    require(value.get("backend") == "slurm", "backend must be slurm")
    require(value.get("ok") is True, "preemption report must have ok=true")
    require(value.get("failure_reason") is None, "preemption report has a failure reason")

    summary = value.get("preemption")
    require(isinstance(summary, dict), "preemption summary is missing")
    require(summary.get("signal") == "USR1", "synthetic signal must be USR1")
    require(summary.get("signal_target") == "step", "signal target must be step")
    attempt = summary.get("observed_attempt")
    require(isinstance(attempt, int) and attempt >= 1, "resumed attempt was not observed")
    require(summary.get("observed_is_resume") is True, "resumed state must report is_resume=true")

    phases = value.get("phases")
    require(isinstance(phases, list), "phase list is missing")
    phase_status = {
        phase.get("name"): phase.get("status")
        for phase in phases
        if isinstance(phase, dict)
    }
    for name in ("running", "signal", "requeue", "attempt_2", "terminal", "evaluate"):
        require(phase_status.get(name) == "ok", f"phase {name} did not complete successfully")

    services = value.get("services")
    require(isinstance(services, list) and services, "service results are missing")
    for service in services:
        require(isinstance(service, dict), "service result must be an object")
        require(service.get("completed_successfully") is True, "service did not complete successfully")
        require(service.get("failures") == [], "service report contains failures")

    require(bool(value.get("job_id")), "preemption report is missing job_id")


def check_preemption_checkpoints(value: dict[str, Any], job_id: str | None) -> None:
    require(value.get("schema_version") == 1, "schema_version must be 1")
    require_job_id(job_id, value.get("job_id"))
    require(value.get("resume_configured") is True, "checkpoint history has no resume contract")
    require(value.get("attempts") == 2, "checkpoint history must contain exactly two attempts")
    require(value.get("requeues") == 1, "checkpoint history must contain exactly one requeue")
    require(value.get("current_attempt") == 1, "latest checkpoint attempt must be 1")
    require(value.get("is_resume") is True, "latest checkpoint state must be resumed")
    require(value.get("degraded") == [], "checkpoint history contains degraded entries")
    entries = value.get("entries")
    require(isinstance(entries, list) and len(entries) == 2, "checkpoint entries must contain two attempts")
    require(
        [entry.get("attempt") for entry in entries if isinstance(entry, dict)] == [0, 1],
        "checkpoint attempt indices must be [0, 1]",
    )
    require(entries[-1].get("job_exit_code") == 0, "resumed attempt did not exit 0")


def check_fs_probes(value: dict[str, Any], _job_id: str | None) -> None:
    require(value.get("schema_version") == 1, "schema_version must be 1")
    summary = value.get("summary")
    require(isinstance(summary, dict), "preflight summary is missing")
    require(summary.get("blockers") == 0, "filesystem preflight reported blockers")
    passed = value.get("passed_checks")
    require(isinstance(passed, list), "preflight passed_checks is missing")
    messages = [
        item.get("message", "")
        for item in passed
        if isinstance(item, dict) and isinstance(item.get("message"), str)
    ]
    probe_messages = [message for message in messages if "shared filesystem probe passed" in message]
    require(probe_messages, "no successful shared filesystem probe was reported")
    message = "\n".join(probe_messages)
    for fragment in (
        "compute saw login write",
        "login saw compute write",
        "rename atomicity ok",
        "compute headroom",
    ):
        require(fragment in message, f"filesystem probe result is missing {fragment!r}")


def require_job_id(job_id: str | None, actual: Any) -> str:
    require(bool(job_id), "--job-id is required for this contract")
    require(str(actual) == job_id, f"expected job_id {job_id}, got {actual!r}")
    return job_id


def check_remote_status(value: dict[str, Any], job_id: str | None) -> None:
    require(value.get("schema_version") == 1, "schema_version must be 1")
    record = value.get("record")
    scheduler = value.get("scheduler")
    require(isinstance(record, dict), "status record is missing")
    require(isinstance(scheduler, dict), "status scheduler block is missing")
    require_job_id(job_id, record.get("job_id"))
    require(scheduler.get("state") == "COMPLETED", "remote status is not COMPLETED")


def check_remote_stats(value: dict[str, Any], job_id: str | None) -> None:
    require(value.get("schema_version") == 1, "schema_version must be 1")
    scheduler = value.get("scheduler")
    require(isinstance(scheduler, dict), "stats scheduler block is missing")
    require_job_id(job_id, value.get("job_id"))
    require(scheduler.get("state") == "COMPLETED", "remote stats is not COMPLETED")
    accounting = value.get("accounting")
    require(isinstance(accounting, dict), "remote stats accounting block is missing")
    require(accounting.get("available") is True, "remote accounting is unavailable")
    rows = accounting.get("rows")
    require(isinstance(rows, list) and rows, "remote accounting contains no rows")


def check_remote_score(value: dict[str, Any], job_id: str | None) -> None:
    require(value.get("schema_version") == 1, "schema_version must be 1")
    require_job_id(job_id, value.get("job_id"))
    require(value.get("scheduler_state") == "COMPLETED", "remote score is not COMPLETED")
    require(value.get("complete") is True, "remote score is incomplete")
    score = value.get("score")
    require(isinstance(score, int) and 0 <= score <= 100, "remote score is outside 0..100")


def check_remote_pull(value: dict[str, Any], job_id: str | None) -> None:
    require(value.get("schema_version") == 1, "schema_version must be 1")
    require_job_id(job_id, value.get("job_id"))
    files = value.get("files")
    require(isinstance(files, int) and files > 0, "remote pull resolved no artifact files")
    bundles = value.get("bundles")
    require(isinstance(bundles, list) and "logs" in bundles, "remote pull is missing the logs bundle")
    command = value.get("suggested_command")
    require(isinstance(command, str) and "rsync " in command, "remote pull has no rsync command")


CHECKS = {
    "preemption": check_preemption,
    "preemption-checkpoints": check_preemption_checkpoints,
    "fs-probes": check_fs_probes,
    "remote-status": check_remote_status,
    "remote-stats": check_remote_stats,
    "remote-score": check_remote_score,
    "remote-pull": check_remote_pull,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("contract", choices=sorted(CHECKS))
    parser.add_argument("path", type=Path)
    parser.add_argument("--job-id")
    args = parser.parse_args()

    try:
        CHECKS[args.contract](load_json(args.path), args.job_id)
    except ContractError as error:
        print(f"{args.contract} contract failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
