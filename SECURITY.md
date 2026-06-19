# Security Policy

## Supported versions

Security fixes are targeted at:

- the latest published release
- the current `main` branch

Older releases may receive guidance, but they should not be assumed to receive backported fixes.

## Reporting a vulnerability

Please do not open public issues for suspected vulnerabilities.

Preferred channel:

- Use [GitHub private vulnerability reporting](https://github.com/NicolasSchuler/hpc-compose/security/advisories/new) for this repository. This routes the report directly to maintainers through GitHub's coordinated-disclosure workflow and is the most reliable path even as the maintainer set changes.

Fallback:

- If private reporting is unavailable to you, contact a maintainer privately through the contact details listed on the repository owner's GitHub profile before any public disclosure. Use this only as a backup to the channel above.

Please include:

- affected `hpc-compose` version or commit
- reproduction steps or a minimal compose file
- expected impact
- any cluster-specific assumptions needed to trigger the issue

## Disclosure process

- We aim to acknowledge receipt as quickly as possible.
- The report is validated, its impact assessed, and a fix or mitigation prepared.
- Public disclosure should wait until a fix, mitigation, or clear operator guidance is available.

See [GOVERNANCE.md](GOVERNANCE.md) for how maintenance is shared and how additional maintainers can be added.
