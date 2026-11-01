# Security Policy

## Reporting a vulnerability

Report privately through GitHub's private vulnerability reporting on this repository: Security, then "Report a vulnerability".
Do not open a public issue.
Include the affected version, the impact, and steps to reproduce.

This is a volunteer-run open-source project, so there is no paid response commitment.
The aim is to acknowledge a report within 5 business days, and to ship a fix or a mitigation for a confirmed critical issue as fast as is responsible.

## Supported versions

Only the latest released minor version receives security fixes until 1.0.

## Treat these as security issues

- A credential appearing in a log, repr, exception, fixture, or metric.
- Any path that sends an order without passing pre-trade risk and the journal.
- An order being sent twice, or a fill being lost.
- Tampering with a journal that `ismet journal verify` does not detect, other than the truncation and full-rewrite limits documented in the threat model.

See [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md).
