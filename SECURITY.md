# Security Policy

## Supported versions

This project is pre-1.0. Only the latest commit on `main` receives security fixes.

| Version | Supported |
|---------|-----------|
| `0.x.y` (latest) | Yes |
| any prior tag | No |

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report privately by emailing the maintainer directly (see the commit history for
contact details) with:

- A description of the vulnerability and its potential impact.
- Steps to reproduce or a minimal proof-of-concept.
- Any suggested mitigations you have already identified.

Expected response time: acknowledgement within 72 hours, triage within 7 days.
If you have not heard back within 72 hours, follow up by opening a _blank_ issue
titled "Security contact needed" — no details, just a ping.

Responsible disclosure: please allow 90 days from acknowledgement before public
disclosure to give time for a fix and coordinated release.

## Known acceptable risks

The following risks are explicitly accepted for the MVP scope and documented here
for transparency. They cross-reference the threat model in `CLAUDE.md §Threat model`.

### Multi-tenant isolation not implemented

The MVP is single-tenant and local-only. There is no row-level security and no
per-tenant session isolation. Do not deploy this as a shared service without
adding those controls first.

### Network exposure not hardened

The system runs on localhost only. TLS termination, WAF rules, and network-level
isolation are out of scope for the MVP.

### Secret rotation is manual

API keys (`ANTHROPIC_API_KEY`, `SNYK_TOKEN`) and database passwords are rotated
manually. There is no automated rotation or expiry enforcement. Treat any leaked
key as immediately compromised and rotate it.

### Model weights supply chain is trusted

The project calls hosted model APIs (Anthropic, OpenAI). The integrity of those
model weights is outside our control. We trust the providers' security posture.

### Eval-set integrity relies on code review

`evals/golden.yaml` is the source of truth for regression testing. A malicious
edit to that file could mask a regression. Mitigation for solo development:
changes to `evals/` should be reviewed with the same care as changes to
`agent/validate.py`. Once a second contributor joins, add a CODEOWNERS rule.

## Security controls in place

See `CLAUDE.md §Safety rules` for the inviolate SQL-safety contract and
`CLAUDE.md §Threat model` for the full threat model with per-threat mitigations.
The short list:

- **Secret scanning**: Gitleaks runs on every commit (pre-commit hook) and on
  every push/PR (CI security workflow).
- **Dependency scanning**: Snyk and `pip-audit` run in CI on every PR.
- **SAST**: Bandit runs in CI on every PR.
- **SQL injection via prompt injection**: four independent layers — parse-tree
  SELECT-only enforcement, read-only DB role, implicit row limit, statement
  timeout.
- **Structured outputs**: all LLM and tool results are typed Pydantic models;
  the agent cannot execute instructions injected through tool output.
