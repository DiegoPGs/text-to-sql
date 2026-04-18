---
name: Bug report
about: Something is broken or behaving unexpectedly
labels: bug
---

## Describe the bug

<!-- A clear, concise description of what is wrong. -->

## Steps to reproduce

1. Run `make ask q="..."`  (or the specific command that triggers the bug)
2. …
3. See error

## Expected behaviour

<!-- What should have happened? -->

## Actual behaviour

<!-- What happened instead? Paste the full error message or unexpected output. -->

## Environment

- OS: <!-- e.g. macOS 14.4, Ubuntu 22.04 -->
- Python: <!-- `python --version` -->
- uv: <!-- `uv --version` -->
- Postgres: <!-- `psql --version` or Docker image digest -->
- Branch / commit: <!-- `git rev-parse --short HEAD` -->

## Logs

<!-- Paste the relevant section from `logs/run-*.jsonl`, or the `--trace` output.
     Redact any API keys or credentials before pasting. -->

```
<paste here>
```

## Additional context

<!-- Anything else that might help — screenshots, related issues, etc. -->
