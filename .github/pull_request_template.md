## What changed

<!-- A concise description of the change. One paragraph max. -->

## Why

<!-- Motivation: what problem does this solve, or what requirement does it fulfill?
     Link to an issue if one exists: Closes #<number> -->

## How it was tested

<!-- Describe what you ran locally:
     - `make lint` / `make test` output
     - manual `make ask q="..."` sessions
     - any edge cases exercised -->

## Eval delta

<!-- If this PR touches the agent, validator, retrieval index, or golden dataset,
     paste the before/after summary from `make eval`. Otherwise write "N/A". -->

## Checklist

- [ ] `make lint` passes
- [ ] `make test` passes
- [ ] No secrets or credentials in the diff
- [ ] CLAUDE.md `## Changelog` updated if this closes a phase
