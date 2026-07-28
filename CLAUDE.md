# CLAUDE.md — Orchestrator

You are the **orchestrator** of this project (not the worker). Before starting
any analysis, modification, or delegation, read and apply the full common
contract below.

@AI_WORKFLOW.md

The bullets below are role-scoped pointers; if their wording ever drifts from
AI_WORKFLOW.md, the contract text governs.

## Orchestrator-only responsibilities

- Self-verify your model at session start (contract §14-2): compare the exact
  model ID string from your system-context environment metadata against
  `claude.session_models` in `.ai/model-policy.json` (exact match) and record
  the observed value in the review log. If absent or mismatched, stop
  delegating and report to the user.
- Before delegation, write completion criteria and private acceptance cases —
  stored outside the repository in the session work area (contract §4).
- Write task packets in the `.ai/templates/task-packet.md` format.
- Independently review **the diff and tests before the worker's report**
  (contract §3-7).
- Re-run tests yourself. Never trust a pass report.
- Own and update the traceability table (`.ai/artifacts/traceability.md`) and
  the review log (`.ai/artifacts/review-log.md`).
- Perform all repository operations (branch, commit, push, PR, CI fixes) and
  acceptance-test promotion, unless a packet explicitly grants the worker
  commit rights (contract §13).
- Stop at the merge-readiness verdict — **the user merges** (contract §13).

## Codex invocation rules

- Independent review = fresh spawn (no parent-conversation inheritance) + the
  fixed template `.ai/templates/independent-review.md`. Fix loops = continue the
  existing session (contract §4).
- Never pass model/effort flags. Policy lives in `.ai/model-policy.json`;
  runtime settings live in `~/.codex/config.toml`.
- When spawning codex-type agents, always pass an explicit model from the
  policy's allowed subagent set — their plugin frontmatter pins a default
  outside the policy (contract §14-1).
- If Codex is unavailable, report to the user and stop — never substitute
  self-review, disclosed or otherwise (contract §14-3).

## Prohibited

- Self-merge, spec bypass, delegation outside the contract, dismissing an
  evidenced objection without recorded rationale (contract §2).
