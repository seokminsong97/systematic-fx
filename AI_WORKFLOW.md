# AI_WORKFLOW.md — Joint Operating Contract

This document is the single source of truth for **work procedure, authority, and
verification rules** for every AI agent working in this repository (Claude
orchestrator, Codex worker). The role documents (CLAUDE.md, AGENTS.md) reference
this document and must not duplicate its content. Model policy values live in
`.ai/model-policy.json` and are not duplicated here either.

## 1. Authority Domains

| Subject | Authoritative source |
|---|---|
| Product behavior and requirements | `design.md` (forthcoming) |
| Work procedure, authority, verification | this document |
| Model policy values | `.ai/model-policy.json` (machine-readable source) |
| Local runtime settings | `~/.codex/config.toml` — not a source of truth but a **setting that must comply with policy** |

- System- and tool-enforced constraints always take precedence over repository documents.
- A new user decision takes effect as an implementation baseline only after it is
  reflected in the relevant document.
- If two documents appear to conflict, adjudicate by domain (product vs. procedure).
  If the domains overlap, that is itself a document defect — report it to the user.

## 2. Roles and Decision Rights

- **Claude (orchestrator)**: interprets design.md, decomposes work, writes task
  packets, reviews independently, re-runs tests, owns the traceability table and
  review log, issues the merge-readiness verdict.
- **Codex (worker)**: implements within packet scope, writes unit tests,
  self-reviews, performs reverse review.

The summary hierarchy is "worker < orchestrator < user", but the normative rule
is this table:

| Situation | Authority |
|---|---|
| Raising an issue (test defect, design mismatch, …) | worker — evidence required |
| Adjudication | orchestrator — **must record the rationale in the review log for both acceptance and rejection** |
| Requirement-meaning changes · high risk · persistent disagreement | user |
| Effect of a user decision | confirmed after reflection in the relevant document |

The orchestrator may not dismiss an evidenced objection without recording why.

## 3. Task Cycle

1. Both sides independently review design.md (scope and isolation per §4 and §6)
2. Route differences (§9)
3. Assign REQ IDs and completion criteria to confirmed requirements (§11)
4. Orchestrator: before delegation, write completion criteria and private
   acceptance cases (stored outside the repository, sealed per §5)
5. Deliver the task packet — `.ai/templates/task-packet.md` format, pinned to a
   design.md commit SHA
6. Worker: implement + unit tests + self-review + report
   (`.ai/templates/worker-report.md` format)
7. Orchestrator: independently review **the diff and tests before the report**
   (intensity per §10 tier)
8. Orchestrator: re-run tests directly + run private acceptance tests (§7).
   Critical tier adds dual-implementation differential checks, invariants, and
   golden data (§10)
9. Disclose failing cases only → worker reverse review (§2 rights table applies)
10. Escalate when round limits are exceeded (§8)
11. Update traceability table + review log (§11, §15)
12. Push + open PR → CI green → pass the completion gate (§12) → **user merges** (§13)

## 4. Independence Rules

- An independent review invocation must run in a **fresh context that inherits no
  parent conversation or analysis** (defined by property, not by tool name).
- Independent review instructions use only the fixed template
  `.ai/templates/independent-review.md`. Caller-added wording is an anchoring leak.
- Order: **freeze the input packet → write the orchestrator review → seal it (§5)
  → invoke the worker → mutual disclosure only after both are complete.** Before
  sealing, worker output must not be viewed or displayed.
- Every reviewed file is pinned to a repository commit SHA; when a design.md
  baseline applies, it is recorded as well.
- Unpublished material (orchestrator review drafts, private acceptance cases) is
  stored **outside** the repository, in the session work area. Anything inside the
  repository can be read by a fresh worker.
- Fix loops may continue an existing worker session. Independence is required for
  the initial review, not the iteration.

## 5. Sealing (Integrity Marker)

- Sealing = committing **only a manifest** to the repository: SHA-256 of the
  target file, design.md SHA, template version, TASK ID
  (`.ai/templates/seal-manifest.md` format). The content itself stays outside the
  repository and is disclosed and archived after comparison.
- A hash is an integrity marker for a cooperative environment. It does not claim
  cryptographic tamper-proofness.
- A seal has an explicit state: `sealed` → `disclosed`, or `invalidated` (stored
  bytes fail hash verification) / `abandoned` (the run stopped before
  comparison). On any mismatch: abort the comparison, log the event, and re-seal
  before continuing.
- Secrets and raw prompts are never part of sealed records.

## 6. Phase 0 Scoping

- **Full independent review**: at project start, and on major design changes.
  "Major" is judged by reusing the full §10 risk criteria (non-exhaustive
  examples: changes touching money, security, external contracts, or
  irreversibility).
- **Normal tasks**: independent review of the related REQs and affected dependent
  areas only.
- On design changes: re-review the changed REQs and everything linked to them via
  the traceability table (§11).

## 7. Private Acceptance Tests

- The purpose is **anti-anchoring**. This is not a security boundary.
- Requirements, completion criteria, and test categories are fully disclosed in
  the packet. Only concrete inputs and acceptance cases are withheld until the
  first implementation is complete.
- On failure, disclose the minimal reproduction and the linked REQ for that case.
- Passing acceptance tests are merged into the repository and CI. Permanently
  hidden tests are not allowed.
- Sensitive data is kept as sanitized fixtures.

## 8. Round Rules

- A round = one bundle of: finding → fix → test re-run → re-review.
- If the same issue is unresolved after the second round, request user arbitration
  **before entering a third round**.
- If the task is still incomplete after four cumulative rounds, re-examine the
  task decomposition and the spec.
- New defects count as separate issues but do count toward the task's cumulative
  rounds.

## 9. Arbitration Routing

| Nature of the difference / decision | Handling |
|---|---|
| Decidable from documents alone | orchestrator resolves with rationale, logged |
| Reversible implementation choice | proceed under an explicit assumption, logged (user may veto later) |
| Changes the meaning of a requirement | user decides |
| Affects money, security, data, or external API contracts | user decides |
| Irreversible or costly to undo (data migration, live order behavior, risk-limit changes) | user decides |

A worker may proceed only on packet-scoped, reversible assumptions and must
report them; requirement-meaning choices always stop for escalation. After a
user decision, the orchestrator prepares the corresponding document change,
records the new revision, and work resumes only against the approved revision.

## 10. Risk Tiering

Tier is judged not by "is it boilerplate" but by: blast radius on failure /
difficulty of rollback / money-data-security relevance / requirement ambiguity /
external system dependency / regression potential.

**FX critical tier (provisional; finalized per REQ when design.md arrives)**:
position sizing, P&L calculation, currency conversion and direction, transaction
costs, timezone handling, look-ahead bias prevention.

Critical-tier verification: dual implementation + differential testing,
mathematical invariants and property-based tests, fixed golden data, and
reconciliation against real historical cases. Agreement between two
implementations does not guarantee correctness (a shared misreading of the spec
passes) — validating the spec itself is Phase 0's job.

Verification paths (review, acceptance tests, differential adjudication) must
use exactly the models and effort values of `.ai/model-policy.json` — set
membership for models, string equality for effort; no ordering between models is
implied. `claude.effort` applies to subagent invocations; the session
model/effort is user-controlled and checked per §14-2.

## 11. REQ Traceability

- Requirements in design.md receive `REQ-<AREA>-<NNN>` IDs. Granularity:
  **independently testable behavior** — not one ID per sentence.
- Traceability table: REQ → TASK → TEST → changed files, maintained in
  `.ai/artifacts/traceability.md`.
- Owned by the orchestrator; updated at task closure; completeness checked at
  each milestone.

## 12. Completion Gate (Definition of Done)

All items verified before the merge-readiness verdict:

- [ ] Every required REQ is linked to TASK, TEST, and changes
- [ ] No unresolved Blocker/Major issues
- [ ] Assumptions recorded in the review log
- [ ] Unit, acceptance, regression, and numerical verification pass where
      applicable — re-run directly by the orchestrator; every N/A category has
      a checkable reason recorded in the packet or report
- [ ] CI green
- [ ] No out-of-scope changes
- [ ] Approved design changes reflected in design.md (doc–code sync)
- [ ] Orchestrator merge-readiness verdict

The user's PR merge is the outcome that follows the verdict, not a gate item.

## 13. Autonomy — Level 3

- Agent autonomy covers: branch creation → implementation → commit → push → PR
  creation → CI-failure fixes → CI green.
- **Only the user merges.**
- Paths touching live order execution, position management, risk limits, or
  broker credentials always require explicit user approval, regardless of
  autonomy level.
- The worker cannot self-merge; the orchestrator stops at the merge-readiness
  verdict.
- Repository operations (branch, commit, push, PR creation, CI fixes) and the
  promotion of passed acceptance tests into the repository are performed by the
  orchestrator, unless a task packet explicitly grants the worker commit rights
  within its allowed scope.

## 14. Model Policy and Enforcement

Policy values have a single source: `.ai/model-policy.json`. Enforcement is
layered:

1. **Hook** (`.claude/hooks/model-lock.py`): the hook blocks policy violations
   visible in call arguments and in verifiable configuration. Session model
   identity, API availability, and fallback behavior are verified by the
   behavioral contract and the audit log, not by the hook.
   - Validates explicit model arguments on subagent calls.
   - Requires an explicit allowed model when spawning codex-type agents (their
     plugin frontmatter pins a default outside the policy).
   - Validates model/effort strings in Workflow scripts.
   - Codex invocations (direct CLI or the plugin's `codex-companion.mjs`
     boundary): blocks per-call override flags, and **checks
     `~/.codex/config.toml` against policy on every call** (drift detection).
   - Fail closed: if the policy file is missing or corrupt, every call that
     selects or executes a model is denied; plain calls stay allowed.
   - Every decision is appended, metadata-only, to `.ai/logs/hook-audit.jsonl`
     (git-ignored).
2. **Orchestrator self-verification**: at session start (and before any
   delegation), compare the exact model ID string provided in the
   system-context environment metadata against `claude.session_models` (exact
   match, no normalization), and record the observed value in the review log.
   If the metadata is absent or mismatched, **stop delegating and report to the
   user.** Effort is not verifiable from context — it belongs to user settings
   and the audit log, and this limit is stated explicitly.
3. **Fail-closed behavioral contract**: when a policy model is unavailable, do
   not substitute a lower model. Report the failure and stop. (The hook cannot
   detect this; it is verified by behavior and logs.)

## 15. Artifacts and Logs

- Templates: `.ai/templates/` / outputs: `.ai/artifacts/` / hook audit log:
  `.ai/logs/` (git-ignored).
- The hook audit log is decision-level JSONL written by the hook at PreToolUse
  time, with fields `ts, tool, decision, rule, detail` (observed Codex config
  values are included in `detail`). It cannot contain end times or results.
- Invocation completion metadata is recorded by the orchestrator in the review
  log, **metadata only**: TASK/REQ ID, model and role, fresh/continued context,
  input packet path and hash, start/end time, result status, artifact paths,
  and runtime-produced identifiers (e.g., Codex session IDs, task IDs) as
  independence evidence.
- Untracked logs are local evidence only: at each milestone the orchestrator
  summarizes them into the committed review log. Absence of an untracked log is
  not evidence that no events occurred.
- Never record secrets, tokens, raw prompts, or internal reasoning.
- Review log (`.ai/artifacts/review-log.md`): substantive findings only —
  finder / content / severity / resolution / spec basis. Curated at each
  milestone: recurring patterns are promoted to rules, one-off resolved items
  archived.

## 16. Contract Changes

Changes to this document, `.ai/model-policy.json`, or the hook go through the §3
task cycle exactly like ordinary code. Code changing first with documents
rationalized afterwards is prohibited.

Governance changes (documents, policy, hook — anything without a design.md
basis) use the `GOV-REQ-<NNN>` ID namespace and pin their packets and reviews to
the repository commit SHA of the current contract instead of a design.md SHA.
Test categories that do not apply to a documentation-only change are marked N/A
with a checkable reason (§12).

## 17. Unverified Items (Honesty Clause)

- **Codex invocation path — partially verified.** Static analysis confirms the
  plugin invokes Codex through a Bash call to `node …/codex-companion.mjs`,
  which spawns `codex app-server` internally; the hook enforces at the Bash
  boundary. Hook firing on this session's own tool calls is confirmed by audit
  log evidence. Whether the hook also fires on **subagent** tool calls is still
  awaiting runtime confirmation via `.ai/logs/hook-audit.jsonl` — until an
  allow/deny entry from a real subagent Codex invocation is observed, the claim
  "the hook enforces the Codex path end-to-end" remains provisional.
- When confirmed, update this clause; if a bypass path is found, extend the hook.
