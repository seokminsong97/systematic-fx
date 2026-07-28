# Review Log

Substantive findings only (contract §15). Entry format: `.ai/templates/review-log-entry.md`.
This bootstrap entry predates the REQ system and uses a batch format.

## 2026-07-28 · GOV-TASK-001 · Bootstrap cross-review of the workflow contract

| Field | Value |
|---|---|
| Scope | AI_WORKFLOW.md, CLAUDE.md, AGENTS.md, .ai/model-policy.json, .ai/templates/*, .claude/hooks/model-lock.py, .claude/settings.json |
| Finder | codex-worker (fresh context) |
| Reviewer model | gpt-5.6-sol / max (per ~/.codex/config.toml; Codex session 019fa9be-e5cb-7830-a305-b6e1b5c4ec35, job task-ms4xa7bs-9661yz) |
| Adjudicator | claude-orchestrator (claude-fable-5 session) |
| Findings | 34 (F1–F34): 3 Blocker, 28 Major, 3 Minor |
| Status | resolved (see disposition) |

**Disposition** (adjudication rationale recorded per contract §2):

- **Accepted and fixed in this revision** — F1 (task-cycle §3/§12 circular
  ordering; reordered, user merge moved out of the gate), F2 (severity scale
  unified to Blocker/Major/Minor), F3 (governance changes: GOV-REQ namespace +
  contract-SHA pinning added to §16), F4 (reviews pin every file to a repo
  commit SHA), F13 (policy schema semantics defined; hook fails closed on
  non-1.x), F14 (effort scope + membership/equality semantics in §10),
  F15 (Codex unavailable → report and stop, no substitute), F16 (§6 examples
  marked non-exhaustive), F17 (packet Risk Tier field), F18 (worker assumptions
  packet-scoped and reversible only), F19/F20 (review template: placeholder
  exemption + scope/packet-hash placeholders), F22/F23 (seal manifest artifact
  type row, version placeholder), F24 (seal lifecycle states), F25 (scope as
  globs + allowed operations), F26 (worker packet preflight), F27/F28 (report
  status/self-review/blocked sections + REQ mapping table), F29 (review-log
  finding ID/status/round/parent/archive fields), F30 (repository operations
  assigned to the orchestrator by default), F31 (N/A test categories with
  checkable reasons), F32 (audit schema, writer, retention, milestone
  summarization in §15), F33 (self-check metadata field named, exact match,
  observed value logged), F34 (orchestrator reflects user decisions into
  documents before work resumes), F7/F12 (hook message authority wording;
  observed config values logged in audit detail).
- **Already fixed before adjudication** — F6, F8, F9, F10, F11: the reviewed
  hook was v1; the v2 rewrite (same day, prior to receiving this review) loads
  the policy file, fails closed, allowlists models/effort by membership,
  checks config.toml per call, and writes the audit log. Verified by a 36-case
  test suite.
- **Accepted with mitigation** — F5 (role-doc duplication): role documents keep
  actionable pointers but now carry an explicit supremacy clause ("the
  contract text governs on drift"); full de-duplication rejected because role
  files must remain independently actionable for their agent.
- **Partially accepted** — F21 (independence not provable from self-declared
  labels): runtime-produced identifiers (Codex session/task IDs) are now
  required in invocation logs as evidence; full tool-produced lineage is not
  available in the current environment, and §5 already scopes hashes as
  cooperative integrity markers, not proofs.

**Process disclosures** (assumptions/violations, contract §15):

1. The review ran while the documents were being translated Korean → English
   (repo-language requirement); the reviewer saw mixed revisions. All fixes
   were applied to the English revision. Line numbers in the raw findings refer
   to the Korean drafts.
2. The reviewer spawn was made without an explicit model override, so the
   plugin wrapper (not the reviewer itself) defaulted to a non-policy Claude
   model. The reviewing model was Codex per config and is unaffected. Rule
   added (CLAUDE.md, hook `codex-wrapper-model` deny) so recurrence is blocked.
3. Hook activation was empirically confirmed mid-session on the orchestrator's
   own tool calls (audit entry 2026-07-28T17:24:28Z); subagent-call coverage
   remains provisional per §17.
