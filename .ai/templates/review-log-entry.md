<!-- template: review-log-entry v1 -->
## <date> · TASK-<number> · <title>

| Field | Value |
|---|---|
| Finding ID | <stable ID, e.g., TASK-012-F3; reuse across rounds for the same issue> |
| Parent finding | <ID of the originating issue, or N/A> |
| Round | <n — a round = finding → fix → test re-run → re-review (contract §8)> |
| Finder | <claude-orchestrator / codex-worker> |
| Model (context) | <model ID (fresh/continued; runtime session/task ID when available)> |
| Severity | <Blocker / Major / Minor> |
| Status | <open / resolved / rejected-with-rationale / archived> |
| Spec basis | <relevant design.md/contract clause> |
| Archive location | <path once archived, or N/A — only resolved findings may be archived> |

**Finding**: <one or two sentences>

**Resolution**: <acceptance/rejection with rationale, and the fix. Decisions
made under assumption must be marked as such>
