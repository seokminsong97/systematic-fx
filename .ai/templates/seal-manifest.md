<!-- template: seal-manifest v1 -->
# Seal Manifest: TASK-<number>

Per contract §5, the content stays outside the repository; only this manifest is
committed.

| Field | Value |
|---|---|
| TASK ID | TASK-<number> |
| Sealed object | <description — e.g., orchestrator independent review, private acceptance cases> |
| Artifact type / template | <template name and version used, or N/A> |
| SHA-256 | `<hash>` |
| Pinned revision | <design.md or contract commit SHA> |
| Sealed at | <ISO 8601> |
| State | sealed |

State transitions (contract §5): `sealed` → `disclosed`, or `invalidated`
(hash verification failed) / `abandoned` (run stopped before comparison).
After comparison, disclose and archive the content under `.ai/artifacts/`,
update the State row, and add the published path here.
