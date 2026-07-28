<!-- template: task-packet v1 -->
# Task Packet: TASK-<number> (or GOV-TASK-<number> for governance changes)

## Objective
<What must be implemented — one paragraph>

## Design Basis
- Pinned revision: design.md commit SHA `<sha>` — for governance changes
  (contract §16), the repository commit SHA of the current contract instead
- Related REQs: <REQ-/GOV-REQ IDs and their sections>

## Risk Tier
- Tier: <critical / standard / low> — classified by the orchestrator against
  the contract §10 criteria, with the deciding criterion named
- Required verification for this tier: <e.g., dual implementation +
  differential test, invariants, golden data — or "standard review only">

## Allowed Scope
<Normalized paths or explicit globs, plus allowed operations per entry:
create / modify / delete / rename. Includes test files.>

## Prohibited
<Constraints for this task — e.g., no API changes, no schema changes>

## Completion Criteria
<Observable behavior and expected results — independently testable units>

## Required Test Categories
<normal / failure / boundary / regression — mark N/A with a checkable reason
where a category does not apply (contract §12). Concrete acceptance cases
withheld per contract §7.>

## Reporting
Report using the `.ai/templates/worker-report.md` format.
