# PostgreSQL Migrations

Ordered SQL migrations own all PostgreSQL schema changes. PostgreSQL is the
research control plane; raw MBP-10 events and wide feature rows remain in
immutable Parquet.

Initialize the repository-private PostgreSQL 18 control plane, create the
database when absent, and apply migrations with:

```bash
make db-up
```

For an externally managed PostgreSQL target, configure both the admin and
application URLs in the ignored `.env`, then run `make db-bootstrap`.

The runner executes migrations in numeric filename order and records the exact
SHA-256 in `systematic_fx.schema_migrations`. A changed checksum for an applied
version is an error; create a new migration instead of editing history. Every
migration contains its own `BEGIN`/`COMMIT`, so a failed file leaves neither a
partial schema nor a migration record.

Governed research pipelines require the exact contiguous migration history
from `0001` through `0030`.

Integration tests require an explicitly disposable or repository-private
database. The test target is never inferred from the application URL:

```text
SYSTEMATIC_FX_TEST_DATABASE_URL=postgresql://...
```

Ad-hoc schema edits are not permitted.

`0002_research_governance.sql` records every AI-visible Discovery exposure and
adds freeze/lineage checks needed before experiments can become evidence. A
prior migration remains immutable; all future changes receive a new ordered
file.

`0003_research_run_ledger.sql` adds an immutable canonical run-spec ledger and
append-preserved execution attempts. The SHA-256 fingerprint covers every
versioned data, split, feature, outcome, cost, execution, code, environment,
seed, signal, entry, barrier, terminal, and model input. A successful
fingerprint can exist only once; later identical requests are recorded as
duplicate skips instead of repeating the research.

`0004_code_snapshot_provenance.sql` requires every new run specification to
bind its base Git object to an exact content-addressed snapshot of runtime code,
configs, migrations, and research policy files. This preserves reproducibility
even while the working tree contains intentional uncommitted research changes.

`0005_campaign_level_run_ownership.sql` permits common feature, outcome, and AI
exposure runs to belong honestly to the campaign without inventing a hypothesis
owner. Strategy and performance runs remain required to reference their exact
experiment.

`0006_publication_outbox.sql` is the shared, checksum-identical migration used
by the research-site publisher. It coalesces private control-plane mutations
into a durable one-way publication request.

`0007_governed_discovery_exposures.sql` requires every Phase 1A AI-visible
exposure to reference the matching campaign-level RunSpec and makes those
exposures append-preserved. This keeps every query and slice tied to its complete
variable fingerprint.

`0008_phase1a_pattern_rollup_integrity.sql` prevents deletion or identity
rewrites of Phase 1A pattern roll-ups, requires their immutable context artifact,
and permits only monotonic support/time/status updates. Append-preserved QUERY
exposures, RunSpecs, and result artifacts remain the slice-level source of truth.

`0009_phase1a_artifact_lineage_integrity.sql` makes that source-of-truth claim
database-enforced. Artifacts referenced by Phase 1A Discovery exposures, run
attempt results or trade ledgers, pattern context, and feature-build manifests
cannot be updated or deleted. Governed feature partitions, their exact raw-source
links, and the linked source-file identity are likewise frozen; source-link
inserts must agree with the partition's recorded current/previous provenance.
The guards key through the Phase 1A campaign or feature-manifest lineage so
unrelated campaigns, pilot partitions, and artifacts keep their prior lifecycle.

`0010_research_execution_atomicity.sql` permits at most one active executor for
an immutable RunSpec and requires every duplicate skip to reference that same
RunSpec's completed success. It also makes Phase 1A AI visibility atomic with
the matching successful attempt and exact result artifact, so an append-preserved
exposure cannot survive a failed or half-committed execution.

`0011_phase1a_control_artifact_immutability.sql` extends the Phase 1A artifact
guard to the exact eligible calendar, sealed split, reconstructible code
snapshot, and screening registration document. It also protects experiment
registration artifacts through their campaign ownership, so provenance inputs
cannot be rewritten or deleted after registration.

`0012_campaign_level_validation_runs.sql` permits dedicated immutable
campaign-level validation/control RunSpecs. This gives partial-recovery audits a
truthful owner without pretending that control work belongs to a strategy
experiment or to a result-producing research query.

`0013_phase1a_outcome_replay.sql` keeps the generic RunSpec and attempt ledger as
the execution authority for the Phase 1A p5 MBP-10 outcome replay. It adds an
append-preserved replay manifest, an append-only `SOURCE_DATE_COMPLETE`
checkpoint artifact hash chain, and normalized summaries for every combination
of three frozen cost/execution scenarios, both directions, and all 484 TP/SL
cells. A success transition is rejected unless all 2,904 summaries and the exact
content-addressed result artifact commit atomically with the matching generic
attempt. Checkpoint and result artifacts remain beneath `data/derived`, and all
replay state is bound to the campaign-level canonical run fingerprint.

`0014_phase1a_outcome_completion_hardening.sql` adds the fail-closed completion
boundary for that replay without rewriting the applied `0013` history. New
cells must retain the frozen LONG/SHORT signal counts and per-fill scenario
costs. A success transition additionally requires all 485 source-date
checkpoints through `2023-08-31`, a finished and fully bound final checkpoint,
and exactly 1,613,172 detail rows whose cache, shard, input, result, and attempt
lineage hashes agree.

`0015_phase1a_outcome_constraints_validated.sql` performs the immediate legacy
scan for the signal-count and scenario-cost constraints introduced by `0014`.
Migration stops on any pre-existing weak cell rather than carrying it forward
as unvalidated research evidence.

`0016_phase1a_ordered_outcome_candidates.sql` adds the ordered p5-to-p1_05
candidate boundary. It registers append-only p5 byte-equivalence proofs,
direction-level screening decisions, the p1_05 cardinalities, and the complete
predecessor lineage that must authorize the second replay.

`0017_phase1a_ordered_trigger_routing.sql` routes the already validated p5 rows
through their original exact guards and p1_05 rows through the ordered-candidate
guards. This prevents a p5 transition from dereferencing p1-only predecessor
state while retaining a common append-preserved manifest table.

`0018_phase1a_outcome_decision_atomicity.sql` makes LONG and SHORT screening
decisions part of the same deferred success transaction. A replay manifest
cannot commit `SUCCEEDED` unless exactly two direction decisions already exist.

`0019_phase1a_outcome_audit_lineage_hardening.sql` reinstalls the ordered audit,
manifest, completion, decision, summary, and checkpoint guards with NULL-safe
`IS DISTINCT FROM` comparisons. It verifies the complete p5 audit and p1_05
predecessor lineage and permits only one canonical equivalence proof per p5
subject.

`0020_publication_run_progress.sql` extends the durable publication outbox to
governed RunSpec, run-attempt, and outcome-checkpoint changes so the public
projection can refresh as research execution progresses.

`0021_phase1a_outcome_manifest_record_alias_hardening.sql` replaces the routed
ordered-outcome manifest guard with unambiguous PL/pgSQL record and SQL table
aliases. This removes the p1 predecessor-audit lookup's unassigned-record
failure while preserving its fail-closed lineage checks and trigger routing.

`0022_bar_pattern_registry_governance.sql` freezes bar-pattern candidate
trial-to-RunSpec bindings and terminal lifecycles, defers exact attempt/trial
consistency checks to transaction commit, and makes registration, code,
Discovery evidence, global result, and terminal artifacts immutable.

`0023_bar_pattern_raw_dataset_lineage_fix.sql` corrects the governed
bar-pattern RunSpec matcher so the control-plane dataset row is bound to the
raw MBP-10 source manifest, while the derived selected-trade-bar manifest
remains independently bound in the candidate trial and RunSpec parameters.

`0024_bar_state_conditional_governance.sql` adds the append-only
`bar_state_artifact_links` registry for compact, content-addressed Discovery
evidence. It binds every feature, label, model, OOS-trade, global-result, and
terminal-result artifact to the exact frozen candidate trial, RunSpec, and
attempt; protects the Bar State campaign and experiment identities; enforces
the twelve-candidate preregistration boundary; and requires one atomic
successful-attempt and terminal-trial pair for each completed candidate. It
also refreshes the durable public-projection outbox whenever those governed
artifact links change and requests one immediate refresh on installation.

`0025_bar_state_raw_dataset_lineage_fix.sql` corrects the governed bar-state
RunSpec matcher so the control-plane dataset row is bound to the raw MBP-10
source manifest, while the derived selected-trade-bar manifest remains
independently bound in the candidate trial and RunSpec parameters.

`0026_bar_state_v2a_optimizer_cap_amendment.sql` adds an immutable dual-profile
governance dispatch for the failed State V2 campaign and its preregistered V2A
optimizer-cap amendment. It preserves the V2 identities and artifact schema,
binds V2A to its distinct configuration, candidate, engine, and artifact
namespace, and admits V2A only after proving exactly twelve started-and-failed
V2 attempts with no linked research evidence. Row locks and append guards
serialize the amendment against late V2 trials, RunSpecs, and attempts; exact
profile lineage and terminal/link validation continue through publication.

`0027_bar_state_v2b_parquet_schema_amendment.sql` adds a third immutable State
profile for the publication-only V2B correction. It keeps V2A's scientific
catalog and 50,000-iteration model policy unchanged, pins V2B's compliant
FEATURE list-child schema, and admits V2B only after proving exactly two clean
FAILED V2A attempts per candidate with no governed evidence. Campaign-row
locks freeze subsequent V2A experiments, trials, RunSpecs, attempts, and links
against registration races while retaining fail-clean aborts for active V2B
attempts if a live prerequisite drifts.

`0028_phase1a_p4_paired_outcomes.sql` adds the governed Phase 1A P4 pair batch
and simultaneous-release registries. It binds both preregistered liquidity-
transition candidates, their frozen configs and signal/input manifests, and
the previously observed outcome lineage before either replay may start. The
pair can publish its two economic surfaces and four screening decisions only
as one atomic release; partial completion is forbidden, while paired failure
and narrowly scoped unpaired-queue cleanup keep retry state terminal and
auditable.

`0029_m0b_governed_control_plane.sql` adds the finite-budget M0b search-data
control plane without granting holdout or promotion authority. One immutable
epoch is bound to one frozen, open, unrevealed campaign and to exact dataset,
CME calendar/contract, split, feature, label, cost, execution, engine,
dependency-lock, code, retry, and manifest identities. Its canonical document
also freezes strategy families, parameter ranges, the volatility-normalized
barrier grid, seeds, null controls, execution/session/roll assumptions,
admission rules, and REAL/NULL budgets. Candidates, canonical RunSpecs,
attempts, hash-chain checkpoints, and single-owner result artifacts must agree.
Successful attempts and `SCREENED_OUT`/`REGISTERED` candidates terminalize
atomically, NULL controls cannot register, and governed artifacts/campaign
identity are append-preserved.

`0030_m0b_numeric_admission_worker_api.sql` replaces the M0b admission marker
with exact integer thresholds and stores one append-only DB-derived decision
per result artifact. Its four leased worker capabilities can claim only
pre-registered candidates whose immutable CandidateWork artifact is bound to
the epoch, RunSpec, source build, features, labels, signals, and first-passage
store. They append bounded content-addressed checkpoints, recover expired
leases within the frozen retry cap, derive at most `REGISTERED` from immutable
integer search metrics, or record replay-safe failure. A separate NOLOGIN API
owner exposes only those four capabilities; the actual worker role has an exact
read allowlist and no direct table/sequence mutation, epoch/campaign mutation,
holdout, promotion, or candidate-generation authority.

CandidateWork v2 binds the candidate's exact rational volatility barrier and
evaluation-policy hash to the canonical candidate, RunSpec, cost, execution,
split, code, and source lineage. Lease bearer values are hidden from worker
logins and bound to the authenticated login. A terminal result must match the
latest complete checkpoint byte-for-byte in hash, size, metrics, and DB-derived
classification; complete checkpoints cannot be failed, superseded, or consumed
by stale recovery.
