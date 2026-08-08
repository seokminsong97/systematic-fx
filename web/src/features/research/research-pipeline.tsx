"use client";

import { MetricCard } from "@/components/ui/metric-card";
import { SectionHeading } from "@/components/ui/section-heading";
import { StatusBadge } from "@/components/ui/status-badge";
import type { GateState, ResearchSnapshot } from "@/domain/research/types";
import { useLiveSnapshot } from "@/lib/use-live-snapshot";

const phases = [
  { title: "Qualify", gateIds: ["source-identity", "structural-quality"] },
  { title: "Authorize screening", gateIds: ["screening-calendar", "sealed-splits", "screening-models"] },
  { title: "Discover", gateIds: ["discovery"] },
  { title: "Evaluate", gateIds: ["outcome-replay", "outcome-equivalence", "backtest-authority"] },
] as const;

function countOrDash(value: number | null): string {
  return value === null ? "—" : value.toLocaleString();
}

export function ResearchPipeline({ initialSnapshot }: { initialSnapshot: ResearchSnapshot }) {
  const { data: snapshot = initialSnapshot, isValidating } = useLiveSnapshot(initialSnapshot);

  return (
    <>
      <section className="section shell">
        <SectionHeading
          kicker="Campaign state"
          title="Execution, evidence, and authority stay separate."
          description="A successful computation can still produce a rejected research result. The campaign authority ceiling is recorded independently from both."
        />
        <div className="metric-grid">
          <MetricCard label="Current stage" value={snapshot.campaign.stage.replaceAll("_", " ")} note={snapshot.campaign.status} />
          <MetricCard label="Screening authorized" value={snapshot.campaign.screeningAuthorized ? "Yes" : "No"} note={snapshot.program.policyState.replaceAll("_", " ")} />
          <MetricCard label="Maximum authority" value={snapshot.program.maximumAuthority.replaceAll("_", " ")} note="No Backtest, Paper, or Live claim" />
          <MetricCard label="Run ledger" value={`${snapshot.summary.succeededRuns}/${snapshot.summary.runSpecs}`} note={`${snapshot.summary.runningRuns} running · revision ${snapshot.metadata.revision}${isValidating ? " · refreshing" : ""}`} />
        </div>
      </section>

      <section className="section shell">
        <SectionHeading
          kicker="Dependency chain"
          title="A visible path from source identity to decision."
          description="Each gate states whether it governs conservative Screening, later Backtest authority, or both. A screening exclusion never rewrites the underlying structural failure."
        />
        <div className="phase-list">
          {phases.map((phase, phaseIndex) => (
            <article className="phase" key={phase.title}>
              <header className="phase-header">
                <span className="phase-number">{String(phaseIndex + 1).padStart(2, "0")}</span>
                <h2>{phase.title}</h2>
              </header>
              <div className="phase-gates">
                {phase.gateIds.map((id) => {
                  const gate = snapshot.gates.find((candidate) => candidate.id === id);
                  if (!gate) return null;
                  return (
                    <div className="phase-gate" key={gate.id}>
                      <div><span className="gate-scope">{gate.scope}</span><h3>{gate.label}</h3><p>{gate.detail}</p></div>
                      <StatusBadge value={gate.state as GateState} />
                    </div>
                  );
                })}
              </div>
            </article>
          ))}
        </div>
      </section>

      <section className="section shell">
        <SectionHeading
          kicker="Ordered candidates"
          title="Replay completion is not the decision."
          description="Candidates run in a frozen order. Each card keeps the technical replay, independent equivalence audit, and directional screening decisions on separate lines."
        />
        <div className="candidate-grid">
          {snapshot.outcomeCandidates.map((candidate) => (
            <article className="candidate-card" key={candidate.id}>
              <div className="candidate-card-header">
                <div>
                  <span className="eyebrow">Candidate {String(candidate.order).padStart(2, "0")} · {candidate.family}</span>
                  <h2>{candidate.title}</h2>
                  <span className="pattern-key">{candidate.id}</span>
                </div>
                <StatusBadge value={candidate.stage} kind="plain" />
              </div>

              <div className="candidate-status-grid">
                <div><span className="meta-label">Replay execution</span><strong><StatusBadge value={candidate.replay.status} kind="plain" /></strong></div>
                <div><span className="meta-label">Equivalence audit</span><strong><StatusBadge value={candidate.validation.status} kind="plain" /></strong></div>
                <div><span className="meta-label">Discovery occurrences</span><strong>{candidate.discoveryOccurrences.toLocaleString()}</strong></div>
                <div><span className="meta-label">Replay dates</span><strong>{candidate.replay.completedDates.toLocaleString()} / {countOrDash(candidate.replay.plannedDates)}</strong></div>
                <div><span className="meta-label">Summary cells</span><strong>{candidate.replay.summaryCells.toLocaleString()} / {countOrDash(candidate.replay.expectedSummaryCells)}</strong></div>
                <div><span className="meta-label">Surface complete</span><strong>{candidate.replay.surfaceComplete ? "Yes" : "No"}</strong></div>
              </div>

              <div className="direction-decisions">
                {candidate.decisions.map((decision) => (
                  <div className="direction-decision" key={decision.direction}>
                    <div className="direction-decision-title">
                      <strong>{decision.direction}</strong>
                      <StatusBadge value={decision.label} kind="decision" />
                    </div>
                    {decision.reasonCategories.length > 0 ? (
                      <ul>{decision.reasonCategories.map((reason) => <li key={reason}>{reason}</li>)}</ul>
                    ) : (
                      <p>No directional screening decision is registered yet.</p>
                    )}
                  </div>
                ))}
              </div>
              <p className="panel-caption candidate-authority">{candidate.authorityNote}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="section shell">
        <SectionHeading
          kicker="Governed execution"
          title="Canonical RunSpecs, resolved by outcome."
          description="Historical retries remain in the attempt count. A duplicate reuse is not a failure, and a technical success is never counted as a screening survivor."
        />
        <div className="data-table-wrap">
          <table className="data-table compact-table run-ledger-table">
            <thead><tr><th>Run kind</th><th>Total</th><th>Succeeded</th><th>Running</th><th>Failed</th></tr></thead>
            <tbody>
              {snapshot.runLedger.byKind.map((item) => (
                <tr key={item.kind}>
                  <td><strong>{item.kind.replaceAll("_", " ")}</strong></td>
                  <td>{item.total.toLocaleString()}</td>
                  <td>{item.succeeded.toLocaleString()}</td>
                  <td>{item.running.toLocaleString()}</td>
                  <td>{(item.failed + item.rejected + item.cancelled).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="panel-caption" style={{ marginTop: 12 }}>
          {snapshot.runLedger.attempts.toLocaleString()} attempts across {snapshot.runLedger.specs.toLocaleString()} canonical RunSpecs · {snapshot.runLedger.reusedSuccesses.toLocaleString()} duplicate requests reused an existing success.
        </p>
      </section>

      <section className="section shell">
        <SectionHeading
          kicker="Decision semantics"
          title="The words mean exactly one thing."
          description="The public ledger names the layer being described so completion cannot be mistaken for evidence or trading authority."
        />
        <div className="method-grid">
          <article className="method-card"><span className="method-number">01</span><h2>Replay succeeded</h2><p>The governed computation and its registered surface completed. It says nothing yet about economic survival.</p></article>
          <article className="method-card"><span className="method-number">02</span><h2>Screening rejected</h2><p>The complete result failed its preregistered stability rule. The failed claim remains visible.</p></article>
          <article className="method-card"><span className="method-number">03</span><h2>Audit passed</h2><p>An independent uninterrupted replay matched the resumed result. Absence of an audit row means pending, not failed.</p></article>
          <article className="method-card"><span className="method-number">04</span><h2>Authority ceiling</h2><p>Even a screening survivor cannot be called a Backtest pass and grants no Paper or Live authority.</p></article>
        </div>
      </section>
    </>
  );
}
