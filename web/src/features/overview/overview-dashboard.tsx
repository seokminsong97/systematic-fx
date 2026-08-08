"use client";

import Link from "next/link";

import { MetricCard } from "@/components/ui/metric-card";
import { SectionHeading } from "@/components/ui/section-heading";
import { StatusBadge } from "@/components/ui/status-badge";
import type { GateState, ResearchSnapshot } from "@/domain/research/types";
import { useLiveSnapshot } from "@/lib/use-live-snapshot";
import { ResearchOverviewCharts } from "./research-overview-charts";

export function OverviewDashboard({ initialSnapshot }: { initialSnapshot: ResearchSnapshot }) {
  const { data: snapshot = initialSnapshot, isValidating } = useLiveSnapshot(initialSnapshot);
  return (
    <>
      <section className="hero">
        <div className="shell">
          <div className="eyebrow">Public evidence · revision {snapshot.metadata.revision}</div>
          <div className="hero-grid">
            <h1>Research before <em>conviction.</em></h1>
            <div>
              <p className="hero-copy">A live account of every hypothesis, blocker, failed gate, and promotion decision in the Systematic FX program.</p>
              <div className="hero-meta">
                <div><span className="meta-label">Campaign</span><span className="meta-value">{snapshot.campaign.name}</span></div>
                <div><span className="meta-label">Current stage</span><span className="meta-value">{snapshot.campaign.stage.replaceAll("_", " ")}</span></div>
                <div><span className="meta-label">Data as of</span><span className="meta-value">{snapshot.metadata.dataAsOf}</span></div>
                <div><span className="meta-label">Connection</span><span className="meta-value">{isValidating ? "Refreshing…" : "Live projection"}</span></div>
              </div>
            </div>
          </div>
          <div className="alert-bar">
            <strong>Screening-only authority</strong>
            <p>{snapshot.campaign.summary}</p>
          </div>
        </div>
      </section>

      <section className="section shell">
        <SectionHeading kicker="Current state" title="The ledger, without the victory lap." description="Completion means a computation ran. Passing means the evidence survived every gate. This dashboard keeps those claims separate." />
        <div className="metric-grid">
          <MetricCard label="Preregistered hypotheses" value={snapshot.summary.hypotheses} note={`${snapshot.summary.families} bounded research families`} />
          <MetricCard label="Observed patterns" value={snapshot.summary.observedPatterns} note={`${snapshot.summary.discoverySlices} governed Discovery slices`} />
          <MetricCard label="Canonical runs" value={`${snapshot.summary.succeededRuns}/${snapshot.summary.runSpecs}`} note={`${snapshot.summary.runningRuns} running · ${snapshot.summary.failedRuns} failed`} />
          <MetricCard label="Survivors / Rejected" value={`${snapshot.summary.screeningSurvivors}/${snapshot.summary.screeningRejected}`} note={`${snapshot.summary.pendingCandidates} ordered candidate pending`} />
        </div>
      </section>

      <section className="section shell">
        <SectionHeading kicker="Evidence map" title="What is known, and what is not." description="Public summaries are generated from a separate projection database. Every open browser refreshes when a newer revision is published." />
        <ResearchOverviewCharts snapshot={snapshot} />
      </section>

      <section className="section shell">
        <SectionHeading kicker="Research gates" title="Progress is gated, not narrated." description="A downstream step cannot become green while an upstream scientific or execution prerequisite remains unresolved." />
        <div className="gate-grid">
          {snapshot.gates.map((gate, index) => (
            <article className="gate-card" key={gate.id}>
              <span className="gate-index">{String(index + 1).padStart(2, "0")}</span>
              <div><span className="gate-scope">{gate.scope}</span><h3>{gate.label}</h3><p>{gate.detail}</p></div>
              <StatusBadge value={gate.state as GateState} />
            </article>
          ))}
        </div>
      </section>

      <section className="section shell">
        <SectionHeading kicker="Research space" title="Six families. Sixty explicit claims." description="Each family owns ten a-priori parent hypotheses. Variants and descendants remain attached to that original multiplicity budget." />
        <div className="family-grid">
          {snapshot.families.map((family) => (
            <Link className="family-card" href={`/hypotheses?family=${family.id}`} key={family.id}>
              <span className="family-id">{family.id}</span><span className="family-count">{family.hypothesisCount} hypotheses</span>
              <h3>{family.title}</h3><p>{family.description}</p>
            </Link>
          ))}
        </div>
      </section>

      <section className="section shell">
        <SectionHeading kicker="Immutable history" title="The work, in sequence." description="Failed checks and blocked transitions stay visible. Later evidence adds a revision; it does not rewrite the past." />
        <div className="timeline">
          {snapshot.timeline.map((item, index) => (
            <article className="timeline-item" key={`${item.date}-${item.title}-${index}`}>
              <span className="timeline-date">{item.date}</span>
              <StatusBadge value={item.state} kind="plain" />
              <div><h3>{item.title}</h3><p>{item.detail}</p></div>
            </article>
          ))}
        </div>
      </section>
    </>
  );
}
