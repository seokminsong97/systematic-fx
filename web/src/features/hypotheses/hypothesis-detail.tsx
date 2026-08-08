"use client";

import { StatusBadge } from "@/components/ui/status-badge";
import type { ResearchSnapshot } from "@/domain/research/types";
import { useLiveSnapshot } from "@/lib/use-live-snapshot";

export function HypothesisDetail({ id, initialSnapshot }: { id: string; initialSnapshot: ResearchSnapshot }) {
  const { data: snapshot = initialSnapshot } = useLiveSnapshot(initialSnapshot);
  const hypothesis = snapshot.hypotheses.find((item) => item.id === id);
  if (!hypothesis) {
    return (
      <section className="section shell">
        <div className="empty-state">
          <h1>This hypothesis is no longer in the public projection.</h1>
          <p>The live revision changed after this page loaded. Return to the hypothesis ledger to continue.</p>
        </div>
      </section>
    );
  }
  return (
    <>
      <section className="page-hero">
        <div className="shell">
          <div className="eyebrow">{hypothesis.family} · {hypothesis.id}</div>
          <h1 className="page-title">{hypothesis.title}</h1>
          <p className="page-description">{hypothesis.hypothesis}</p>
        </div>
      </section>
      <div className="shell detail-grid">
        <div>
          <section className="detail-section"><h2>Entry concept</h2><p>{hypothesis.entryCondition}</p></section>
          <section className="detail-section"><h2>Economic rationale</h2><p>{hypothesis.economicRationale}</p></section>
          <section className="detail-section"><h2>Feature boundary</h2><div className="feature-list">{hypothesis.features.map((feature) => <span className="feature-chip" key={feature}>{feature}</span>)}</div></section>
          <section className="detail-section">
            <h2>Observed pattern links</h2>
            {hypothesis.observedPatternIds.length > 0
              ? <div className="feature-list">{hypothesis.observedPatternIds.map((patternId) => <span className="feature-chip" key={patternId}>{patternId}</span>)}</div>
              : <p>No governed Discovery pattern is linked to this parent hypothesis yet.</p>}
          </section>
        </div>
        <aside className="detail-sidebar">
          <div className="detail-fact"><span className="meta-label">Research stage</span><strong><StatusBadge value={hypothesis.status} kind="plain" /></strong></div>
          <div className="detail-fact"><span className="meta-label">Decision</span><strong><StatusBadge value={hypothesis.decision} kind="decision" /></strong></div>
          <div className="detail-fact"><span className="meta-label">Direction</span><strong>{hypothesis.direction}</strong></div>
          <div className="detail-fact"><span className="meta-label">Model family</span><strong>{hypothesis.modelFamily.replaceAll("_", " ")}</strong></div>
          <div className="detail-fact"><span className="meta-label">Observed patterns</span><strong>{hypothesis.observedPatternIds.length}</strong></div>
          <div className="detail-fact"><span className="meta-label">Discovery support</span><strong>{hypothesis.supportCount.toLocaleString()}</strong></div>
          <div className="detail-fact"><span className="meta-label">Live revision</span><strong>{snapshot.metadata.revision}</strong></div>
        </aside>
      </div>
    </>
  );
}
