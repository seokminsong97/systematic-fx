"use client";

import { SectionHeading } from "@/components/ui/section-heading";
import { StatusBadge } from "@/components/ui/status-badge";
import type { ResearchSnapshot } from "@/domain/research/types";
import { useLiveSnapshot } from "@/lib/use-live-snapshot";

export function PatternLedger({ initialSnapshot }: { initialSnapshot: ResearchSnapshot }) {
  const { data: snapshot = initialSnapshot } = useLiveSnapshot(initialSnapshot);

  return (
    <section className="section shell">
      <SectionHeading
        kicker="Discovery observations"
        title="Patterns are evidence, not strategies."
        description="A pattern is a governed Discovery observation. Its ledger lifecycle, ordered replay state, and economic screening decision are intentionally separate."
      />
      {snapshot.patterns.length === 0 ? (
        <div className="empty-state">
          <span className="empty-state-icon" aria-hidden="true">Ø</span>
          <h2>No public patterns registered</h2>
          <p>
            Discovery is currently gated. When a pattern is recorded in the research ledger,
            the publication worker will add it here in a new immutable revision.
          </p>
        </div>
      ) : (
        <div className="pattern-grid">
          {snapshot.patterns.map((pattern) => (
            <article className="pattern-card" key={pattern.id}>
              <div className="pattern-card-top">
                <span className="pattern-key">{pattern.family} · {pattern.id}</span>
                <StatusBadge value={pattern.evidenceState} kind="plain" />
              </div>
              <h2>{pattern.title}</h2>
              <p>{pattern.description}</p>
              <dl className="pattern-facts">
                <div><dt>Direction</dt><dd>{pattern.direction}</dd></div>
                <div><dt>Support</dt><dd>{pattern.supportCount.toLocaleString()}</dd></div>
                <div><dt>Counterexamples</dt><dd>{pattern.counterexampleCount}</dd></div>
                <div><dt>Observed slices</dt><dd>{pattern.observedSlices.toLocaleString()}</dd></div>
                <div><dt>Pattern lifecycle</dt><dd><StatusBadge value={pattern.status} kind="plain" /></dd></div>
                <div><dt>Screening decision</dt><dd><StatusBadge value={pattern.screeningDecision} kind="decision" /></dd></div>
              </dl>
              <p className="panel-caption">Updated {new Date(pattern.updatedAt).toLocaleString()} · revision {snapshot.metadata.revision}</p>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}
