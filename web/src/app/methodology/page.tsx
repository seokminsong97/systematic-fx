import type { Metadata } from "next";

import { SectionHeading } from "@/components/ui/section-heading";

export const metadata: Metadata = { title: "Methodology" };

const principles = [
  ["01", "Preregister the search", "Parent hypotheses, model families, directions, and trial budgets are bounded before performance exposure."],
  ["02", "Name the authority boundary", "Conservative exclusions can authorize Screening while unresolved structural and reference-data gates continue to block Backtest, Paper, and Live claims."],
  ["03", "Separate discovery", "Observed patterns are recorded with regimes and counterexamples. They do not become validated strategies by narration."],
  ["04", "Freeze screening rules", "Conservative costs, execution assumptions, feature definitions, entry logic, exits, and candidate order are versioned before outcome replay."],
  ["05", "Respect multiplicity", "Variants and descendants consume the budget of their parent lineage. Rejected trials are retained in the denominator."],
  ["06", "Keep holdout sealed", "Finalists are bounded in advance and sealed results are not exposed until the registered reveal condition is met."],
  ["07", "Separate computation from decision", "Replay SUCCEEDED, screening REJECT, pattern OPEN, and audit RUNNING remain distinct states with different meanings."],
  ["08", "Publish append-only", "Each allowlisted public payload is schema-validated, content-hashed, and written as a new immutable revision without raw data, paths, or secrets."],
] as const;

export default function MethodologyPage() {
  return (
    <>
      <section className="page-hero">
        <div className="shell">
          <div className="eyebrow">How to read this ledger</div>
          <h1 className="page-title">Methodology</h1>
          <p className="page-description">
            This site is an evidence register, not a performance advertisement. Its structure is designed
            to make blockers, negative results, and unresolved uncertainty as legible as success.
          </p>
        </div>
      </section>
      <section className="section shell">
        <SectionHeading
          kicker="Publication principles"
          title="Rules before results."
          description="These controls define what can appear publicly and what each displayed state actually claims."
        />
        <div className="method-grid">
          {principles.map(([number, title, description]) => (
            <article className="method-card" key={number}>
              <span className="method-number">{number}</span>
              <h2>{title}</h2>
              <p>{description}</p>
            </article>
          ))}
        </div>
      </section>
      <section className="section shell">
        <div className="disclosure-callout">
          <div><span className="section-kicker">Boundary</span><h2>Public transparency is not database transparency.</h2></div>
          <p>
            Public readers receive the claims and evidence required to audit research progress. The private
            control plane retains raw inputs, full artifacts, internal identifiers, credentials, and details
            that would create security, licensing, or data-leakage risk.
          </p>
        </div>
      </section>
    </>
  );
}
