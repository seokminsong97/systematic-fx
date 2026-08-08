import type { Metadata } from "next";

import { PatternLedger } from "@/features/patterns/pattern-ledger";
import { getLatestResearchSnapshot } from "@/server/repositories/research-publication";

export const metadata: Metadata = { title: "Patterns" };

export default async function PatternsPage() {
  const snapshot = await getLatestResearchSnapshot();
  return (
    <>
      <section className="page-hero">
        <div className="shell">
          <div className="eyebrow">Immutable observation ledger</div>
          <h1 className="page-title">Patterns</h1>
          <p className="page-description">
            What was seen, where it held, and where it broke. Rejections stay visible and promoted
            observations remain separate from validated strategies.
          </p>
        </div>
      </section>
      <PatternLedger initialSnapshot={snapshot} />
    </>
  );
}
