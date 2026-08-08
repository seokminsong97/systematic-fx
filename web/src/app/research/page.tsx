import type { Metadata } from "next";

import { ResearchPipeline } from "@/features/research/research-pipeline";
import { getLatestResearchSnapshot } from "@/server/repositories/research-publication";

export const metadata: Metadata = { title: "Research pipeline" };

export default async function ResearchPage() {
  const snapshot = await getLatestResearchSnapshot();
  return (
    <>
      <section className="page-hero">
        <div className="shell">
          <div className="eyebrow">Scientific control plane</div>
          <h1 className="page-title">Research pipeline</h1>
          <p className="page-description">
            A live, dependency-aware view of what completed, what the evidence rejected, what is under validation, and what remains blocked.
          </p>
        </div>
      </section>
      <ResearchPipeline initialSnapshot={snapshot} />
    </>
  );
}
