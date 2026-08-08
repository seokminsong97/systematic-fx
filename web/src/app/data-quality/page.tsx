import type { Metadata } from "next";

import { DataQualityDashboard } from "@/features/data-quality/data-quality-dashboard";
import { getLatestResearchSnapshot } from "@/server/repositories/research-publication";

export const metadata: Metadata = { title: "Data integrity" };

export default async function DataQualityPage() {
  const snapshot = await getLatestResearchSnapshot();
  return (
    <>
      <section className="page-hero">
        <div className="shell">
          <div className="eyebrow">Input qualification</div>
          <h1 className="page-title">Data integrity</h1>
          <p className="page-description">
            The admissibility boundary beneath every research claim—identity, coverage, structural checks,
            and the unresolved exceptions that prevent interpretation.
          </p>
        </div>
      </section>
      <DataQualityDashboard initialSnapshot={snapshot} />
    </>
  );
}
