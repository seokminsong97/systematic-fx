import type { Metadata } from "next";

import { HypothesisExplorer } from "@/features/hypotheses/hypothesis-explorer";
import { getLatestResearchSnapshot } from "@/server/repositories/research-publication";

export const metadata: Metadata = { title: "Hypotheses" };

export default async function HypothesesPage({ searchParams }: { searchParams: Promise<{ family?: string }> }) {
  const [snapshot, params] = await Promise.all([getLatestResearchSnapshot(), searchParams]);
  return (
    <>
      <section className="page-hero"><div className="shell"><div className="eyebrow">Preregistered research space</div><h1 className="page-title">Hypotheses</h1><p className="page-description">Every parent claim is registered before performance exposure. Retained, failed, and rejected descendants remain attached to their original lineage.</p></div></section>
      <section className="section shell"><HypothesisExplorer initialSnapshot={snapshot} initialFamily={params.family ?? "ALL"} /></section>
    </>
  );
}
