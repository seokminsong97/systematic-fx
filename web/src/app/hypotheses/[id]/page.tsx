import type { Metadata } from "next";
import { notFound } from "next/navigation";

import { HypothesisDetail } from "@/features/hypotheses/hypothesis-detail";
import { getLatestResearchSnapshot } from "@/server/repositories/research-publication";

export async function generateMetadata({ params }: { params: Promise<{ id: string }> }): Promise<Metadata> {
  const { id } = await params;
  return { title: id };
}

export default async function HypothesisPage({ params }: { params: Promise<{ id: string }> }) {
  const [{ id }, snapshot] = await Promise.all([params, getLatestResearchSnapshot()]);
  if (!snapshot.hypotheses.some((item) => item.id === id)) notFound();
  return <HypothesisDetail id={id} initialSnapshot={snapshot} />;
}
