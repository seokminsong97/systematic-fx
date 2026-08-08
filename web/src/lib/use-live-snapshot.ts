"use client";

import useSWR from "swr";

import type { ResearchSnapshot } from "@/domain/research/types";

async function fetchSnapshot(url: string): Promise<ResearchSnapshot> {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Research refresh failed with HTTP ${response.status}`);
  }
  return response.json() as Promise<ResearchSnapshot>;
}

export function useLiveSnapshot(initialData: ResearchSnapshot) {
  const configured = Number(process.env.NEXT_PUBLIC_REFRESH_INTERVAL_MS ?? "15000");
  const refreshInterval = Number.isFinite(configured) && configured >= 1000 ? configured : 15000;
  return useSWR("/api/research/snapshot", fetchSnapshot, {
    fallbackData: initialData,
    refreshInterval,
    revalidateOnFocus: true,
    keepPreviousData: true,
  });
}
