import { OverviewDashboard } from "@/features/overview/overview-dashboard";
import { getLatestResearchSnapshot } from "@/server/repositories/research-publication";

export default async function HomePage() {
  const snapshot = await getLatestResearchSnapshot();
  return <OverviewDashboard initialSnapshot={snapshot} />;
}
