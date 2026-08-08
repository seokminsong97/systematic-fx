"use client";

import dynamic from "next/dynamic";

import { MetricCard } from "@/components/ui/metric-card";
import { SectionHeading } from "@/components/ui/section-heading";
import { StatusBadge } from "@/components/ui/status-badge";
import type { ResearchSnapshot } from "@/domain/research/types";
import { useLiveSnapshot } from "@/lib/use-live-snapshot";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

export function DataQualityDashboard({ initialSnapshot }: { initialSnapshot: ResearchSnapshot }) {
  const { data: snapshot = initialSnapshot } = useLiveSnapshot(initialSnapshot);
  const quality = snapshot.dataQuality;
  const sourceCoverage = quality.sourceFiles === 0
    ? 0
    : Math.round((quality.identifiedFiles / quality.sourceFiles) * 100);
  const chartOption = {
    animationDuration: 500,
    grid: { left: 38, right: 18, top: 28, bottom: 48 },
    tooltip: { trigger: "axis" },
    xAxis: {
      type: "category",
      data: quality.failedDates.map((item) => item.date),
      axisLabel: { color: "#65695f", rotate: quality.failedDates.length > 8 ? 40 : 0 },
      axisLine: { lineStyle: { color: "rgba(16,19,15,.18)" } },
    },
    yAxis: {
      type: "value",
      minInterval: 1,
      axisLabel: { color: "#65695f" },
      splitLine: { lineStyle: { color: "rgba(16,19,15,.08)" } },
    },
    series: [{
      type: "bar",
      data: quality.failedDates.map((item) => item.violations),
      itemStyle: { color: "#b8382f" },
      barMaxWidth: 32,
    }],
  };

  return (
    <>
      <section className="section shell">
        <SectionHeading
          kicker="Latest complete scan"
          title="Data earns the right to be researched."
          description={`Coverage ${quality.coverageStart ?? "not set"} — ${quality.coverageEnd ?? "not set"}. Every metric below is read from the current public revision.`}
        />
        <div className="metric-grid">
          <MetricCard label="Source identity" value={`${sourceCoverage}%`} note={`${quality.identifiedFiles.toLocaleString()} / ${quality.sourceFiles.toLocaleString()} files`} />
          <MetricCard label="Passed / Failed files" value={`${quality.passedFiles}/${quality.failedFiles}`} note={quality.datasetStatus} />
          <MetricCard label="Events scanned" value={quality.eventRows.toLocaleString()} note={`${quality.rowGroups.toLocaleString()} row groups`} />
          <MetricCard label="Hard violations" value={quality.hardViolations.toLocaleString()} note={`${quality.warningSymbols} partial-metadata symbols`} />
        </div>
      </section>

      <section className="section shell">
        <SectionHeading
          kicker="Failure surface"
          title="The exceptions stay inspectable."
          description="The public projection exposes aggregate failure dates and counts, not raw market data, filesystem paths, credentials, or provider payloads."
        />
        <div className="two-column">
          <div className="panel">
            <div className="panel-header"><h3>Hard violations by source date</h3><span className="panel-caption">Revision {snapshot.metadata.revision}</span></div>
            {quality.failedDates.length > 0
              ? <ReactECharts option={chartOption} className="chart" />
              : <div className="chart-empty">No failed source dates in the latest complete scan.</div>}
          </div>
          <div className="panel">
            <div className="panel-header"><h3>Qualification state</h3><StatusBadge value={quality.datasetStatus} kind="plain" /></div>
            <dl className="quality-facts">
              <div><dt>Coverage start</dt><dd>{quality.coverageStart ?? "Pending"}</dd></div>
              <div><dt>Coverage end</dt><dd>{quality.coverageEnd ?? "Pending"}</dd></div>
              <div><dt>Registered source files</dt><dd>{quality.sourceFiles.toLocaleString()}</dd></div>
              <div><dt>Full-content identities</dt><dd>{quality.identifiedFiles.toLocaleString()}</dd></div>
              <div><dt>Screening-eligible dates</dt><dd>{quality.eligibleDays.toLocaleString()}</dd></div>
              <div><dt>Conservatively excluded dates</dt><dd>{quality.ineligibleDays.toLocaleString()}</dd></div>
              <div><dt>Provider warnings</dt><dd>{quality.warningSymbols.toLocaleString()}</dd></div>
            </dl>
          </div>
        </div>
      </section>

      {quality.failedDates.length > 0 && (
        <section className="section shell">
          <SectionHeading
            kicker="Exception register"
            title="Failed source dates"
            description="Only sanitized counts are published; investigation details remain in the private research control plane."
          />
          <div className="data-table-wrap">
            <table className="data-table compact-table">
              <thead><tr><th>Source date</th><th>Hard violations</th><th>Disposition</th></tr></thead>
              <tbody>
                {quality.failedDates.map((item) => (
                  <tr key={item.date}>
                    <td>{item.date}</td>
                    <td>{item.violations.toLocaleString()}</td>
                    <td><StatusBadge value="FAIL" /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </>
  );
}
