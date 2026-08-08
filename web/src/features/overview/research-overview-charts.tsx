"use client";

import dynamic from "next/dynamic";

import type { ResearchSnapshot } from "@/domain/research/types";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

export function ResearchOverviewCharts({ snapshot }: { snapshot: ResearchSnapshot }) {
  const familyOption = {
    animationDuration: 600,
    grid: { left: 24, right: 16, top: 18, bottom: 30, containLabel: true },
    xAxis: {
      type: "category",
      data: snapshot.families.map((family) => family.id),
      axisLine: { lineStyle: { color: "rgba(16,19,15,.18)" } },
      axisTick: { show: false },
      axisLabel: { color: "#65695f", fontSize: 11, fontWeight: 700 },
    },
    yAxis: {
      type: "value",
      minInterval: 1,
      splitLine: { lineStyle: { color: "rgba(16,19,15,.08)" } },
      axisLabel: { color: "#80847a", fontSize: 10 },
    },
    tooltip: { trigger: "axis" },
    series: [
      {
        type: "bar",
        data: snapshot.families.map((family) => family.hypothesisCount),
        itemStyle: { color: "#10130f", borderRadius: [3, 3, 0, 0] },
        barMaxWidth: 42,
      },
    ],
  };
  const qualityOption = {
    animationDuration: 600,
    tooltip: { trigger: "item" },
    series: [
      {
        type: "pie",
        radius: ["58%", "82%"],
        center: ["50%", "48%"],
        label: { show: false },
        data: [
          { name: "Passed", value: snapshot.dataQuality.passedFiles, itemStyle: { color: "#1c7145" } },
          { name: "Failed", value: snapshot.dataQuality.failedFiles, itemStyle: { color: "#b8382f" } },
        ],
      },
    ],
    graphic: [
      { type: "text", left: "center", top: "39%", style: { text: snapshot.dataQuality.sourceFiles.toLocaleString(), font: "32px Georgia", fill: "#10130f", textAlign: "center" } },
      { type: "text", left: "center", top: "55%", style: { text: "SOURCE FILES", font: "700 10px Arial", fill: "#65695f", textAlign: "center" } },
    ],
  };
  return (
    <div className="two-column">
      <article className="panel">
        <div className="panel-header"><h3>Preregistered research space</h3><span className="panel-caption">Hypotheses by family</span></div>
        <ReactECharts className="chart" option={familyOption} />
      </article>
      <article className="panel">
        <div className="panel-header"><h3>Structural scan</h3><span className="panel-caption">Complete file coverage</span></div>
        <ReactECharts className="chart" option={qualityOption} />
      </article>
    </div>
  );
}
