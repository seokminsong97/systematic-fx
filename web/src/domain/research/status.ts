import type { GateState, ResearchDecision, ScreeningDecisionLabel } from "./types";

export type StatusTone = "positive" | "warning" | "negative" | "neutral" | "blocked";

export function gateTone(state: GateState): StatusTone {
  return {
    PASS: "positive",
    WARN: "warning",
    FAIL: "negative",
    PENDING: "neutral",
    BLOCKED: "blocked",
  }[state] as StatusTone;
}

export function decisionTone(decision: ResearchDecision | ScreeningDecisionLabel): StatusTone {
  return {
    NOT_OBSERVED: "neutral",
    DISCOVERY_OBSERVED: "warning",
    BLOCKED: "blocked",
    OUTCOME_RUNNING: "warning",
    SCREENING_REJECT: "negative",
    SCREENING_SURVIVOR: "positive",
    MIXED: "warning",
    PENDING: "neutral",
    FAILED: "negative",
  }[decision] as StatusTone;
}

export function plainStatusTone(value: string): StatusTone {
  if (["PASS", "PASSED", "SUCCEEDED", "SCREENING_SURVIVOR", "PROMOTED", "READY", "VALIDATED", "PAPER_ELIGIBLE"].includes(value)) {
    return "positive";
  }
  if (["WARN", "RUNNING", "RETAINED", "FROZEN", "INCONCLUSIVE"].includes(value)) {
    return "warning";
  }
  if (["FAIL", "FAILED", "REJECTED", "SCREENING_REJECT", "SCREENING_REJECTED", "ABORTED"].includes(value)) {
    return "negative";
  }
  if (value === "BLOCKED") return "blocked";
  return "neutral";
}

export function readableStatus(value: string): string {
  return value.replaceAll("_", " ").toLowerCase().replace(/^./, (letter) => letter.toUpperCase());
}
