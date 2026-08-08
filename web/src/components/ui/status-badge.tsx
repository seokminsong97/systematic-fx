import { decisionTone, gateTone, plainStatusTone, readableStatus } from "@/domain/research/status";
import type { GateState, ResearchDecision, ScreeningDecisionLabel } from "@/domain/research/types";

type StatusBadgeProps =
  | { value: GateState; kind?: "gate" }
  | { value: ResearchDecision | ScreeningDecisionLabel; kind: "decision" }
  | { value: string; kind: "plain" };

export function StatusBadge(props: StatusBadgeProps) {
  const tone =
    props.kind === "decision"
      ? decisionTone(props.value)
      : props.kind === "plain"
        ? plainStatusTone(props.value)
        : gateTone(props.value);
  return <span className={`status-badge tone-${tone}`}>{readableStatus(props.value)}</span>;
}
