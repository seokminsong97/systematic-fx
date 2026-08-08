export type GateState = "PASS" | "WARN" | "FAIL" | "PENDING" | "BLOCKED";
export type GateScope = "SCREENING" | "BACKTEST" | "BOTH";
export type ResearchDecision =
  | "NOT_OBSERVED"
  | "DISCOVERY_OBSERVED"
  | "BLOCKED"
  | "OUTCOME_RUNNING"
  | "SCREENING_REJECT"
  | "SCREENING_SURVIVOR"
  | "MIXED"
  | "FAILED";
export type ScreeningDecisionLabel =
  | "PENDING"
  | "SCREENING_REJECT"
  | "SCREENING_SURVIVOR"
  | "MIXED";
export type CandidateStage =
  | "DISCOVERY_OBSERVED"
  | "BLOCKED"
  | "NOT_STARTED"
  | "REPLAY_QUEUED"
  | "REPLAY_RUNNING"
  | "VALIDATION_RUNNING"
  | "DECISION_PENDING"
  | "SCREENING_REJECTED"
  | "SCREENING_SURVIVOR"
  | "MIXED_DECISION"
  | "FAILED";

export interface ResearchMetadata {
  schemaVersion: "2.0.0";
  revision: number;
  dataAsOf: string;
  publishedAt: string;
  sourceRevision: string;
  disclosurePolicyVersion: "2.0.0";
}

export interface ProgramAuthority {
  mode: "SCREENING_ONLY";
  policyState: "FROZEN_SCREENING_POLICY";
  maximumAuthority: "SCREENING_SURVIVOR";
  backtestEligible: false;
  paperEligible: false;
  liveEligible: false;
  disclosure: string;
}

export interface CampaignSummary {
  key: string;
  name: string;
  status: "DRAFT" | "FROZEN" | "RUNNING" | "CLOSED" | "ABORTED";
  stage:
    | "DISCOVERY"
    | "OUTCOME_SCREENING"
    | "OUTCOME_REPLAY"
    | "OUTCOME_VALIDATION"
    | "ORDERED_SCREENING";
  screeningAuthorized: boolean;
  researchEligible: boolean;
  strategyVariantBudget: number;
  sealedHoldoutFinalistBudget: number;
  summary: string;
}

export interface Gate {
  id: string;
  label: string;
  state: GateState;
  scope: GateScope;
  detail: string;
}

export interface ResearchFamily {
  id: string;
  title: string;
  question: string;
  description: string;
  hypothesisCount: number;
}

export interface Hypothesis {
  id: string;
  family: string;
  title: string;
  direction: "LONG" | "SHORT" | "BOTH";
  modelFamily: string;
  hypothesis: string;
  entryCondition: string;
  economicRationale: string;
  features: string[];
  status:
    | "PROPOSED"
    | "REGISTERED"
    | "RUNNING"
    | "RETAINED"
    | "FROZEN"
    | "REJECTED"
    | "FAILED";
  decision: ResearchDecision;
  observedPatternIds: string[];
  supportCount: number;
  updatedAt: string | null;
  visibility: "PUBLIC_NOW" | "PUBLIC_AFTER_FREEZE" | "PUBLIC_AFTER_CAMPAIGN_CLOSE";
}

export interface Pattern {
  id: string;
  family: string;
  title: string;
  status: "OPEN" | "REGISTERED" | "REJECTED" | "PROMOTED";
  direction: "LONG" | "SHORT" | "BOTH" | "NONE";
  description: string;
  parentHypothesisIds: string[];
  counterexampleCount: number;
  supportCount: number;
  observedSlices: number;
  evidenceState: CandidateStage;
  screeningDecision: ScreeningDecisionLabel;
  updatedAt: string;
}

export interface RunKindSummary {
  kind: string;
  total: number;
  succeeded: number;
  running: number;
  queued: number;
  failed: number;
  rejected: number;
  cancelled: number;
}

export interface ScreeningDecision {
  direction: "LONG" | "SHORT";
  label: Exclude<ScreeningDecisionLabel, "MIXED">;
  positiveRegionSize: number | null;
  reasonCategories: string[];
}

export interface OutcomeCandidate {
  id: string;
  order: number;
  title: string;
  family: string;
  parentHypothesisIds: string[];
  stage: CandidateStage;
  discoveryOccurrences: number;
  replay: {
    status: "NOT_STARTED" | "BLOCKED" | "QUEUED" | "RUNNING" | "SUCCEEDED" | "FAILED";
    completedDates: number;
    plannedDates: number | null;
    sourceSlices: number | null;
    sourceOccurrences: number | null;
    scenarioCount: number | null;
    directionCount: number | null;
    cellsPerSurface: number | null;
    summaryCells: number;
    expectedSummaryCells: number | null;
    detailRecords: number | null;
    surfaceComplete: boolean;
    startedAt: string | null;
    finishedAt: string | null;
  };
  validation: {
    kind: "UNINTERRUPTED_RESUME_BYTE_EQUIVALENCE";
    status: "PENDING" | "RUNNING" | "PASSED" | "FAILED";
    updatedAt: string | null;
  };
  decisions: ScreeningDecision[];
  authorityNote: string;
}

export interface ResearchSnapshot {
  metadata: ResearchMetadata;
  program: ProgramAuthority;
  campaign: CampaignSummary;
  summary: {
    families: number;
    hypotheses: number;
    observedPatterns: number;
    discoverySlices: number;
    queryExposures: number;
    runSpecs: number;
    runAttempts: number;
    succeededRuns: number;
    runningRuns: number;
    failedRuns: number;
    reusedRuns: number;
    outcomeCandidates: number;
    evaluatedCandidates: number;
    screeningSurvivors: number;
    screeningRejected: number;
    pendingCandidates: number;
    blockedCandidates: number;
  };
  dataQuality: {
    datasetStatus: "REGISTERED" | "VALIDATING" | "READY" | "REJECTED" | "RETIRED";
    sourceFiles: number;
    identifiedFiles: number;
    passedFiles: number;
    failedFiles: number;
    rowGroups: number;
    eventRows: number;
    hardViolations: number;
    warningSymbols: number;
    coverageStart: string | null;
    coverageEnd: string | null;
    eligibleDays: number;
    ineligibleDays: number;
    failedDates: Array<{ date: string; violations: number }>;
  };
  gates: Gate[];
  families: ResearchFamily[];
  hypotheses: Hypothesis[];
  patterns: Pattern[];
  runLedger: {
    specs: number;
    attempts: number;
    reusedSuccesses: number;
    byKind: RunKindSummary[];
  };
  outcomeCandidates: OutcomeCandidate[];
  timeline: Array<{ date: string; state: string; title: string; detail: string }>;
}
