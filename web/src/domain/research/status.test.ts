import { describe, expect, it } from "vitest";

import { decisionTone, gateTone, plainStatusTone, readableStatus } from "./status";

describe("research status presentation", () => {
  it("keeps pass, blocked, and rejection visually distinct", () => {
    expect(gateTone("PASS")).toBe("positive");
    expect(gateTone("BLOCKED")).toBe("blocked");
    expect(decisionTone("SCREENING_REJECT")).toBe("negative");
    expect(decisionTone("SCREENING_SURVIVOR")).toBe("positive");
    expect(plainStatusTone("PROMOTED")).toBe("positive");
    expect(plainStatusTone("FAILED")).toBe("negative");
  });

  it("turns storage labels into readable copy", () => {
    expect(readableStatus("NOT_OBSERVED")).toBe("Not observed");
    expect(readableStatus("OUTCOME_VALIDATION")).toBe("Outcome validation");
  });
});
