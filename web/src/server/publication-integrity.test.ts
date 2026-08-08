import { describe, expect, it } from "vitest";

import { publicPayloadSha256 } from "./publication-integrity";

describe("public projection integrity", () => {
  it("uses the same canonical key ordering as the publisher", () => {
    expect(publicPayloadSha256({ b: 2, a: 1 })).toBe(
      "43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777",
    );
    expect(publicPayloadSha256({ b: 2, a: 1 })).toBe(publicPayloadSha256({ a: 1, b: 2 }));
  });

  it("rejects values that JSON cannot safely publish", () => {
    expect(() => publicPayloadSha256({ result: Number.NaN })).toThrow("non-finite");
    expect(() => publicPayloadSha256({ result: undefined })).toThrow("non-JSON");
  });
});
