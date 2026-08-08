import { describe, expect, it } from "vitest";

import { parseSocketDatabaseUrl } from "./connection-options";

describe("public database connection options", () => {
  it("converts a libpq Unix-socket URL into postgres.js options", () => {
    expect(
      parseSocketDatabaseUrl(
        "postgresql://reader:safe-secret@/systematic_fx_public?host=%2Fprivate%2Fsocket&port=55432",
      ),
    ).toEqual({
      host: "/private/socket",
      port: 55432,
      database: "systematic_fx_public",
      user: "reader",
      password: "safe-secret",
    });
  });

  it("leaves ordinary TCP URLs to postgres.js", () => {
    expect(parseSocketDatabaseUrl("postgresql://reader:secret@db.example/public")).toBeNull();
  });
});
