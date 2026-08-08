import "server-only";

import postgres from "postgres";

import { parseSocketDatabaseUrl } from "@/server/db/connection-options";

type SqlClient = ReturnType<typeof postgres>;

const globalDatabase = globalThis as unknown as { publicSiteSql?: SqlClient };

export function getPublicDatabase(): SqlClient {
  const databaseUrl = process.env.SITE_DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("SITE_DATABASE_URL is required for the dynamic research site");
  }
  if (!globalDatabase.publicSiteSql) {
    const socket = parseSocketDatabaseUrl(databaseUrl);
    const runtimeOptions = {
      max: 5,
      idle_timeout: 20,
      connect_timeout: 10,
      prepare: false,
    } as const;
    globalDatabase.publicSiteSql = socket
      ? postgres({ ...socket, ...runtimeOptions })
      : postgres(databaseUrl, runtimeOptions);
  }
  return globalDatabase.publicSiteSql;
}
