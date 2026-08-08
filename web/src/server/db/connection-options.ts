export interface SocketDatabaseOptions {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
}

export function parseSocketDatabaseUrl(databaseUrl: string): SocketDatabaseOptions | null {
  if (!databaseUrl.includes("@/")) return null;
  const parsed = new URL(databaseUrl.replace("@/", "@localhost/"));
  const host = parsed.searchParams.get("host");
  const port = Number(parsed.searchParams.get("port") ?? "5432");
  const database = decodeURIComponent(parsed.pathname.slice(1));
  if (!host?.startsWith("/") || !Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("SITE_DATABASE_URL has invalid PostgreSQL socket coordinates");
  }
  if (!parsed.username || !database) {
    throw new Error("SITE_DATABASE_URL must include a role and database name");
  }
  return {
    host,
    port,
    database,
    user: decodeURIComponent(parsed.username),
    password: decodeURIComponent(parsed.password),
  };
}
