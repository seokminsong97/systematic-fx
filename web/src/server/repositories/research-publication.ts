import "server-only";

import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";

import schema from "../../../../contracts/publication/research-snapshot.v2.schema.json";
import type { ResearchSnapshot } from "@/domain/research/types";
import { getPublicDatabase } from "@/server/db/client";
import { publicPayloadSha256 } from "@/server/publication-integrity";

const ajv = new Ajv2020({ allErrors: true, strict: true });
addFormats(ajv);
const validateSnapshot = ajv.compile<ResearchSnapshot>(schema);

export class PublicationUnavailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PublicationUnavailableError";
  }
}

export async function getLatestResearchSnapshot(): Promise<ResearchSnapshot> {
  const campaignKey = process.env.SITE_CAMPAIGN_KEY ?? "phase1a_conservative_screening_v1";
  const sql = getPublicDatabase();
  const rows = await sql<
    Array<{ revision: number; payload: unknown; payload_sha256: string }>
  >`
    SELECT revision, payload, payload_sha256
    FROM systematic_fx_public.current_research_publications
    WHERE campaign_key = ${campaignKey}
    LIMIT 1
  `;
  const row = rows[0];
  if (!row) {
    throw new PublicationUnavailableError(`No public projection exists for ${campaignKey}`);
  }
  if (publicPayloadSha256(row.payload) !== row.payload_sha256) {
    throw new PublicationUnavailableError("Public projection content hash does not match its envelope");
  }
  if (!validateSnapshot(row.payload)) {
    const details = ajv.errorsText(validateSnapshot.errors, { separator: "; " });
    throw new PublicationUnavailableError(`Public projection violates its contract: ${details}`);
  }
  if (row.payload.metadata.revision !== Number(row.revision)) {
    throw new PublicationUnavailableError("Public projection revision does not match its envelope");
  }
  return row.payload;
}
