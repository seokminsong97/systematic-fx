import { getLatestResearchSnapshot } from "@/server/repositories/research-publication";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function GET(request: Request) {
  try {
    const snapshot = await getLatestResearchSnapshot();
    const etag = `\"revision-${snapshot.metadata.revision}\"`;
    if (request.headers.get("if-none-match") === etag) {
      return new Response(null, { status: 304, headers: { ETag: etag } });
    }
    return Response.json(snapshot, {
      headers: {
        "Cache-Control": "no-store, max-age=0",
        ETag: etag,
      },
    });
  } catch (error) {
    console.error("public research projection unavailable", error);
    return Response.json(
      { error: "Research projection is temporarily unavailable" },
      { status: 503, headers: { "Cache-Control": "no-store" } },
    );
  }
}
