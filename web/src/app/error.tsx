"use client";

export default function ErrorPage({ reset }: { error: Error & { digest?: string }; reset: () => void }) {
  return (
    <div className="shell page-hero">
      <div className="eyebrow">Projection unavailable</div>
      <h1 className="page-title">The public ledger could not be loaded.</h1>
      <p className="page-description">The research database remains isolated. This page will recover when the public projection service is available.</p>
      <button className="field" style={{ width: 180, marginTop: 24, cursor: "pointer" }} onClick={reset}>Try again</button>
    </div>
  );
}
