import Link from "next/link";

export default function NotFound() {
  return (
    <section className="section shell">
      <div className="empty-state">
        <span className="empty-state-icon" aria-hidden="true">404</span>
        <h1>Research record not found</h1>
        <p>The requested route or public record does not exist in the current projection.</p>
        <Link className="text-link" href="/">Return to the research ledger →</Link>
      </div>
    </section>
  );
}
