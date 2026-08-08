import Link from "next/link";

const links = [
  ["Research", "/research"],
  ["Hypotheses", "/hypotheses"],
  ["Patterns", "/patterns"],
  ["Data integrity", "/data-quality"],
  ["Methodology", "/methodology"],
] as const;

export function SiteHeader() {
  return (
    <header className="site-header">
      <div className="shell site-header-inner">
        <Link className="brand" href="/">
          <span className="brand-mark">SF·X</span>
          <span>Research Ledger</span>
        </Link>
        <nav className="site-nav" aria-label="Primary navigation">
          {links.map(([label, href]) => (
            <Link href={href} key={href}>{label}</Link>
          ))}
        </nav>
        <span className="live-pill"><span className="live-dot" />Live projection</span>
      </div>
    </header>
  );
}
