interface MetricCardProps {
  label: string;
  value: string | number;
  note: string;
}

export function MetricCard({ label, value, note }: MetricCardProps) {
  return (
    <article className="metric-card">
      <span className="metric-label">{label}</span>
      <strong className="metric-value">{value}</strong>
      <span className="metric-note">{note}</span>
    </article>
  );
}
