interface SectionHeadingProps {
  kicker: string;
  title: string;
  description: string;
}

export function SectionHeading({ kicker, title, description }: SectionHeadingProps) {
  return (
    <div className="section-heading">
      <div>
        <span className="section-kicker">{kicker}</span>
        <h2>{title}</h2>
      </div>
      <p>{description}</p>
    </div>
  );
}
