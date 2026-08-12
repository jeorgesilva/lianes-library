interface Props {
  values: number[];
  color?: string;
  width?: number;
  height?: number;
}

// Minimal price-history sparkline for a wishlist card — deliberately not
// RateLineChart (crosshair, axes, hover tooltip): this just needs to answer
// "is it trending down?" at a glance inside a small card.
export function Sparkline({ values, color = "var(--color-price-drop)", width = 120, height = 32 }: Props) {
  if (values.length < 2) {
    return <div style={{ height }} className="flex items-center text-xs text-text-muted">Not enough price history yet</div>;
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const step = width / (values.length - 1);

  const points = values.map((v, i) => `${i * step},${height - ((v - min) / range) * height}`).join(" ");

  return (
    <svg viewBox={`0 0 ${width} ${height}`} width={width} height={height} role="img" aria-label="Price history">
      <polyline points={points} fill="none" stroke={color} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}
