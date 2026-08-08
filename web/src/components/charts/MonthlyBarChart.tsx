import { useState } from "react";

interface Point {
  month: string;
  value: number;
}

interface Props {
  data: Point[];
  color: string;
  formatValue?: (v: number) => string;
}

const WIDTH = 640;
const HEIGHT = 200;
const PAD_LEFT = 32;
const PAD_BOTTOM = 24;
const PAD_TOP = 12;

function monthLabel(month: string) {
  const [, m] = month.split("-");
  const names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return names[Number(m) - 1] ?? month;
}

// Single-series bar chart: one hue, no legend needed (title names the series).
export function MonthlyBarChart({ data, color, formatValue = (v) => String(v) }: Props) {
  const [hovered, setHovered] = useState<number | null>(null);

  if (data.length === 0) {
    return <p className="text-sm text-text-muted">No data for this period yet.</p>;
  }

  const max = Math.max(...data.map((d) => d.value), 1);
  const plotWidth = WIDTH - PAD_LEFT;
  const plotHeight = HEIGHT - PAD_TOP - PAD_BOTTOM;
  const slot = plotWidth / data.length;
  const barWidth = Math.min(24, slot * 0.6);
  const gap = 2; // surface gap between adjacent bars

  const yTicks = [0, Math.round(max / 2), max];

  return (
    <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} className="w-full" role="img" aria-label="Bar chart">
      {yTicks.map((t) => {
        const y = PAD_TOP + plotHeight - (t / max) * plotHeight;
        return (
          <g key={t}>
            <line x1={PAD_LEFT} x2={WIDTH} y1={y} y2={y} stroke="#2d3748" strokeWidth={1} />
            <text x={PAD_LEFT - 6} y={y + 3} textAnchor="end" fontSize={10} fill="#9ca3af">
              {t}
            </text>
          </g>
        );
      })}

      {data.map((d, i) => {
        const x = PAD_LEFT + i * slot + (slot - barWidth) / 2;
        const barHeight = Math.max((d.value / max) * plotHeight, d.value > 0 ? 2 : 0);
        const y = PAD_TOP + plotHeight - barHeight;
        const isHovered = hovered === i;

        return (
          <g
            key={d.month}
            onPointerEnter={() => setHovered(i)}
            onPointerLeave={() => setHovered((h) => (h === i ? null : h))}
            style={{ cursor: "pointer" }}
          >
            {/* hit target wider than the visual bar */}
            <rect x={PAD_LEFT + i * slot + gap / 2} y={PAD_TOP} width={slot - gap} height={plotHeight} fill="transparent" />
            <rect
              x={x}
              y={y}
              width={barWidth}
              height={barHeight}
              rx={4}
              fill={color}
              opacity={isHovered ? 1 : 0.85}
            />
            <text x={x + barWidth / 2} y={HEIGHT - PAD_BOTTOM + 14} textAnchor="middle" fontSize={10} fill="#9ca3af">
              {monthLabel(d.month)}
            </text>
            {isHovered && (
              <g>
                <rect
                  x={Math.min(Math.max(x - 20, PAD_LEFT), WIDTH - 76)}
                  y={y - 24}
                  width={76}
                  height={20}
                  rx={4}
                  fill="#1e232e"
                  stroke="#2d3748"
                />
                <text
                  x={Math.min(Math.max(x - 20, PAD_LEFT), WIDTH - 76) + 38}
                  y={y - 10}
                  textAnchor="middle"
                  fontSize={11}
                  fill="#e0e0e0"
                  fontWeight={600}
                >
                  {monthLabel(d.month)}: {formatValue(d.value)}
                </text>
              </g>
            )}
          </g>
        );
      })}
    </svg>
  );
}
