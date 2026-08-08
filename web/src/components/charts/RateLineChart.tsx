import { useState } from "react";

interface Point {
  month: string;
  pct: number;
  total: number;
}

interface Props {
  data: Point[];
  color: string;
}

const WIDTH = 640;
const HEIGHT = 200;
const PAD_LEFT = 36;
const PAD_BOTTOM = 24;
const PAD_TOP = 12;

function monthLabel(month: string) {
  const [, m] = month.split("-");
  const names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return names[Number(m) - 1] ?? month;
}

// Single-series line + area wash. Crosshair snaps to the nearest month.
export function RateLineChart({ data, color }: Props) {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);

  if (data.length === 0) {
    return <p className="text-sm text-text-muted">No returned loans yet — nothing to measure.</p>;
  }

  const plotWidth = WIDTH - PAD_LEFT;
  const plotHeight = HEIGHT - PAD_TOP - PAD_BOTTOM;
  const maxPct = Math.max(...data.map((d) => d.pct), 10);
  const step = data.length > 1 ? plotWidth / (data.length - 1) : 0;

  const xFor = (i: number) => PAD_LEFT + i * step;
  const yFor = (pct: number) => PAD_TOP + plotHeight - (pct / maxPct) * plotHeight;

  const linePath = data.map((d, i) => `${i === 0 ? "M" : "L"} ${xFor(i)} ${yFor(d.pct)}`).join(" ");
  const areaPath = `${linePath} L ${xFor(data.length - 1)} ${PAD_TOP + plotHeight} L ${xFor(0)} ${PAD_TOP + plotHeight} Z`;

  const yTicks = [0, Math.round(maxPct / 2), Math.round(maxPct)];

  const handleMove = (e: React.PointerEvent<SVGRectElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const ratio = x / rect.width;
    const i = Math.round(ratio * (data.length - 1));
    setHoverIndex(Math.min(Math.max(i, 0), data.length - 1));
  };

  const hovered = hoverIndex !== null ? data[hoverIndex] : null;

  return (
    <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} className="w-full" role="img" aria-label="Late-return rate over time">
      {yTicks.map((t) => {
        const y = yFor(t);
        return (
          <g key={t}>
            <line x1={PAD_LEFT} x2={WIDTH} y1={y} y2={y} stroke="#2d3748" strokeWidth={1} />
            <text x={PAD_LEFT - 6} y={y + 3} textAnchor="end" fontSize={10} fill="#9ca3af">
              {t}%
            </text>
          </g>
        );
      })}

      <path d={areaPath} fill={color} opacity={0.1} />
      <path d={linePath} fill="none" stroke={color} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />

      {data.map((d, i) => (
        <text key={d.month} x={xFor(i)} y={HEIGHT - PAD_BOTTOM + 14} textAnchor="middle" fontSize={10} fill="#9ca3af">
          {i % Math.ceil(data.length / 6) === 0 ? monthLabel(d.month) : ""}
        </text>
      ))}

      {hoverIndex !== null && (
        <g>
          <circle cx={xFor(hoverIndex)} cy={yFor(hovered!.pct)} r={4} fill={color} stroke="#1e232e" strokeWidth={2} />
        </g>
      )}

      {/* full-height hit area drives the crosshair */}
      <rect
        x={PAD_LEFT}
        y={PAD_TOP}
        width={plotWidth}
        height={plotHeight}
        fill="transparent"
        onPointerMove={handleMove}
        onPointerLeave={() => setHoverIndex(null)}
        style={{ cursor: "crosshair" }}
      />

      {hovered && (
        <g>
          <line x1={xFor(hoverIndex!)} x2={xFor(hoverIndex!)} y1={PAD_TOP} y2={PAD_TOP + plotHeight} stroke="#2d3748" strokeWidth={1} />
          <rect
            x={Math.min(Math.max(xFor(hoverIndex!) - 55, PAD_LEFT), WIDTH - 110)}
            y={PAD_TOP}
            width={110}
            height={34}
            rx={4}
            fill="#1e232e"
            stroke="#2d3748"
          />
          <text
            x={Math.min(Math.max(xFor(hoverIndex!) - 55, PAD_LEFT), WIDTH - 110) + 8}
            y={PAD_TOP + 14}
            fontSize={11}
            fontWeight={600}
            fill="#e0e0e0"
          >
            {monthLabel(hovered.month)}: {hovered.pct}%
          </text>
          <text
            x={Math.min(Math.max(xFor(hoverIndex!) - 55, PAD_LEFT), WIDTH - 110) + 8}
            y={PAD_TOP + 27}
            fontSize={10}
            fill="#9ca3af"
          >
            {hovered.total} loan(s) returned
          </text>
        </g>
      )}
    </svg>
  );
}
