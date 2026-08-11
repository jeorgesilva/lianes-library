import type { ReactNode } from "react";

type Tone = "success" | "warning" | "danger" | "price-drop" | "neutral";

const toneClasses: Record<Tone, string> = {
  success: "bg-success/15 text-success",
  warning: "bg-warning/15 text-warning",
  danger: "bg-danger/15 text-danger",
  "price-drop": "bg-price-drop/15 text-price-drop",
  neutral: "bg-border/50 text-text-muted",
};

export function StatusBadge({
  tone,
  children,
  className = "",
}: {
  tone: Tone;
  children: ReactNode;
  className?: string;
}) {
  return (
    <span
      className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium ${toneClasses[tone]} ${className}`}
    >
      {children}
    </span>
  );
}
