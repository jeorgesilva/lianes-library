import type { HTMLAttributes, ReactNode } from "react";

/**
 * The default bento cell chrome used across the Dashboard. A couple of cells
 * (the "Continue Reading" hero and the Diary preview) own their own full
 * chrome instead of using this, because they bleed cover art to the card
 * edge — see ContinueReadingHero in Dashboard.tsx and JournalPreviewCard.
 */
export function BentoCard({ children, className = "", ...props }: HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={`relative flex flex-col overflow-hidden rounded-3xl border border-border bg-surface p-5 shadow-sm transition-shadow duration-200 hover:shadow-md sm:p-6 ${className}`}
      {...props}
    >
      {children}
    </div>
  );
}

export function BentoHeader({
  eyebrow,
  title,
  action,
}: {
  eyebrow?: string;
  title: string;
  action?: ReactNode;
}) {
  return (
    <div className="mb-4 flex items-start justify-between gap-3">
      <div>
        {eyebrow && <p className="text-[10px] font-semibold uppercase tracking-wider text-accent">{eyebrow}</p>}
        <h2 className="font-display text-lg font-semibold leading-tight text-text">{title}</h2>
      </div>
      {action}
    </div>
  );
}
