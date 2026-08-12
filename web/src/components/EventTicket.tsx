interface EventTicketProps {
  title: string;
  venue?: string | null;
  city?: string | null;
  /** "YYYY-MM-DD", matching LiteraryEvent.event_date. */
  date: string;
  url?: string | null;
  onExportIcs?: () => void;
}

// The one component in this pass with a literal metaphor: a torn admission
// ticket. Stub (date) + dashed perforation + body (title/venue). The two
// "punch hole" circles are colored bg-surface, so this assumes it's placed
// on a bg-surface card (BentoCard/Card) — swap to bg-bg if used loose on
// the page background.
export function EventTicket({ title, venue, city, date, url, onExportIcs }: EventTicketProps) {
  const parsed = new Date(`${date}T00:00:00`);
  const hasDate = !Number.isNaN(parsed.getTime());
  const day = hasDate ? parsed.toLocaleDateString("en-US", { day: "2-digit" }) : "--";
  const month = hasDate ? parsed.toLocaleDateString("en-US", { month: "short" }).toUpperCase() : "TBD";

  return (
    <div className="relative flex overflow-hidden rounded-2xl border border-border bg-surface shadow-sm">
      <div className="relative flex w-16 shrink-0 flex-col items-center justify-center gap-0.5 bg-primary py-3 text-on-primary sm:w-20">
        <span className="font-display text-xl font-bold leading-none sm:text-2xl">{day}</span>
        <span className="text-[9px] font-semibold tracking-widest">{month}</span>
        <span aria-hidden className="absolute -top-2 right-0 h-4 w-4 translate-x-1/2 rounded-full bg-surface" />
        <span aria-hidden className="absolute -bottom-2 right-0 h-4 w-4 translate-x-1/2 rounded-full bg-surface" />
      </div>

      <div className="flex flex-1 items-center justify-between gap-2 border-l border-dashed border-border px-3 py-2.5 sm:px-4">
        <div className="min-w-0">
          <p className="truncate font-display text-sm font-semibold text-text">{title}</p>
          {(venue || city) && (
            <p className="truncate text-xs text-text-muted">{[venue, city].filter(Boolean).join(" · ")}</p>
          )}
        </div>
        <div className="flex shrink-0 items-center gap-2">
          {url && (
            <a href={url} target="_blank" rel="noreferrer" className="text-xs font-medium text-primary hover:underline">
              View
            </a>
          )}
          {onExportIcs && (
            <button
              type="button"
              onClick={onExportIcs}
              className="rounded-lg border border-border px-2 py-1 text-[10px] font-medium text-text-muted transition-colors hover:text-text"
            >
              .ics
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
