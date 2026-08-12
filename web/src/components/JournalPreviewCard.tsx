interface JournalPreviewCardProps {
  bookTitle: string;
  coverUrl?: string | null;
  entryContent?: string;
  entryDate?: string;
  mood?: string | null;
  onOpen: () => void;
  className?: string;
}

// The one deliberately glass surface in this system (see the design brief:
// glassmorphism reserved for the Diary). A frosted panel — bg-surface/55 +
// backdrop-blur-md + a hairline white border — floats over a softly blurred
// cover image, with the entry itself set in the display serif like a pulled
// quote from a page.
export function JournalPreviewCard({
  bookTitle,
  coverUrl,
  entryContent,
  entryDate,
  mood,
  onOpen,
  className = "",
}: JournalPreviewCardProps) {
  return (
    <button
      type="button"
      onClick={onOpen}
      className={`group relative flex min-h-56 w-full cursor-pointer flex-col justify-end overflow-hidden rounded-3xl border border-border p-0 text-left shadow-sm transition-shadow duration-200 hover:shadow-md focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary ${className}`}
    >
      {coverUrl ? (
        <img
          src={coverUrl}
          alt=""
          aria-hidden
          className="absolute inset-0 h-full w-full scale-110 object-cover opacity-50 blur-md transition-transform duration-300 group-hover:scale-125"
        />
      ) : (
        <div className="absolute inset-0 bg-gradient-to-br from-primary/30 to-accent/20" />
      )}
      <div className="absolute inset-0 bg-gradient-to-t from-bg via-bg/60 to-bg/10" />

      <div className="relative m-3 rounded-2xl border border-white/15 bg-surface/55 p-4 backdrop-blur-md sm:p-5">
        <span aria-hidden className="font-display text-3xl leading-none text-accent">
          &ldquo;
        </span>
        {entryContent ? (
          <p className="-mt-3 line-clamp-3 font-display text-base leading-snug text-text">{entryContent}</p>
        ) : (
          <p className="-mt-3 font-display text-base leading-snug text-text-muted">
            No entries yet — write the first page.
          </p>
        )}
        <div className="mt-3 flex items-center justify-between gap-2 text-xs text-text-muted">
          <span className="truncate font-medium text-text">{bookTitle}</span>
          <span className="flex shrink-0 items-center gap-2">
            {mood && <span className="rounded-full bg-border/60 px-2 py-0.5 text-[10px]">{mood}</span>}
            {entryDate && (
              <span>{new Date(entryDate).toLocaleDateString("en-US", { month: "short", day: "numeric" })}</span>
            )}
          </span>
        </div>
      </div>
    </button>
  );
}
