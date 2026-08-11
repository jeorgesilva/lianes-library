import type { Book } from "../lib/api";

export function HeroBanner({
  book,
  eyebrow,
  description,
  progress,
  primaryAction,
  secondaryAction,
}: {
  book: Book;
  eyebrow: string;
  description?: string;
  progress?: { current: number; total: number };
  primaryAction?: { label: string; onClick: () => void };
  secondaryAction?: { label: string; onClick: () => void };
}) {
  const progressPct =
    progress && progress.total > 0
      ? Math.min(100, Math.round((progress.current / progress.total) * 100))
      : null;

  return (
    <section className="relative mb-8 overflow-hidden rounded-2xl border border-border bg-surface">
      {book.cover_url && (
        <img
          src={book.cover_url}
          alt=""
          aria-hidden
          className="absolute inset-0 h-full w-full scale-110 object-cover opacity-25 blur-xl"
        />
      )}
      <div className="absolute inset-0 bg-gradient-to-r from-bg via-bg/80 to-transparent" />

      <div className="relative flex flex-col gap-4 p-6 sm:flex-row sm:items-center sm:gap-8 sm:p-10">
        {book.cover_url && (
          <img
            src={book.cover_url}
            alt={book.title}
            className="h-40 w-28 shrink-0 rounded-md object-cover shadow-xl sm:h-56 sm:w-40"
          />
        )}

        <div className="min-w-0 flex-1">
          <p className="text-xs font-semibold uppercase tracking-wide text-accent">{eyebrow}</p>
          <h1 className="mt-1 font-display text-2xl font-bold text-text sm:text-4xl">
            {book.title}
          </h1>
          {book.author && <p className="mt-1 text-sm text-text-muted">{book.author}</p>}
          {description && <p className="mt-3 max-w-lg text-sm text-text-muted">{description}</p>}

          {progressPct !== null && (
            <div className="mt-4 max-w-sm">
              <div className="h-1.5 w-full overflow-hidden rounded-full bg-border">
                <div
                  className="h-full rounded-full bg-primary transition-all duration-300"
                  style={{ width: `${progressPct}%` }}
                />
              </div>
              <p className="mt-1 text-xs text-text-muted">
                {progress?.current} / {progress?.total} pages · {progressPct}%
              </p>
            </div>
          )}

          {(primaryAction || secondaryAction) && (
            <div className="mt-5 flex gap-3">
              {primaryAction && (
                <button
                  type="button"
                  onClick={primaryAction.onClick}
                  className="rounded-lg bg-primary px-5 py-2 text-sm font-medium text-white transition-colors hover:bg-primary/90"
                >
                  {primaryAction.label}
                </button>
              )}
              {secondaryAction && (
                <button
                  type="button"
                  onClick={secondaryAction.onClick}
                  className="rounded-lg border border-border bg-surface px-5 py-2 text-sm font-medium text-text transition-colors hover:bg-border"
                >
                  {secondaryAction.label}
                </button>
              )}
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
