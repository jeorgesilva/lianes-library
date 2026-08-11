import type { ReactNode } from "react";
import type { Book } from "../lib/api";

export function BookCard({
  book,
  badge,
  footer,
  actions,
  onClick,
}: {
  book: Book;
  badge?: ReactNode;
  footer?: ReactNode;
  actions?: ReactNode;
  onClick?: () => void;
}) {
  return (
    <div
      className={`group w-32 shrink-0 sm:w-36 ${onClick ? "cursor-pointer" : ""}`}
      onClick={onClick}
    >
      <div className="relative h-44 w-full overflow-hidden rounded-md bg-surface shadow-sm transition-transform duration-200 group-hover:scale-105 group-hover:shadow-lg sm:h-52">
        {book.cover_url ? (
          <img
            src={book.cover_url}
            alt={book.title}
            loading="lazy"
            className="h-full w-full object-cover"
          />
        ) : (
          <div className="flex h-full w-full items-center justify-center p-2 text-center font-display text-xs text-text-muted">
            {book.title.slice(0, 60)}
          </div>
        )}

        {badge && <div className="absolute left-1.5 top-1.5">{badge}</div>}

        <div className="absolute inset-0 flex flex-col justify-end gap-1.5 bg-gradient-to-t from-black/90 via-black/40 to-transparent p-2 opacity-0 transition-opacity duration-200 group-hover:opacity-100">
          <span className="text-[11px] font-semibold leading-tight text-white">
            {book.title}
          </span>
          {book.author && (
            <span className="text-[10px] text-white/70">{book.author}</span>
          )}
          {actions && <div className="mt-1 flex gap-1.5">{actions}</div>}
        </div>
      </div>

      <p className="mt-2 truncate text-xs font-medium text-text" title={book.title}>
        {book.title}
      </p>
      {book.author && (
        <p className="truncate text-[11px] text-text-muted" title={book.author}>
          {book.author}
        </p>
      )}
      {footer}
    </div>
  );
}
