import { useRef, type ReactNode } from "react";
import type { Book } from "../lib/api";
import { BookCard } from "./BookCard";
import { BookRowSkeleton } from "./Skeleton";

export function BookCarousel({
  title,
  subtitle,
  books,
  isLoading,
  renderBadge,
  renderActions,
  onCardClick,
}: {
  title?: string;
  subtitle?: string;
  books: Book[];
  isLoading?: boolean;
  renderBadge?: (book: Book) => ReactNode;
  renderActions?: (book: Book) => ReactNode;
  onCardClick?: (book: Book) => void;
}) {
  const scrollerRef = useRef<HTMLDivElement>(null);

  if (isLoading) return <BookRowSkeleton title={title} />;
  if (books.length === 0) return null;

  const scrollBy = (direction: -1 | 1) => {
    const el = scrollerRef.current;
    if (!el) return;
    el.scrollBy({ left: direction * el.clientWidth * 0.8, behavior: "smooth" });
  };

  return (
    <section className="group/row relative mb-8">
      {title && <h2 className="font-display text-lg font-semibold text-text">{title}</h2>}
      {subtitle && <p className="mb-1 text-xs text-text-muted">{subtitle}</p>}

      <div className="relative">
        <button
          type="button"
          aria-label="Scroll left"
          onClick={() => scrollBy(-1)}
          className="absolute left-0 top-0 z-10 hidden h-full w-10 items-center justify-center bg-gradient-to-r from-bg to-transparent text-text opacity-0 transition-opacity duration-200 group-hover/row:opacity-100 sm:flex hover:text-primary"
        >
          ‹
        </button>

        <div
          ref={scrollerRef}
          className="flex gap-3 overflow-x-auto scroll-smooth pb-2 [scrollbar-width:none] [&::-webkit-scrollbar]:hidden"
        >
          {books.map((book) => (
            <BookCard
              key={book.book_id}
              book={book}
              badge={renderBadge?.(book)}
              actions={renderActions?.(book)}
              onClick={onCardClick ? () => onCardClick(book) : undefined}
            />
          ))}
        </div>

        <button
          type="button"
          aria-label="Scroll right"
          onClick={() => scrollBy(1)}
          className="absolute right-0 top-0 z-10 hidden h-full w-10 items-center justify-center bg-gradient-to-l from-bg to-transparent text-text opacity-0 transition-opacity duration-200 group-hover/row:opacity-100 sm:flex hover:text-primary"
        >
          ›
        </button>
      </div>
    </section>
  );
}
