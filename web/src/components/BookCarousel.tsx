import type { Book } from "../lib/api";

export function BookCarousel({ title, books }: { title: string; books: Book[] }) {
  if (books.length === 0) return null;
  return (
    <section className="mb-6">
      <h2 className="mb-3 text-lg font-semibold text-text">{title}</h2>
      <div className="flex gap-3 overflow-x-auto pb-2">
        {books.map((book) => (
          <div
            key={book.book_id}
            className="group relative h-40 w-28 shrink-0 overflow-hidden rounded-md bg-surface transition-transform hover:scale-105"
            title={book.title}
          >
            {book.cover_url ? (
              <img
                src={book.cover_url}
                alt={book.title}
                className="h-full w-full object-cover"
              />
            ) : (
              <div className="flex h-full w-full items-center justify-center p-2 text-center text-[10px] text-text-muted">
                {book.title.slice(0, 40)}
              </div>
            )}
            <div className="absolute inset-0 flex flex-col items-center justify-center bg-black/80 p-2 text-center opacity-0 transition-opacity group-hover:opacity-100">
              <span className="text-[11px] font-bold text-white">{book.title.slice(0, 60)}</span>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}
