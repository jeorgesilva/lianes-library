import { useQuery } from "@tanstack/react-query";
import { api, type Book } from "../lib/api";
import { BookCarousel } from "../components/BookCarousel";

const FANTASY_WORDS = ["dragon", "magic", "potter", "wizard", "legend"];
const SCIFI_WORDS = ["space", "science", "star", "robot", "future"];

function matches(book: Book, words: string[]) {
  const haystack = JSON.stringify(book).toLowerCase();
  return words.some((w) => haystack.includes(w));
}

export function Dashboard() {
  const booksQuery = useQuery({ queryKey: ["books"], queryFn: () => api.books.list() });
  const loansQuery = useQuery({ queryKey: ["loans", "active"], queryFn: api.loans.active });

  if (booksQuery.isLoading) return <p className="text-text-muted">Loading...</p>;
  if (booksQuery.isError) return <p className="text-red-400">⚠️ Error loading the Dashboard: {(booksQuery.error as Error).message}</p>;

  const books = booksQuery.data ?? [];
  const activeLoans = loansQuery.data ?? [];

  if (books.length === 0) {
    return <p className="text-text-muted">📭 Your shelf is empty. Want to scan some books in the "Book Catalog"?</p>;
  }

  const borrowedIds = new Set(activeLoans.map((l) => l.book_id));
  const lent = books.filter((b) => borrowedIds.has(b.book_id));
  const newArrivals = [...books].sort((a, b) => b.book_id - a.book_id).slice(0, 10);
  const fantasy = books.filter((b) => matches(b, FANTASY_WORDS));
  const scifi = books.filter((b) => matches(b, SCIFI_WORDS));

  return (
    <div>
      <h1 className="mb-6 text-3xl font-bold text-accent">🍿 Liane's Discovery</h1>
      <BookCarousel title="⏳ Lent" books={lent} />
      <BookCarousel title="✨ New Arrivals" books={newArrivals} />
      <BookCarousel title="🐉 Epic Fantasy" books={fantasy} />
      <BookCarousel title="🚀 Space Travel and Sci-Fi" books={scifi} />
      <BookCarousel title="📚 Explore the whole library" books={books} />
    </div>
  );
}
