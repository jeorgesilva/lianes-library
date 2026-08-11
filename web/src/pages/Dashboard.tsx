import { useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api, type Book } from "../lib/api";
import { BookCarousel } from "../components/BookCarousel";
import { HeroBanner } from "../components/HeroBanner";
import { StatusBadge } from "../components/StatusBadge";
import { EmptyState } from "../components/EmptyState";
import { BookRowSkeleton } from "../components/Skeleton";

const FANTASY_WORDS = ["dragon", "magic", "potter", "wizard", "legend"];
const SCIFI_WORDS = ["space", "science", "star", "robot", "future"];

function matches(book: Book, words: string[]) {
  const haystack = JSON.stringify(book).toLowerCase();
  return words.some((w) => haystack.includes(w));
}

export function Dashboard() {
  const navigate = useNavigate();
  const booksQuery = useQuery({ queryKey: ["books"], queryFn: () => api.books.list() });
  const loansQuery = useQuery({ queryKey: ["loans", "active"], queryFn: api.loans.active });
  const readingQuery = useQuery({ queryKey: ["reading", "READING"], queryFn: () => api.reading.list("READING") });

  if (booksQuery.isLoading) {
    return (
      <div>
        <div className="mb-8 h-56 animate-pulse rounded-2xl bg-surface" />
        <BookRowSkeleton />
        <BookRowSkeleton />
      </div>
    );
  }

  if (booksQuery.isError) {
    return <p className="text-danger">⚠️ Error loading the Dashboard: {(booksQuery.error as Error).message}</p>;
  }

  const books = booksQuery.data ?? [];
  const activeLoans = loansQuery.data ?? [];

  if (books.length === 0) {
    return (
      <EmptyState
        icon="📭"
        title="Your shelf is empty"
        description='Want to scan some books in the "Book Catalog"?'
        action={
          <button
            type="button"
            onClick={() => navigate("/catalog")}
            className="rounded-lg bg-primary px-4 py-2 text-sm font-medium text-white hover:bg-primary/90"
          >
            Go to Book Catalog
          </button>
        }
      />
    );
  }

  const borrowedIds = new Set(activeLoans.map((l) => l.book_id));
  const lent = books.filter((b) => borrowedIds.has(b.book_id));
  const newArrivals = [...books].sort((a, b) => b.book_id - a.book_id).slice(0, 10);
  const fantasy = books.filter((b) => matches(b, FANTASY_WORDS));
  const scifi = books.filter((b) => matches(b, SCIFI_WORDS));
  const featured = newArrivals[0];
  const continueReading = readingQuery.data?.[0];

  return (
    <div>
      {continueReading ? (
        <HeroBanner
          book={continueReading}
          eyebrow="Continue Reading"
          description={continueReading.author ?? undefined}
          progress={
            continueReading.total_pages
              ? { current: continueReading.current_page ?? 0, total: continueReading.total_pages }
              : undefined
          }
          primaryAction={{
            label: "Open Journal",
            onClick: () => navigate(`/reading?tab=journal&log=${continueReading.reading_log_id}`),
          }}
          secondaryAction={{ label: "View Queue", onClick: () => navigate("/reading") }}
        />
      ) : (
        <HeroBanner
          book={featured}
          eyebrow="Featured Pick"
          description={featured.genre ? `Latest addition to your shelf · ${featured.genre}` : "Latest addition to your shelf"}
          primaryAction={{ label: "Open in Catalog", onClick: () => navigate("/catalog") }}
        />
      )}

      <BookCarousel
        title="⏳ Lent"
        books={lent}
        renderBadge={() => <StatusBadge tone="warning">Lent</StatusBadge>}
      />
      <BookCarousel title="✨ New Arrivals" books={newArrivals} />
      <BookCarousel
        title="🐉 Epic Fantasy"
        subtitle="Because your shelf has dragons and wizards"
        books={fantasy}
      />
      <BookCarousel
        title="🚀 Space Travel and Sci-Fi"
        subtitle="Because your shelf reaches for the stars"
        books={scifi}
      />
      <BookCarousel title="📚 Explore the whole library" books={books} />
    </div>
  );
}
