import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api, type Book, type ReadingLogItem } from "../lib/api";
import { useAuth } from "../lib/AuthContext";
import { borrowTone, loanTone } from "../lib/dueDate";
import { BentoCard, BentoHeader } from "../components/BentoCard";
import { StatTile } from "../components/StatTile";
import { JournalPreviewCard } from "../components/JournalPreviewCard";
import { EventTicket } from "../components/EventTicket";
import { BookCarousel } from "../components/BookCarousel";
import { StatusBadge } from "../components/StatusBadge";
import { EmptyState } from "../components/EmptyState";
import { Skeleton } from "../components/Skeleton";
import { Button } from "../components/Button";

const FANTASY_WORDS = ["dragon", "magic", "potter", "wizard", "legend"];
const SCIFI_WORDS = ["space", "science", "star", "robot", "future"];

function matchesGenre(book: Book, words: string[]) {
  const haystack = JSON.stringify(book).toLowerCase();
  return words.some((w) => haystack.includes(w));
}

type ExploreTab = "new" | "fantasy" | "scifi" | "all";

const EXPLORE_TABS: { id: ExploreTab; label: string }[] = [
  { id: "new", label: "New Arrivals" },
  { id: "fantasy", label: "Epic Fantasy" },
  { id: "scifi", label: "Sci-Fi" },
  { id: "all", label: "Everything" },
];

function greeting() {
  const hour = new Date().getHours();
  if (hour < 12) return "Good morning";
  if (hour < 18) return "Good afternoon";
  return "Good evening";
}

// ---- small file-local presentational helpers (single-use, so they live
// here rather than as their own component files) -----------------------

function MiniCover({ coverUrl, title }: { coverUrl?: string | null; title: string }) {
  return coverUrl ? (
    <img src={coverUrl} alt={title} className="h-14 w-10 shrink-0 rounded object-cover shadow-sm" />
  ) : (
    <div className="flex h-14 w-10 shrink-0 items-center justify-center rounded bg-surface-hover text-center font-display text-[7px] leading-tight text-text-muted">
      {title.slice(0, 20)}
    </div>
  );
}

function SeeAllLink({ onClick }: { onClick: () => void }) {
  return (
    <button type="button" onClick={onClick} className="text-xs font-medium text-primary hover:underline">
      See all →
    </button>
  );
}

function CellSkeletonRows() {
  return (
    <div className="flex flex-col gap-3">
      {Array.from({ length: 3 }).map((_, i) => (
        <div key={i} className="flex items-center gap-3">
          <Skeleton className="h-14 w-10" />
          <div className="flex-1">
            <Skeleton className="h-3 w-3/4" />
            <Skeleton className="mt-1.5 h-3 w-1/2" />
          </div>
        </div>
      ))}
    </div>
  );
}

function DashboardSkeleton() {
  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-6">
      <Skeleton className="h-72 rounded-3xl sm:col-span-2 lg:col-span-4" />
      <Skeleton className="h-72 rounded-3xl sm:col-span-2 lg:col-span-2" />
      <Skeleton className="h-56 rounded-3xl lg:col-span-3" />
      <Skeleton className="h-56 rounded-3xl lg:col-span-3" />
      <Skeleton className="h-44 rounded-3xl lg:col-span-2" />
      <Skeleton className="h-44 rounded-3xl lg:col-span-2" />
      <Skeleton className="h-44 rounded-3xl lg:col-span-2" />
    </div>
  );
}

// The hero cell owns its own full chrome (border/bg/rounding) instead of
// using BentoCard, because it needs to bleed the blurred cover image to the
// card edge — BentoCard's default padding would clip it.
function ContinueReadingHero({
  book,
  featured,
  className = "",
  onOpenJournal,
  onViewQueue,
  onOpenCatalog,
}: {
  book?: ReadingLogItem;
  featured?: Book;
  className?: string;
  onOpenJournal: (logId: number) => void;
  onViewQueue: () => void;
  onOpenCatalog: () => void;
}) {
  const isContinuing = !!book;
  const coverUrl = book?.cover_url ?? featured?.cover_url;
  const title = book?.title ?? featured?.title;
  const author = book?.author ?? featured?.author;

  if (!title) {
    return (
      <div
        className={`flex min-h-72 flex-col items-center justify-center gap-2 rounded-3xl border border-border bg-surface p-6 text-center shadow-sm ${className}`}
      >
        <span className="text-3xl">📖</span>
        <p className="font-display text-text">Nothing on your shelf yet</p>
      </div>
    );
  }

  const progressPct =
    book?.total_pages && book.total_pages > 0
      ? Math.min(100, Math.round(((book.current_page ?? 0) / book.total_pages) * 100))
      : null;

  return (
    <div
      className={`relative flex min-h-72 flex-col justify-end overflow-hidden rounded-3xl border border-border bg-surface shadow-sm transition-shadow duration-200 hover:shadow-md ${className}`}
    >
      {coverUrl && (
        <img
          src={coverUrl}
          alt=""
          aria-hidden
          className="absolute inset-0 h-full w-full scale-110 object-cover opacity-30 blur-lg"
        />
      )}
      <div className="absolute inset-0 bg-gradient-to-t from-surface via-surface/85 to-surface/20" />

      <div className="relative flex flex-col gap-4 p-6 sm:flex-row sm:items-end sm:gap-6">
        {coverUrl && (
          <img
            src={coverUrl}
            alt={title}
            className="h-40 w-28 shrink-0 rounded-md object-cover shadow-xl sm:h-48 sm:w-32"
          />
        )}
        <div className="min-w-0 flex-1">
          <p className="text-xs font-semibold uppercase tracking-wide text-accent">
            {isContinuing ? "Continue reading" : "Featured pick"}
          </p>
          <h2 className="mt-1 font-display text-2xl font-bold text-text sm:text-3xl">{title}</h2>
          {author && <p className="mt-1 text-sm text-text-muted">{author}</p>}

          {progressPct !== null && (
            <div className="mt-3 max-w-sm">
              <div className="h-1.5 w-full overflow-hidden rounded-full bg-border">
                <div
                  className="h-full rounded-full bg-primary transition-all duration-300"
                  style={{ width: `${progressPct}%` }}
                />
              </div>
              <p className="mt-1 text-xs text-text-muted">
                {book?.current_page ?? 0} / {book?.total_pages} pages · {progressPct}%
              </p>
            </div>
          )}

          <div className="mt-4 flex gap-3">
            {isContinuing && book ? (
              <>
                <Button onClick={() => onOpenJournal(book.reading_log_id)}>Open Journal</Button>
                <Button variant="secondary" onClick={onViewQueue}>
                  View Queue
                </Button>
              </>
            ) : (
              <Button onClick={onOpenCatalog}>Open in Catalog</Button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export function Dashboard() {
  const navigate = useNavigate();
  const { user } = useAuth();
  const [exploreTab, setExploreTab] = useState<ExploreTab>("new");

  // ---- data: every query is independent and read-only. Actions (marking
  // returned, adding to wishlist, writing journal entries, ...) live on
  // each feature's own page — the dashboard is a scannable status board,
  // not a second place to perform every mutation. ----------------------
  const booksQuery = useQuery({ queryKey: ["books"], queryFn: () => api.books.list() });
  const loansQuery = useQuery({ queryKey: ["loans", "active"], queryFn: api.loans.active });
  const continueReadingQuery = useQuery({ queryKey: ["reading", "READING"], queryFn: () => api.reading.list("READING") });
  const wishlistQuery = useQuery({ queryKey: ["wishlist", "ACTIVE"], queryFn: () => api.wishlist.list("ACTIVE") });
  const borrowedQuery = useQuery({ queryKey: ["borrowed", "active"], queryFn: () => api.borrowed.list("active") });
  const recommendationsQuery = useQuery({ queryKey: ["recommendations"], queryFn: api.recommendations.list });
  const eventsQuery = useQuery({ queryKey: ["events"], queryFn: () => api.events.list() });

  const continueReading = continueReadingQuery.data?.[0];

  const entriesQuery = useQuery({
    queryKey: ["reading", continueReading?.reading_log_id, "entries"],
    queryFn: () => api.reading.entries(continueReading!.reading_log_id),
    enabled: !!continueReading,
  });

  const books = booksQuery.data ?? [];
  const activeLoans = loansQuery.data ?? [];
  const wishlist = wishlistQuery.data ?? [];
  const borrowed = borrowedQuery.data ?? [];
  const recommendations = recommendationsQuery.data ?? [];
  const events = eventsQuery.data ?? [];

  const newArrivals = useMemo(() => [...books].sort((a, b) => b.book_id - a.book_id).slice(0, 12), [books]);
  const fantasyBooks = useMemo(() => books.filter((b) => matchesGenre(b, FANTASY_WORDS)), [books]);
  const scifiBooks = useMemo(() => books.filter((b) => matchesGenre(b, SCIFI_WORDS)), [books]);

  const topDrops = useMemo(
    () =>
      [...wishlist]
        .filter((w) => w.latest_price != null)
        .sort((a, b) => (b.drop_pct ?? 0) - (a.drop_pct ?? 0))
        .slice(0, 3),
    [wishlist]
  );
  const upcomingBorrowed = useMemo(
    () => [...borrowed].sort((a, b) => (a.days_until_due ?? Infinity) - (b.days_until_due ?? Infinity)).slice(0, 3),
    [borrowed]
  );
  const upcomingLoans = useMemo(
    () => [...activeLoans].sort((a, b) => b.days_overdue - a.days_overdue).slice(0, 3),
    [activeLoans]
  );
  const upcomingEvents = useMemo(
    () => [...events].sort((a, b) => a.event_date.localeCompare(b.event_date)).slice(0, 3),
    [events]
  );
  const latestEntry = useMemo(() => {
    const entries = entriesQuery.data;
    if (!entries || entries.length === 0) return undefined;
    return [...entries].sort((a, b) => new Date(b.entry_date).getTime() - new Date(a.entry_date).getTime())[0];
  }, [entriesQuery.data]);

  if (booksQuery.isLoading) {
    return <DashboardSkeleton />;
  }

  if (booksQuery.isError) {
    return <p className="text-danger">⚠️ Error loading the dashboard: {(booksQuery.error as Error).message}</p>;
  }

  if (books.length === 0) {
    return (
      <EmptyState
        icon="📭"
        title="Your shelf is empty"
        description='Want to scan some books in the "Book Catalog"?'
        action={<Button onClick={() => navigate("/catalog")}>Go to Book Catalog</Button>}
      />
    );
  }

  const readingNowCount = continueReadingQuery.data?.length ?? 0;
  const overdueLoans = activeLoans.filter((l) => l.days_overdue > 0).length;
  const overdueBorrowed = borrowed.filter((b) => (b.days_until_due ?? 1) < 0).length;
  const featured = newArrivals[0];
  const topRecommendations = recommendations.slice(0, 5);
  const exploreBooks =
    exploreTab === "new" ? newArrivals : exploreTab === "fantasy" ? fantasyBooks : exploreTab === "scifi" ? scifiBooks : books;

  const stats: { label: string; value: number; tone?: "danger" | "success" | "neutral" }[] = [
    { label: "Books on shelf", value: books.length },
    { label: "Reading now", value: readingNowCount },
    { label: "Loaned out", value: activeLoans.length },
    {
      label: "Overdue",
      value: overdueLoans + overdueBorrowed,
      tone: overdueLoans + overdueBorrowed > 0 ? "danger" : "neutral",
    },
  ];

  return (
    <div>
      <header className="mb-6">
        <p className="text-xs font-semibold uppercase tracking-wider text-accent">
          {greeting()}
          {user?.first_name ? `, ${user.first_name}` : ""}
        </p>
        <h1 className="font-display text-2xl font-bold text-text sm:text-3xl">Liane's Library</h1>
      </header>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-6">
        <ContinueReadingHero
          className="sm:col-span-2 lg:col-span-4 lg:row-span-2"
          book={continueReading}
          featured={featured}
          onOpenJournal={(logId) => navigate(`/reading?tab=journal&log=${logId}`)}
          onViewQueue={() => navigate("/reading")}
          onOpenCatalog={() => navigate("/catalog")}
        />

        <BentoCard className="sm:col-span-2 lg:col-span-2 lg:row-span-2">
          <BentoHeader eyebrow="At a glance" title="Your library" />
          <div className="grid flex-1 grid-cols-2 gap-3">
            {stats.map((s) => (
              <StatTile key={s.label} label={s.label} value={s.value} tone={s.tone} />
            ))}
          </div>
        </BentoCard>

        <JournalPreviewCard
          className="lg:col-span-3"
          bookTitle={continueReading?.title ?? "Start a book"}
          coverUrl={continueReading?.cover_url}
          entryContent={latestEntry?.content}
          entryDate={latestEntry?.entry_date}
          mood={latestEntry?.mood}
          onOpen={() =>
            continueReading
              ? navigate(`/reading?tab=journal&log=${continueReading.reading_log_id}`)
              : navigate("/reading")
          }
        />

        <BentoCard className="lg:col-span-3">
          <BentoHeader eyebrow="Price watch" title="Wishlist" action={<SeeAllLink onClick={() => navigate("/wishlist")} />} />
          {wishlistQuery.isLoading ? (
            <CellSkeletonRows />
          ) : topDrops.length === 0 ? (
            <p className="text-sm text-text-muted">No price drops yet — add a title to start tracking.</p>
          ) : (
            <ul className="flex flex-col gap-3">
              {topDrops.map((item) => (
                <li key={item.wishlist_item_id} className="flex items-center gap-3">
                  <MiniCover coverUrl={item.cover_url} title={item.title} />
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-sm font-medium text-text">{item.title}</p>
                    <p className="text-xs text-text-muted tabular-nums">
                      {item.latest_currency ?? "BRL"} {item.latest_price?.toFixed(2)}
                    </p>
                  </div>
                  {item.drop_pct != null && item.drop_pct > 0 && (
                    <StatusBadge tone="price-drop" className="shrink-0">
                      ↓ {item.drop_pct}%
                    </StatusBadge>
                  )}
                </li>
              ))}
            </ul>
          )}
        </BentoCard>

        <BentoCard className="lg:col-span-2">
          <BentoHeader eyebrow="Due soon" title="Borrowed" action={<SeeAllLink onClick={() => navigate("/borrowed-by-me")} />} />
          {borrowedQuery.isLoading ? (
            <CellSkeletonRows />
          ) : upcomingBorrowed.length === 0 ? (
            <p className="text-sm text-text-muted">Nothing borrowed right now.</p>
          ) : (
            <ul className="flex flex-col gap-2.5">
              {upcomingBorrowed.map((record) => {
                const status = borrowTone(record);
                return (
                  <li key={record.borrow_id} className="flex items-center justify-between gap-2">
                    <div className="min-w-0">
                      <p className="truncate text-sm text-text">{record.title}</p>
                      <p className="truncate text-xs text-text-muted">from {record.lender_name}</p>
                    </div>
                    <StatusBadge tone={status.tone} className="shrink-0">
                      {status.label}
                    </StatusBadge>
                  </li>
                );
              })}
            </ul>
          )}
        </BentoCard>

        <BentoCard className="lg:col-span-2">
          <BentoHeader eyebrow="Lent out" title="Loans" action={<SeeAllLink onClick={() => navigate("/loans")} />} />
          {loansQuery.isLoading ? (
            <CellSkeletonRows />
          ) : upcomingLoans.length === 0 ? (
            <p className="text-sm text-text-muted">Nothing on loan right now.</p>
          ) : (
            <ul className="flex flex-col gap-2.5">
              {upcomingLoans.map((loan) => {
                const status = loanTone(loan);
                return (
                  <li key={loan.transaction_id} className="flex items-center justify-between gap-2">
                    <div className="min-w-0">
                      <p className="truncate text-sm text-text">{loan.book_title}</p>
                      <p className="truncate text-xs text-text-muted">with {loan.borrower_name}</p>
                    </div>
                    <StatusBadge tone={status.tone} className="shrink-0">
                      {status.label}
                    </StatusBadge>
                  </li>
                );
              })}
            </ul>
          )}
        </BentoCard>

        <BentoCard className="lg:col-span-2">
          <BentoHeader eyebrow="Because you read" title="For you" action={<SeeAllLink onClick={() => navigate("/recommendations")} />} />
          {recommendationsQuery.isLoading ? (
            <p className="text-sm text-text-muted">Finding matches…</p>
          ) : topRecommendations.length === 0 ? (
            <p className="text-sm text-text-muted">Add a genre or author to your catalog to unlock picks.</p>
          ) : (
            <div className="flex gap-2 overflow-x-hidden">
              {topRecommendations.map((item) => (
                <MiniCover key={item.recommendation_id} coverUrl={item.cover_url} title={item.title} />
              ))}
            </div>
          )}
        </BentoCard>

        <BentoCard className="sm:col-span-2 lg:col-span-3">
          <BentoHeader eyebrow="Near you" title="Literary events" action={<SeeAllLink onClick={() => navigate("/events")} />} />
          {eventsQuery.isLoading ? (
            <CellSkeletonRows />
          ) : upcomingEvents.length === 0 ? (
            <p className="text-sm text-text-muted">
              No upcoming events — set a city on the Events page to enable weekly discovery.
            </p>
          ) : (
            <div className="flex flex-col gap-2.5">
              {upcomingEvents.map((event) => (
                <EventTicket
                  key={event.event_id}
                  title={event.title}
                  venue={event.venue_name}
                  city={event.city}
                  date={event.event_date}
                  url={event.url}
                />
              ))}
            </div>
          )}
        </BentoCard>

        <BentoCard className="sm:col-span-2 lg:col-span-3">
          <BentoHeader eyebrow="Explore" title={EXPLORE_TABS.find((t) => t.id === exploreTab)?.label ?? "Library"} />
          <div className="mb-3 flex flex-wrap gap-1.5">
            {EXPLORE_TABS.map((tab) => (
              <button
                key={tab.id}
                type="button"
                onClick={() => setExploreTab(tab.id)}
                className={`rounded-full px-3 py-1 text-xs font-medium transition-colors ${
                  exploreTab === tab.id ? "bg-primary text-on-primary" : "bg-surface-hover text-text-muted hover:text-text"
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>
          <BookCarousel books={exploreBooks} />
        </BentoCard>
      </div>
    </div>
  );
}
