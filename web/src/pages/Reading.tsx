import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type ReadingLogItem, type ReadingStatus } from "../lib/api";
import { Card } from "../components/Card";
import { Button } from "../components/Button";
import { Field, Input } from "../components/Input";
import { Tabs } from "../components/Tabs";
import { StatusBadge } from "../components/StatusBadge";
import { EmptyState } from "../components/EmptyState";

const COLUMNS: { status: ReadingStatus; label: string }[] = [
  { status: "WANT_TO_READ", label: "📖 Want to Read" },
  { status: "READING", label: "📗 Reading" },
  { status: "READ", label: "✅ Read" },
];

function AddToQueueForm() {
  const queryClient = useQueryClient();
  const { data: books } = useQuery({ queryKey: ["books"], queryFn: () => api.books.list() });
  const [bookId, setBookId] = useState<number | "">("");
  const [title, setTitle] = useState("");
  const [author, setAuthor] = useState("");
  const [totalPages, setTotalPages] = useState("");

  const mutation = useMutation({
    mutationFn: () =>
      api.reading.create({
        book_id: bookId === "" ? undefined : bookId,
        title: title || undefined,
        author: author || undefined,
        total_pages: totalPages ? Number(totalPages) : undefined,
        status: "WANT_TO_READ",
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["reading"] });
      setBookId("");
      setTitle("");
      setAuthor("");
      setTotalPages("");
    },
  });

  return (
    <Card className="mb-6">
      <h2 className="mb-3 font-display font-semibold text-text">Add to Queue</h2>
      <form
        className="grid grid-cols-1 gap-3 sm:grid-cols-4 sm:items-end"
        onSubmit={(e) => {
          e.preventDefault();
          mutation.mutate();
        }}
      >
        <Field label="From your catalog (optional)" className="sm:col-span-2">
          <select
            className="w-full rounded-lg border border-border bg-surface px-3 py-2 text-sm text-text"
            value={bookId}
            onChange={(e) => {
              const id = e.target.value ? Number(e.target.value) : "";
              setBookId(id);
              const book = books?.find((b) => b.book_id === id);
              if (book) {
                setTitle(book.title);
                setAuthor(book.author ?? "");
              }
            }}
          >
            <option value="">— Or type a new title below —</option>
            {books?.map((b) => (
              <option key={b.book_id} value={b.book_id}>
                {b.title}
              </option>
            ))}
          </select>
        </Field>
        <Field label="Title">
          <Input
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            disabled={bookId !== ""}
            required={bookId === ""}
          />
        </Field>
        <Field label="Author">
          <Input value={author} onChange={(e) => setAuthor(e.target.value)} disabled={bookId !== ""} />
        </Field>
        <Field label="Total pages">
          <Input
            type="number"
            min={1}
            value={totalPages}
            onChange={(e) => setTotalPages(e.target.value)}
          />
        </Field>
        <Button type="submit" disabled={mutation.isPending} className="sm:col-span-4 sm:w-fit">
          Add to Want to Read
        </Button>
      </form>
    </Card>
  );
}

function ProgressEditor({ item }: { item: ReadingLogItem }) {
  const queryClient = useQueryClient();
  const [page, setPage] = useState(String(item.current_page ?? ""));

  const mutation = useMutation({
    mutationFn: () => api.reading.updateProgress(item.reading_log_id, Number(page) || 0, item.total_pages ?? undefined),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["reading"] }),
  });

  const pct =
    item.total_pages && item.total_pages > 0
      ? Math.min(100, Math.round(((item.current_page ?? 0) / item.total_pages) * 100))
      : null;

  return (
    <div className="mt-2">
      {pct !== null && (
        <div className="mb-1.5 h-1.5 w-full overflow-hidden rounded-full bg-border">
          <div className="h-full rounded-full bg-primary" style={{ width: `${pct}%` }} />
        </div>
      )}
      <form
        className="flex items-center gap-1.5"
        onSubmit={(e) => {
          e.preventDefault();
          mutation.mutate();
        }}
      >
        <input
          type="number"
          min={0}
          max={item.total_pages ?? undefined}
          value={page}
          onChange={(e) => setPage(e.target.value)}
          placeholder="page"
          className="w-16 rounded-md border border-border bg-surface px-2 py-1 text-xs text-text"
        />
        <span className="text-xs text-text-muted">/ {item.total_pages ?? "?"}</span>
        <button
          type="submit"
          className="ml-auto rounded-md px-2 py-1 text-xs text-primary hover:bg-primary/10"
        >
          Update
        </button>
      </form>
    </div>
  );
}

function RatingStars({ item }: { item: ReadingLogItem }) {
  const queryClient = useQueryClient();
  const mutation = useMutation({
    mutationFn: (rating: number) => api.reading.updateStatus(item.reading_log_id, "READ", rating),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["reading"] }),
  });

  return (
    <div className="mt-1 flex gap-0.5">
      {[1, 2, 3, 4, 5].map((n) => (
        <button
          key={n}
          type="button"
          onClick={() => mutation.mutate(n)}
          className={`text-sm ${n <= (item.rating ?? 0) ? "text-warning" : "text-border"}`}
        >
          ★
        </button>
      ))}
    </div>
  );
}

function QueueCard({ item, onOpenJournal }: { item: ReadingLogItem; onOpenJournal: (id: number) => void }) {
  const queryClient = useQueryClient();
  const statusMutation = useMutation({
    mutationFn: (status: ReadingStatus) => api.reading.updateStatus(item.reading_log_id, status),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["reading"] }),
  });
  const deleteMutation = useMutation({
    mutationFn: () => api.reading.remove(item.reading_log_id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["reading"] }),
  });

  return (
    <Card className="flex gap-3">
      {item.cover_url ? (
        <img src={item.cover_url} alt={item.title} className="h-20 w-14 shrink-0 rounded object-cover" />
      ) : (
        <div className="flex h-20 w-14 shrink-0 items-center justify-center rounded bg-surface-hover text-center font-display text-[9px] text-text-muted">
          {item.title.slice(0, 30)}
        </div>
      )}
      <div className="min-w-0 flex-1">
        <div className="flex items-start justify-between gap-2">
          <p className="truncate text-sm font-medium text-text" title={item.title}>
            {item.title}
          </p>
          <button
            type="button"
            aria-label="Remove"
            onClick={() => deleteMutation.mutate()}
            className="shrink-0 text-xs text-text-muted hover:text-danger"
          >
            ✕
          </button>
        </div>
        {item.author && <p className="truncate text-xs text-text-muted">{item.author}</p>}

        {item.status === "READING" && <ProgressEditor item={item} />}
        {item.status === "READ" && <RatingStars item={item} />}
        {item.status === "DNF" && <StatusBadge tone="neutral" className="mt-1">Did not finish</StatusBadge>}

        <div className="mt-2 flex flex-wrap gap-2 text-xs">
          {item.status === "WANT_TO_READ" && (
            <button
              type="button"
              onClick={() => statusMutation.mutate("READING")}
              className="text-primary hover:underline"
            >
              Start reading →
            </button>
          )}
          {item.status === "DNF" && (
            <button
              type="button"
              onClick={() => statusMutation.mutate("WANT_TO_READ")}
              className="text-primary hover:underline"
            >
              ↺ Restart
            </button>
          )}
          {item.status === "READING" && (
            <>
              <button
                type="button"
                onClick={() => statusMutation.mutate("WANT_TO_READ")}
                className="text-text-muted hover:underline"
              >
                ← Back to queue
              </button>
              <button
                type="button"
                onClick={() => statusMutation.mutate("READ")}
                className="text-success hover:underline"
              >
                Mark as read ✓
              </button>
              <button
                type="button"
                onClick={() => statusMutation.mutate("DNF")}
                className="text-text-muted hover:underline"
              >
                Did not finish
              </button>
            </>
          )}
          <button type="button" onClick={() => onOpenJournal(item.reading_log_id)} className="text-accent hover:underline">
            📓 Journal
          </button>
        </div>
      </div>
    </Card>
  );
}

function QueuePanel({ onOpenJournal }: { onOpenJournal: (id: number) => void }) {
  const { data: items, isLoading } = useQuery({ queryKey: ["reading"], queryFn: () => api.reading.list() });

  return (
    <div>
      <AddToQueueForm />
      {isLoading ? (
        <p className="text-text-muted">Loading...</p>
      ) : !items || items.length === 0 ? (
        <EmptyState icon="📚" title="Your reading queue is empty" description="Add a book above to start planning what's next." />
      ) : (
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
          {COLUMNS.map((col) => {
            const colItems = items.filter((i) => i.status === col.status || (col.status === "READ" && i.status === "DNF"));
            return (
              <div key={col.status}>
                <h3 className="mb-2 font-display text-sm font-semibold text-text">
                  {col.label} <span className="text-text-muted">({colItems.length})</span>
                </h3>
                <div className="flex flex-col gap-3">
                  {colItems.map((item) => (
                    <QueueCard key={item.reading_log_id} item={item} onOpenJournal={onOpenJournal} />
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function JournalEntryRow({ entry }: { entry: { entry_id: number; entry_date: string; content: string; page_at_entry: number | null; mood: string | null; contains_spoilers: number } }) {
  const [revealed, setRevealed] = useState(false);
  const spoiler = entry.contains_spoilers === 1;

  return (
    <div className="border-b border-border py-3 last:border-0">
      <div className="flex items-center gap-2 text-xs text-text-muted">
        <span>{new Date(entry.entry_date).toLocaleDateString()}</span>
        {entry.page_at_entry != null && <span>· page {entry.page_at_entry}</span>}
        {entry.mood && <StatusBadge tone="neutral">{entry.mood}</StatusBadge>}
      </div>
      <p
        className={`mt-1 whitespace-pre-wrap text-sm text-text ${spoiler && !revealed ? "cursor-pointer select-none blur-sm" : ""}`}
        onClick={() => spoiler && setRevealed(true)}
      >
        {spoiler && !revealed ? "Contains spoilers — click to reveal" : entry.content}
      </p>
    </div>
  );
}

function JournalPanel({ logId, onSelectLog }: { logId: number | null; onSelectLog: (id: number) => void }) {
  const queryClient = useQueryClient();
  const { data: items } = useQuery({ queryKey: ["reading"], queryFn: () => api.reading.list() });
  const { data: entries, isLoading } = useQuery({
    queryKey: ["reading", logId, "entries"],
    queryFn: () => api.reading.entries(logId as number),
    enabled: logId !== null,
  });

  const [content, setContent] = useState("");
  const [mood, setMood] = useState("");
  const [pageAtEntry, setPageAtEntry] = useState("");
  const [spoilers, setSpoilers] = useState(false);

  const mutation = useMutation({
    mutationFn: () =>
      api.reading.addEntry(logId as number, {
        content,
        mood: mood || undefined,
        page_at_entry: pageAtEntry ? Number(pageAtEntry) : undefined,
        contains_spoilers: spoilers,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["reading", logId, "entries"] });
      setContent("");
      setMood("");
      setPageAtEntry("");
      setSpoilers(false);
    },
  });

  if (!items || items.length === 0) {
    return <EmptyState icon="📓" title="Nothing to journal yet" description="Add a book to your reading queue first." />;
  }

  const selected = items.find((i) => i.reading_log_id === logId) ?? items[0];

  return (
    <div>
      <Field label="Book" className="mb-4 max-w-sm">
        <select
          className="w-full rounded-lg border border-border bg-surface px-3 py-2 text-sm text-text"
          value={selected.reading_log_id}
          onChange={(e) => onSelectLog(Number(e.target.value))}
        >
          {items.map((i) => (
            <option key={i.reading_log_id} value={i.reading_log_id}>
              {i.title}
            </option>
          ))}
        </select>
      </Field>

      <Card className="mb-6">
        <form
          className="flex flex-col gap-3"
          onSubmit={(e) => {
            e.preventDefault();
            if (content.trim()) mutation.mutate();
          }}
        >
          <textarea
            className="min-h-24 w-full rounded-lg border border-border bg-surface px-3 py-2 text-sm text-text placeholder:text-text-muted focus:outline-none focus:ring-2 focus:ring-primary"
            placeholder="What's on your mind about this book?"
            value={content}
            onChange={(e) => setContent(e.target.value)}
          />
          <div className="flex flex-wrap items-center gap-3">
            <Input
              className="w-32"
              placeholder="mood, e.g. tense"
              value={mood}
              onChange={(e) => setMood(e.target.value)}
            />
            <Input
              className="w-24"
              type="number"
              min={0}
              placeholder="page"
              value={pageAtEntry}
              onChange={(e) => setPageAtEntry(e.target.value)}
            />
            <label className="flex items-center gap-1.5 text-xs text-text-muted">
              <input type="checkbox" checked={spoilers} onChange={(e) => setSpoilers(e.target.checked)} />
              Contains spoilers
            </label>
            <Button type="submit" disabled={mutation.isPending || !content.trim()} className="ml-auto">
              Add entry
            </Button>
          </div>
        </form>
      </Card>

      {isLoading ? (
        <p className="text-text-muted">Loading entries...</p>
      ) : !entries || entries.length === 0 ? (
        <EmptyState icon="✍️" title="No entries yet" description="Write your first note about this book above." />
      ) : (
        <Card>
          {entries.map((entry) => (
            <JournalEntryRow key={entry.entry_id} entry={entry} />
          ))}
        </Card>
      )}
    </div>
  );
}

export function Reading() {
  const [searchParams, setSearchParams] = useSearchParams();
  const logParam = searchParams.get("log");
  const [selectedLog, setSelectedLog] = useState<number | null>(logParam ? Number(logParam) : null);
  const [activeTab, setActiveTab] = useState(searchParams.get("tab") === "journal" ? 1 : 0);

  useEffect(() => {
    if (logParam) setSelectedLog(Number(logParam));
  }, [logParam]);

  const openJournal = (id: number) => {
    setSelectedLog(id);
    setActiveTab(1);
    setSearchParams({ tab: "journal", log: String(id) });
  };

  return (
    <div>
      <h1 className="mb-6 font-display text-2xl font-bold text-text">📚 My Reading</h1>
      <Tabs
        tabs={[
          { label: "Queue", content: <QueuePanel onOpenJournal={openJournal} /> },
          {
            label: "Journal",
            content: (
              <JournalPanel
                logId={selectedLog}
                onSelectLog={(id) => {
                  setSelectedLog(id);
                  setSearchParams({ tab: "journal", log: String(id) });
                }}
              />
            ),
          },
        ]}
        active={activeTab}
        onChange={(i) => {
          setActiveTab(i);
          setSearchParams(i === 1 ? { tab: "journal", ...(selectedLog ? { log: String(selectedLog) } : {}) } : {});
        }}
      />
    </div>
  );
}
