import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type BorrowRecord } from "../lib/api";
import { Card } from "../components/Card";
import { Button } from "../components/Button";
import { Field, Input } from "../components/Input";
import { StatusBadge } from "../components/StatusBadge";
import { EmptyState } from "../components/EmptyState";
import { BarcodeScanner } from "../components/BarcodeScanner";

function AddBorrowForm() {
  const queryClient = useQueryClient();
  const [title, setTitle] = useState("");
  const [author, setAuthor] = useState("");
  const [isbn, setIsbn] = useState("");
  const [coverUrl, setCoverUrl] = useState("");
  const [lenderName, setLenderName] = useState("");
  const [dueDate, setDueDate] = useState("");
  const [notes, setNotes] = useState("");

  const lookupMutation = useMutation({
    mutationFn: async (scannedIsbn: string) => {
      const book = await api.openLibrary.lookup(scannedIsbn);
      if (!book) throw new Error("No book found for that ISBN.");
      return { ...book, isbn: scannedIsbn };
    },
    onSuccess: (book) => {
      setTitle(book.title);
      setAuthor(book.author);
      setIsbn(book.isbn);
      setCoverUrl(book.cover_url ?? "");
    },
  });

  const mutation = useMutation({
    mutationFn: () =>
      api.borrowed.create({
        title,
        lender_name: lenderName,
        author: author || undefined,
        isbn: isbn || undefined,
        cover_url: coverUrl || undefined,
        due_date: dueDate || undefined,
        notes: notes || undefined,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["borrowed"] });
      setTitle("");
      setAuthor("");
      setIsbn("");
      setCoverUrl("");
      setLenderName("");
      setDueDate("");
      setNotes("");
    },
  });

  return (
    <Card className="mb-6">
      <h2 className="mb-3 font-display font-semibold text-text">Add a book you borrowed</h2>
      <BarcodeScanner
        onDecode={(scannedIsbn) => {
          lookupMutation.mutate(scannedIsbn);
        }}
      />
      {lookupMutation.isError && (
        <p className="mt-2 text-sm text-danger">{(lookupMutation.error as Error).message}</p>
      )}
      <form
        className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2"
        onSubmit={(e) => {
          e.preventDefault();
          mutation.mutate();
        }}
      >
        <Field label="Title">
          <Input required value={title} onChange={(e) => setTitle(e.target.value)} />
        </Field>
        <Field label="Author">
          <Input value={author} onChange={(e) => setAuthor(e.target.value)} />
        </Field>
        <Field label="From whom (person or place)">
          <Input required value={lenderName} onChange={(e) => setLenderName(e.target.value)} placeholder="e.g. Municipal Library, Marina" />
        </Field>
        <Field label="Agreed return date">
          <Input type="date" value={dueDate} onChange={(e) => setDueDate(e.target.value)} />
        </Field>
        <Field label="Notes" className="sm:col-span-2">
          <Input value={notes} onChange={(e) => setNotes(e.target.value)} />
        </Field>
        <Button type="submit" disabled={mutation.isPending || !title || !lenderName} className="sm:col-span-2 sm:w-fit">
          Add to Borrowed
        </Button>
      </form>
    </Card>
  );
}

function dueTone(record: BorrowRecord): { tone: "success" | "warning" | "danger" | "neutral"; label: string } {
  if (record.returned_date) return { tone: "neutral", label: "Returned" };
  if (record.days_until_due == null) return { tone: "neutral", label: "No due date" };
  if (record.days_until_due < 0) return { tone: "danger", label: `${Math.abs(record.days_until_due)}d overdue` };
  if (record.days_until_due <= record.reminder_lead_days) return { tone: "warning", label: `Due in ${record.days_until_due}d` };
  return { tone: "success", label: `Due in ${record.days_until_due}d` };
}

function BorrowCard({ record }: { record: BorrowRecord }) {
  const queryClient = useQueryClient();
  const returnMutation = useMutation({
    mutationFn: () => api.borrowed.return(record.borrow_id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["borrowed"] }),
  });
  const removeMutation = useMutation({
    mutationFn: () => api.borrowed.remove(record.borrow_id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["borrowed"] }),
  });
  const status = dueTone(record);

  return (
    <Card className="flex gap-3">
      {record.cover_url ? (
        <img src={record.cover_url} alt={record.title} className="h-20 w-14 shrink-0 rounded object-cover" />
      ) : (
        <div className="flex h-20 w-14 shrink-0 items-center justify-center rounded bg-surface-hover text-center font-display text-[9px] text-text-muted">
          {record.title.slice(0, 30)}
        </div>
      )}
      <div className="min-w-0 flex-1">
        <div className="flex items-start justify-between gap-2">
          <p className="truncate text-sm font-medium text-text" title={record.title}>
            {record.title}
          </p>
          <button
            type="button"
            aria-label="Remove"
            onClick={() => removeMutation.mutate()}
            className="shrink-0 text-xs text-text-muted hover:text-danger"
          >
            ✕
          </button>
        </div>
        {record.author && <p className="truncate text-xs text-text-muted">{record.author}</p>}
        <p className="mt-1 text-xs text-text-muted">From {record.lender_name}</p>
        <div className="mt-2 flex flex-wrap items-center gap-2">
          <StatusBadge tone={status.tone}>{status.label}</StatusBadge>
          {!record.returned_date && (
            <button
              type="button"
              onClick={() => returnMutation.mutate()}
              disabled={returnMutation.isPending}
              className="text-xs text-primary hover:underline"
            >
              Mark returned
            </button>
          )}
        </div>
      </div>
    </Card>
  );
}

function BorrowList({ status }: { status: "active" | "returned" }) {
  const { data: records, isLoading } = useQuery({
    queryKey: ["borrowed", status],
    queryFn: () => api.borrowed.list(status),
  });

  if (isLoading) return <p className="text-text-muted">Loading...</p>;
  if (!records || records.length === 0) {
    return status === "active" ? (
      <EmptyState icon="📦" title="Nothing borrowed right now" description="Add a book above when you borrow one from a library or friend." />
    ) : (
      <EmptyState icon="✅" title="No returned books yet" description="Books you've returned will show up here." />
    );
  }

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
      {records.map((record) => (
        <BorrowCard key={record.borrow_id} record={record} />
      ))}
    </div>
  );
}

export function BorrowedByMe() {
  return (
    <div>
      <h1 className="mb-6 font-display text-2xl font-bold text-text">📦 Borrowed by Me</h1>
      <AddBorrowForm />
      <h2 className="mb-3 font-display text-sm font-semibold uppercase tracking-wider text-text-muted">Currently borrowed</h2>
      <BorrowList status="active" />
      <h2 className="mb-3 mt-8 font-display text-sm font-semibold uppercase tracking-wider text-text-muted">Returned</h2>
      <BorrowList status="returned" />
    </div>
  );
}
