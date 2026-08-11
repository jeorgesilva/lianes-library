import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../lib/api";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import { Field, Input } from "../components/Input";
import { Tabs } from "../components/Tabs";
import { BarcodeScanner } from "../components/BarcodeScanner";

function SmartScan() {
  const queryClient = useQueryClient();
  const [lookup, setLookup] = useState<{ title: string; author: string; isbn: string; cover_url?: string } | null>(null);
  const [error, setError] = useState<string | null>(null);

  const lookupMutation = useMutation({
    mutationFn: async (isbn: string) => {
      const book = await api.openLibrary.lookup(isbn);
      if (!book) throw new Error("No book found for that ISBN.");
      return { ...book, isbn };
    },
    onSuccess: setLookup,
    onError: (e: Error) => setError(e.message),
  });

  const saveMutation = useMutation({
    mutationFn: () =>
      api.books.create({
        title: lookup!.title,
        author: lookup!.author,
        isbn: lookup!.isbn,
        cost: 0,
        cover_url: lookup!.cover_url,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["books"] });
      setLookup(null);
    },
  });

  return (
    <div className="flex flex-col gap-4">
      <BarcodeScanner
        onDecode={(isbn) => {
          setError(null);
          lookupMutation.mutate(isbn);
        }}
      />
      {error && <p className="text-sm text-danger">{error}</p>}
      {lookup && (
        <Card className="flex gap-4">
          {lookup.cover_url && <img src={lookup.cover_url} alt={lookup.title} className="h-32 w-24 object-cover rounded" />}
          <div className="flex flex-col gap-1">
            <strong>{lookup.title}</strong>
            <span className="text-text-muted text-sm">{lookup.author}</span>
            <span className="text-text-muted text-xs">ISBN {lookup.isbn}</span>
            <Button type="button" className="mt-2 w-fit" onClick={() => saveMutation.mutate()} disabled={saveMutation.isPending}>
              ➕ Save to Collection
            </Button>
          </div>
        </Card>
      )}
    </div>
  );
}

function AddManually() {
  const queryClient = useQueryClient();
  const [form, setForm] = useState({ title: "", author: "", isbn: "", cost: "" });

  const mutation = useMutation({
    mutationFn: () =>
      api.books.create({
        title: form.title,
        author: form.author,
        isbn: form.isbn,
        cost: form.cost ? Number(form.cost) : undefined,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["books"] });
      setForm({ title: "", author: "", isbn: "", cost: "" });
    },
  });

  return (
    <form
      className="flex max-w-sm flex-col gap-3"
      onSubmit={(e) => {
        e.preventDefault();
        mutation.mutate();
      }}
    >
      <Field label="Title">
        <Input required value={form.title} onChange={(e) => setForm({ ...form, title: e.target.value })} />
      </Field>
      <Field label="Author">
        <Input value={form.author} onChange={(e) => setForm({ ...form, author: e.target.value })} />
      </Field>
      <Field label="ISBN">
        <Input value={form.isbn} onChange={(e) => setForm({ ...form, isbn: e.target.value })} />
      </Field>
      <Field label="Cost">
        <Input type="number" step="0.01" value={form.cost} onChange={(e) => setForm({ ...form, cost: e.target.value })} />
      </Field>
      <Button type="submit" disabled={mutation.isPending} className="w-fit">
        Add Book
      </Button>
    </form>
  );
}

function Collection() {
  const { data: books, isLoading } = useQuery({ queryKey: ["books"], queryFn: () => api.books.list() });
  if (isLoading) return null;
  return (
    <div className="mt-6 overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-text-muted">
          <tr>
            <th className="pb-2">ID</th>
            <th className="pb-2">Title</th>
            <th className="pb-2">Author</th>
            <th className="pb-2">Status</th>
          </tr>
        </thead>
        <tbody>
          {books?.map((b) => (
            <tr key={b.book_id} className="border-t border-border">
              <td className="py-2">{b.book_id}</td>
              <td className="py-2">{b.title}</td>
              <td className="py-2">{b.author}</td>
              <td className="py-2">{b.book_status}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function BookCatalog() {
  return (
    <div>
      <h1 className="mb-6 text-2xl font-bold text-text">📖 Book Catalog</h1>
      <Tabs
        tabs={[
          { label: "📷 Smart Scan", content: <SmartScan /> },
          { label: "✍️ Add Manually", content: <AddManually /> },
        ]}
      />
      <Collection />
    </div>
  );
}
