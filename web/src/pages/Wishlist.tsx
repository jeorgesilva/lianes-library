import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type WishlistItem } from "../lib/api";
import { Card } from "../components/Card";
import { Button } from "../components/Button";
import { Field, Input } from "../components/Input";
import { StatusBadge } from "../components/StatusBadge";
import { EmptyState } from "../components/EmptyState";
import { BarcodeScanner } from "../components/BarcodeScanner";
import { Sparkline } from "../components/Sparkline";

function AddWishlistForm() {
  const queryClient = useQueryClient();
  const [title, setTitle] = useState("");
  const [author, setAuthor] = useState("");
  const [isbn, setIsbn] = useState("");
  const [coverUrl, setCoverUrl] = useState("");
  const [targetPrice, setTargetPrice] = useState("");
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
      api.wishlist.create({
        title,
        author: author || undefined,
        isbn: isbn || undefined,
        cover_url: coverUrl || undefined,
        target_price: targetPrice ? Number(targetPrice) : undefined,
        notes: notes || undefined,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["wishlist"] });
      setTitle("");
      setAuthor("");
      setIsbn("");
      setCoverUrl("");
      setTargetPrice("");
      setNotes("");
    },
  });

  return (
    <Card className="mb-6">
      <h2 className="mb-3 font-display font-semibold text-text">Add a book to your wishlist</h2>
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
        <Field label="Target price (optional)">
          <Input
            type="number"
            min="0"
            step="0.01"
            value={targetPrice}
            onChange={(e) => setTargetPrice(e.target.value)}
            placeholder="Notify me at or below this price"
          />
        </Field>
        <Field label="Notes">
          <Input value={notes} onChange={(e) => setNotes(e.target.value)} />
        </Field>
        <Button type="submit" disabled={mutation.isPending || !title} className="sm:col-span-2 sm:w-fit">
          Add to Wishlist
        </Button>
      </form>
    </Card>
  );
}

function WishlistCard({ item }: { item: WishlistItem }) {
  const queryClient = useQueryClient();
  const { data: snapshots } = useQuery({
    queryKey: ["wishlist", item.wishlist_item_id, "snapshots"],
    queryFn: () => api.wishlist.snapshots(item.wishlist_item_id),
  });

  const purchaseMutation = useMutation({
    mutationFn: () => api.wishlist.markPurchased(item.wishlist_item_id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["wishlist"] }),
  });
  const archiveMutation = useMutation({
    mutationFn: () => api.wishlist.archive(item.wishlist_item_id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["wishlist"] }),
  });
  const removeMutation = useMutation({
    mutationFn: () => api.wishlist.remove(item.wishlist_item_id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["wishlist"] }),
  });

  const currency = item.latest_currency ?? "BRL";
  const isLowest = item.latest_price != null && item.lowest_price != null && item.latest_price <= item.lowest_price;

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
            onClick={() => removeMutation.mutate()}
            className="shrink-0 text-xs text-text-muted hover:text-danger"
          >
            ✕
          </button>
        </div>
        {item.author && <p className="truncate text-xs text-text-muted">{item.author}</p>}

        {item.latest_price != null ? (
          <div className="mt-2 flex items-center gap-2">
            <span className="font-display text-sm font-semibold text-text">
              {currency} {item.latest_price.toFixed(2)}
            </span>
            {isLowest && item.drop_pct != null && item.drop_pct > 0 && (
              <StatusBadge tone="price-drop">↓ {item.drop_pct}% (lowest yet)</StatusBadge>
            )}
          </div>
        ) : (
          <p className="mt-2 text-xs text-text-muted">No price found yet — checked daily.</p>
        )}

        {snapshots && snapshots.length > 1 && (
          <div className="mt-1">
            <Sparkline values={snapshots.map((s) => s.price)} />
          </div>
        )}

        <div className="mt-2 flex flex-wrap items-center gap-3">
          {item.latest_price_url && (
            <a href={item.latest_price_url} target="_blank" rel="noreferrer" className="text-xs text-primary hover:underline">
              Buy →
            </a>
          )}
          <button type="button" onClick={() => purchaseMutation.mutate()} className="text-xs text-primary hover:underline">
            Mark purchased
          </button>
          <button type="button" onClick={() => archiveMutation.mutate()} className="text-xs text-text-muted hover:underline">
            Archive
          </button>
        </div>
      </div>
    </Card>
  );
}

function WishlistGrid({ status }: { status: "ACTIVE" | "PURCHASED" | "ARCHIVED" }) {
  const { data: items, isLoading } = useQuery({
    queryKey: ["wishlist", status],
    queryFn: () => api.wishlist.list(status),
  });

  if (isLoading) return <p className="text-text-muted">Loading...</p>;
  if (!items || items.length === 0) {
    return status === "ACTIVE" ? (
      <EmptyState icon="💜" title="Nothing on your wishlist" description="Add a title above and it'll be checked for price drops daily." />
    ) : (
      <EmptyState icon="📦" title="Nothing here" />
    );
  }

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
      {items.map((item) => (
        <WishlistCard key={item.wishlist_item_id} item={item} />
      ))}
    </div>
  );
}

export function Wishlist() {
  return (
    <div>
      <h1 className="mb-6 font-display text-2xl font-bold text-text">💜 Wishlist</h1>
      <AddWishlistForm />
      <h2 className="mb-3 font-display text-sm font-semibold uppercase tracking-wider text-text-muted">Watching</h2>
      <WishlistGrid status="ACTIVE" />
      <h2 className="mb-3 mt-8 font-display text-sm font-semibold uppercase tracking-wider text-text-muted">Purchased</h2>
      <WishlistGrid status="PURCHASED" />
    </div>
  );
}
