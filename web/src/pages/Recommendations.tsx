import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type Recommendation } from "../lib/api";
import { Button } from "../components/Button";
import { EmptyState } from "../components/EmptyState";

function RecommendationCard({ item }: { item: Recommendation }) {
  const queryClient = useQueryClient();

  const dismissMutation = useMutation({
    mutationFn: () => api.recommendations.dismiss(item.recommendation_id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["recommendations"] }),
  });
  const wishlistMutation = useMutation({
    mutationFn: () =>
      api.wishlist.create({
        title: item.title,
        author: item.author ?? undefined,
        isbn: item.isbn ?? undefined,
        cover_url: item.cover_url ?? undefined,
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["wishlist"] }),
  });

  return (
    <div className="group w-36 shrink-0 sm:w-40">
      <div className="relative h-52 w-full overflow-hidden rounded-md bg-surface shadow-sm transition-transform duration-200 group-hover:scale-105 group-hover:shadow-lg sm:h-56">
        {item.cover_url ? (
          <img src={item.cover_url} alt={item.title} loading="lazy" className="h-full w-full object-cover" />
        ) : (
          <div className="flex h-full w-full items-center justify-center p-2 text-center font-display text-xs text-text-muted">
            {item.title.slice(0, 60)}
          </div>
        )}

        <div className="absolute inset-0 flex flex-col justify-end gap-1.5 bg-gradient-to-t from-black/90 via-black/40 to-transparent p-2 opacity-0 transition-opacity duration-200 group-hover:opacity-100">
          <span className="text-[11px] font-semibold leading-tight text-white">{item.title}</span>
          {item.author && <span className="text-[10px] text-white/70">{item.author}</span>}
          <div className="mt-1 flex flex-wrap gap-1.5">
            {item.best_price_url && (
              <a
                href={item.best_price_url}
                target="_blank"
                rel="noreferrer"
                className="rounded bg-primary/90 px-1.5 py-0.5 text-[10px] font-medium text-white hover:bg-primary"
              >
                {item.best_price != null ? `Buy · R$ ${item.best_price.toFixed(2)}` : "Buy"}
              </a>
            )}
            <button
              type="button"
              onClick={() => wishlistMutation.mutate()}
              disabled={wishlistMutation.isPending}
              className="rounded bg-white/15 px-1.5 py-0.5 text-[10px] font-medium text-white hover:bg-white/25"
            >
              + Wishlist
            </button>
            <button
              type="button"
              onClick={() => dismissMutation.mutate()}
              disabled={dismissMutation.isPending}
              className="rounded bg-white/15 px-1.5 py-0.5 text-[10px] font-medium text-white hover:bg-white/25"
            >
              Not interested
            </button>
          </div>
        </div>
      </div>

      <p className="mt-2 truncate text-xs font-medium text-text" title={item.title}>
        {item.title}
      </p>
      {item.author && (
        <p className="truncate text-[11px] text-text-muted" title={item.author}>
          {item.author}
        </p>
      )}
      {item.reason && <p className="mt-0.5 truncate text-[10px] italic text-text-muted" title={item.reason}>{item.reason}</p>}
    </div>
  );
}

function RecommendationRow({ title, items }: { title: string; items: Recommendation[] }) {
  if (items.length === 0) return null;
  return (
    <section className="mb-8">
      <h2 className="mb-2 font-display text-lg font-semibold text-text">{title}</h2>
      <div className="flex gap-3 overflow-x-auto pb-2 [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
        {items.map((item) => (
          <RecommendationCard key={item.recommendation_id} item={item} />
        ))}
      </div>
    </section>
  );
}

export function Recommendations() {
  const queryClient = useQueryClient();
  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["recommendations"],
    queryFn: api.recommendations.list,
  });

  const refreshMutation = useMutation({
    mutationFn: api.recommendations.refresh,
    onSuccess: (fresh) => queryClient.setQueryData(["recommendations"], fresh),
  });

  if (isLoading) return <p className="text-text-muted">Finding books you might like…</p>;
  if (isError) return <p className="text-danger">⚠️ {(error as Error).message}</p>;

  const items = data ?? [];
  const genres = [...new Set(items.map((i) => i.source_genre).filter((g): g is string => !!g))];
  const byAuthor = items.filter((i) => !i.source_genre);

  return (
    <div>
      <div className="mb-6 flex items-center justify-between">
        <h1 className="font-display text-2xl font-bold text-text">✨ Recommendations</h1>
        <Button variant="secondary" onClick={() => refreshMutation.mutate()} disabled={refreshMutation.isPending}>
          {refreshMutation.isPending ? "Refreshing…" : "Refresh"}
        </Button>
      </div>

      {items.length === 0 ? (
        <EmptyState
          icon="✨"
          title="No recommendations yet"
          description="Add a few books with a genre or author to your catalog, then hit Refresh — we'll suggest titles based on what's already on your shelf."
        />
      ) : (
        <>
          {genres.map((genre) => (
            <RecommendationRow
              key={genre}
              title={`Because you read ${genre}`}
              items={items.filter((i) => i.source_genre === genre)}
            />
          ))}
          <RecommendationRow title="More by authors you like" items={byAuthor} />
        </>
      )}
    </div>
  );
}
