export function Skeleton({ className = "" }: { className?: string }) {
  return <div className={`animate-pulse rounded-md bg-surface-hover ${className}`} />;
}

export function BookCardSkeleton() {
  return (
    <div className="w-32 shrink-0 sm:w-36">
      <Skeleton className="h-44 w-full sm:h-52" />
      <Skeleton className="mt-2 h-3 w-4/5" />
      <Skeleton className="mt-1.5 h-3 w-3/5" />
    </div>
  );
}

export function BookRowSkeleton({ title }: { title?: string }) {
  return (
    <section className="mb-8">
      {title ? (
        <h2 className="mb-3 font-display text-lg font-semibold text-text">{title}</h2>
      ) : (
        <Skeleton className="mb-3 h-5 w-40" />
      )}
      <div className="flex gap-3 overflow-x-hidden pb-2">
        {Array.from({ length: 6 }).map((_, i) => (
          <BookCardSkeleton key={i} />
        ))}
      </div>
    </section>
  );
}
