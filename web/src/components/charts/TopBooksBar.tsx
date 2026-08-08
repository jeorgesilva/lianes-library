interface TopBook {
  book_id: number;
  title: string;
  author: string | null;
  loan_count: number;
}

interface Props {
  books: TopBook[];
  color: string;
}

// Ranked horizontal bars — direct count labels make sense at N=10 (ranking IS
// the story), so no hover layer is needed here.
export function TopBooksBar({ books, color }: Props) {
  if (books.length === 0) {
    return <p className="text-sm text-text-muted">No loans recorded yet.</p>;
  }

  const max = Math.max(...books.map((b) => b.loan_count), 1);

  return (
    <ul className="flex flex-col gap-2">
      {books.map((b) => (
        <li key={b.book_id} className="flex items-center gap-3">
          <span className="w-40 shrink-0 truncate text-sm text-text" title={b.title}>
            {b.title}
          </span>
          <div className="h-4 flex-1 rounded bg-bg">
            <div
              className="h-4 rounded"
              style={{ width: `${Math.max((b.loan_count / max) * 100, 4)}%`, backgroundColor: color, opacity: 0.85 }}
            />
          </div>
          <span className="w-6 shrink-0 text-right text-sm font-semibold text-text tabular-nums">{b.loan_count}</span>
        </li>
      ))}
    </ul>
  );
}
