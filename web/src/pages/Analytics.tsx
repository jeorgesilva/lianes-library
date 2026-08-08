import { useQuery } from "@tanstack/react-query";
import { api } from "../lib/api";
import { Card } from "../components/Card";
import { MonthlyBarChart } from "../components/charts/MonthlyBarChart";
import { RateLineChart } from "../components/charts/RateLineChart";
import { TopBooksBar } from "../components/charts/TopBooksBar";

const PRIMARY = "#6c63ff";
const WARNING = "#fab219";

function StatTile({ label, value }: { label: string; value: number }) {
  return (
    <Card>
      <p className="text-sm text-text-muted">{label}</p>
      <p className="mt-1 text-3xl font-semibold text-text">{value.toLocaleString()}</p>
    </Card>
  );
}

export function Analytics() {
  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["analytics", "summary"],
    queryFn: () => api.analytics.summary(),
  });

  if (isLoading) return <p className="text-text-muted">Loading...</p>;
  if (isError) return <p className="text-red-400">⚠️ Error loading analytics: {(error as Error).message}</p>;
  if (!data) return null;

  return (
    <div>
      <h1 className="mb-6 text-2xl font-bold text-text">📊 Analytics</h1>

      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        <StatTile label="Books in collection" value={data.totals.books} />
        <StatTile label="Active borrowers" value={data.totals.borrowers} />
        <StatTile label="Active loans" value={data.totals.active_loans} />
        <StatTile label="Overdue right now" value={data.totals.overdue_now} />
      </div>

      <div className="mt-6 grid gap-4 lg:grid-cols-2">
        <Card>
          <h2 className="mb-3 font-semibold text-text">Loans per month</h2>
          <MonthlyBarChart
            data={data.loans_per_month.map((d) => ({ month: d.month, value: d.count }))}
            color={PRIMARY}
          />
        </Card>

        <Card>
          <h2 className="mb-3 font-semibold text-text">Late-return rate</h2>
          <RateLineChart
            data={data.late_return_rate_per_month.map((d) => ({ month: d.month, pct: d.late_pct, total: d.total }))}
            color={WARNING}
          />
        </Card>
      </div>

      <Card className="mt-4">
        <h2 className="mb-3 font-semibold text-text">Most-borrowed books</h2>
        <TopBooksBar books={data.top_books} color={PRIMARY} />
      </Card>
    </div>
  );
}
