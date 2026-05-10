import { PageHeader } from "@/components/page-header";
import { EventsTable } from "@/components/tables/events-table";
import { YearRangeFilter } from "@/components/year-range-filter";
import { Card, CardContent } from "@/components/ui/card";
import { listEvents } from "@/lib/api";

export const revalidate = 30;

export default async function Page({
  searchParams,
}: {
  searchParams: Promise<{ q?: string; from_year?: string; to_year?: string }>;
}) {
  const sp = await searchParams;
  const from = sp.from_year ? Number(sp.from_year) : undefined;
  const to = sp.to_year ? Number(sp.to_year) : undefined;
  const { items } = await listEvents({ q: sp.q, from_year: from, to_year: to, limit: 1000 });
  return (
    <div className="space-y-6">
      <PageHeader
        title="Events"
        subtitle={`${items.length} event${items.length === 1 ? "" : "s"}${from || to ? ` (${from ?? "…"}–${to ?? "…"})` : ""}${sp.q ? ` matching "${sp.q}"` : ""}`}
        right={<YearRangeFilter minYear={2013} maxYear={2026} />}
      />
      <Card><CardContent className="p-4"><EventsTable rows={items} /></CardContent></Card>
    </div>
  );
}
