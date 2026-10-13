export const dynamic = "force-dynamic";
import Link from "next/link";
import { ArrowRight, CalendarRange, Globe2, MapPin } from "lucide-react";

import { KpiCard } from "@/components/kpi-card";
import { PageHeader } from "@/components/page-header";
import { EventsTable } from "@/components/tables/events-table";
import { YearRangeFilter } from "@/components/year-range-filter";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { listEvents } from "@/lib/api";
import { formatDate, number } from "@/lib/format";
import { buildMetadata } from "@/lib/site";

export const revalidate = 30;
export const metadata = buildMetadata({
  title: "Events",
  description: "Browse public competition pages with dates, locations, event types, and official result sources.",
  path: "/events",
});

export default async function Page({
  searchParams,
}: {
  searchParams: Promise<{ q?: string; from_year?: string; to_year?: string }>;
}) {
  const sp = await searchParams;
  const from = sp.from_year ? Number(sp.from_year) : undefined;
  const to = sp.to_year ? Number(sp.to_year) : undefined;
  const { items } = await listEvents({ q: sp.q, from_year: from, to_year: to, limit: 1000 });
  const featured = items[0];
  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Competition Index"
        title="Events"
        subtitle={`${items.length} event${items.length === 1 ? "" : "s"}${from || to ? ` (${from ?? "…"}–${to ?? "…"})` : ""}${sp.q ? ` matching "${sp.q}"` : ""}`}
        meta={[
          `${number(items.length)} visible competitions`,
          from || to ? `Year filter ${from ?? "…"}–${to ?? "…"} ` : "Sorted by date by default",
        ]}
        right={<YearRangeFilter minYear={2013} maxYear={2026} />}
      />

      <section className="grid gap-4">
        <div className="grid gap-4 sm:grid-cols-3">
          <KpiCard label="Events" value={items.length} icon={CalendarRange} href="/events#archive" ctaLabel="Browse archive" />
          <KpiCard
            label="Latest Date"
            value={featured ? formatDate(featured.event_start_date) : "—"}
            icon={Globe2}
            href={featured ? `/events/${featured.event_key}` : "/events"}
            ctaLabel="Open latest event"
          />
          <KpiCard
            label="Featured Location"
            value={featured?.location ?? "—"}
            icon={MapPin}
            hint={featured?.event_name ?? undefined}
            href={featured ? `/events/${featured.event_key}` : "/events"}
            ctaLabel="Open featured event"
          />
        </div>
      </section>

      {featured ? (
        <Card className="border-border bg-card">
          <CardHeader>
            <CardTitle className="text-base text-foreground">Featured visible competition</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
            <div>
              <p className="text-lg font-semibold text-foreground">{featured.event_name ?? "Unnamed event"}</p>
              <p className="mt-1 text-sm text-muted-foreground">
                {formatDate(featured.event_start_date)} · {featured.location ?? "Location pending"} · {featured.competition_stream ?? featured.event_type ?? "format pending"}
              </p>
            </div>
            <Link href={`/events/${featured.event_key}`} className="inline-flex items-center gap-2 text-sm font-medium text-brand hover:text-brand">
              Open event page <ArrowRight className="h-4 w-4" />
            </Link>
          </CardContent>
        </Card>
      ) : null}

      <Card id="archive" className="border-border bg-card scroll-mt-28">
        <CardHeader>
          <CardTitle className="text-base text-foreground">Competition archive</CardTitle>
        </CardHeader>
        <CardContent className="p-4"><EventsTable rows={items} /></CardContent>
      </Card>
    </div>
  );
}
