export const dynamic = "force-dynamic";
import Link from "next/link";
import { ArrowRight, Search, Trophy, Users } from "lucide-react";

import { KpiCard } from "@/components/kpi-card";
import { PageHeader } from "@/components/page-header";
import { SkatersTable } from "@/components/tables/skaters-table";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { searchSkaters } from "@/lib/api";
import { formatDate, nationDisplay, number, points } from "@/lib/format";
import { buildMetadata } from "@/lib/site";

export const revalidate = 30;
export const metadata = buildMetadata({
  title: "Skaters",
  description: "Browse public figure-skating skater profiles, recent appearances, clubs, and best scores.",
  path: "/skaters",
});

export default async function Page({ searchParams }: { searchParams: Promise<{ q?: string }> }) {
  const sp = await searchParams;
  const q = sp.q?.trim();
  const { items } = await searchSkaters(q, 500);
  const featured = items[0];
  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Skater Index"
        title="Skaters"
        subtitle={
          q
            ? `${items.length} match${items.length === 1 ? "" : "es"} for "${q}"`
            : `${items.length} skaters indexed across the current public result set. Use the topbar search to narrow by name.`
        }
        meta={[
          `${number(items.length)} visible profiles`,
          q ? `Filtered by "${q}"` : "Sorted by latest appearance by default",
        ]}
        right={
          <Link
            href="/insights"
                className="inline-flex h-11 items-center gap-2 rounded-[10px] border border-input px-4 text-sm font-semibold text-foreground hover:bg-ice-2 hover:no-underline"
              >
            See analytics <ArrowRight className="h-4 w-4" />
          </Link>
        }
      />

      <section className="grid gap-4">
        <div className="grid gap-4 sm:grid-cols-3">
          <KpiCard label="Profiles" value={items.length} icon={Users} href="/skaters#directory" ctaLabel="Browse directory" />
          <KpiCard
            label="Latest Seen"
            value={featured ? formatDate(featured.latest_seen_date) : "—"}
            icon={Search}
            href={featured ? `/skaters/${featured.skater_key}` : "/skaters"}
            ctaLabel="Open latest profile"
          />
          <KpiCard
            label="Top Visible TSS"
            value={featured ? points(featured.best_tss) : "—"}
            icon={Trophy}
            hint={featured ? featured.skater_name : undefined}
            href={featured ? `/skaters/${featured.skater_key}` : "/skaters"}
            ctaLabel="Open featured skater"
          />
        </div>
      </section>

      {featured ? (
        <Card className="border-border bg-card">
          <CardHeader>
            <CardTitle className="text-base text-foreground">Featured visible profile</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
            <div>
              <p className="text-lg font-semibold text-foreground">{featured.skater_name}</p>
              <p className="mt-1 text-sm text-muted-foreground">
                {nationDisplay(featured.nation_code)}
                {featured.club_name ? ` · ${featured.club_name}` : ""}
                {` · latest seen ${formatDate(featured.latest_seen_date)}`}
              </p>
            </div>
            <Link href={`/skaters/${featured.skater_key}`} className="inline-flex items-center gap-2 text-sm font-medium text-brand hover:text-brand">
              Open profile <ArrowRight className="h-4 w-4" />
            </Link>
          </CardContent>
        </Card>
      ) : null}

      <Card id="directory" className="border-border bg-card scroll-mt-28">
        <CardHeader>
          <CardTitle className="text-base text-foreground">Skater directory</CardTitle>
        </CardHeader>
        <CardContent className="p-4"><SkatersTable rows={items} /></CardContent>
      </Card>
    </div>
  );
}
