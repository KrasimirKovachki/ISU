import Link from "next/link";
import { ArrowRight, Building2, Trophy, Users } from "lucide-react";

import { KpiCard } from "@/components/kpi-card";
import { PageHeader } from "@/components/page-header";
import { ClubsTable } from "@/components/tables/clubs-table";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { listClubs } from "@/lib/api";
import { formatDate, nationDisplay, number, points } from "@/lib/format";

export const revalidate = 30;

export default async function Page({ searchParams }: { searchParams: Promise<{ q?: string }> }) {
  const sp = await searchParams;
  const { items } = await listClubs(sp.q);
  const featured = items[0];
  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Club Index"
        title="Clubs"
        subtitle={`${items.length} club${items.length === 1 ? "" : "s"} indexed${sp.q ? ` matching "${sp.q}"` : ""}`}
        meta={[
          `${number(items.length)} visible club pages`,
          sp.q ? `Filtered by "${sp.q}"` : "Sorted by score volume by default",
        ]}
      />

      <section className="grid gap-4 lg:grid-cols-[0.92fr_1.08fr]">
        <Card className="bg-slate-950 text-slate-50">
          <CardHeader>
            <CardTitle className="text-base text-white">Public club browse surface</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-6 text-slate-300">
            <p>Club pages are strong public entry points because families and coaches often follow a club before they follow an individual skater.</p>
            <p>This index should surface the most active clubs clearly and make it easy to move from club to roster pages.</p>
          </CardContent>
        </Card>
        <div className="grid gap-4 sm:grid-cols-3">
          <KpiCard label="Clubs" value={items.length} icon={Building2} />
          <KpiCard label="Top Visible Club" value={featured?.club_name ?? "—"} icon={Users} hint={featured ? nationDisplay(featured.country_code) : undefined} />
          <KpiCard label="Best Visible TSS" value={featured ? points(featured.best_tss) : "—"} icon={Trophy} hint={featured ? `latest ${formatDate(featured.latest_seen_date)}` : undefined} />
        </div>
      </section>

      {featured ? (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Featured club</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
            <div>
              <p className="text-lg font-semibold">{featured.club_name}</p>
              <p className="mt-1 text-sm text-muted-foreground">
                {nationDisplay(featured.country_code)} · {number(featured.skaters)} skaters · {number(featured.events)} events · latest {formatDate(featured.latest_seen_date)}
              </p>
            </div>
            <Link href={`/clubs/${encodeURIComponent(featured.club_name)}`} className="inline-flex items-center gap-2 text-sm font-medium text-sky-800 hover:text-sky-950">
              Open club page <ArrowRight className="h-4 w-4" />
            </Link>
          </CardContent>
        </Card>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Club directory</CardTitle>
        </CardHeader>
        <CardContent className="p-4"><ClubsTable rows={items} /></CardContent>
      </Card>
    </div>
  );
}
