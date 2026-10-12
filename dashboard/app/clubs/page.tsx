export const dynamic = "force-dynamic";
import Link from "next/link";
import { ArrowRight, Building2, Trophy, Users } from "lucide-react";

import { KpiCard } from "@/components/kpi-card";
import { PageHeader } from "@/components/page-header";
import { ClubsTable } from "@/components/tables/clubs-table";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { listClubs } from "@/lib/api";
import { formatDate, nationDisplay, number, points } from "@/lib/format";
import { buildMetadata } from "@/lib/site";

export const revalidate = 30;
export const metadata = buildMetadata({
  title: "Clubs",
  description: "Browse public club pages with skaters, activity, best scores, and competition presence.",
  path: "/clubs",
});

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
        right={
          <Link
            href="/insights#active-clubs"
            className="surface-action inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-medium"
          >
            See club insights <ArrowRight className="h-4 w-4" />
          </Link>
        }
        className="ice-sheen ice-grid border-slate-900/80 bg-[linear-gradient(135deg,_rgb(2_6_23)_0%,_rgb(8_47_73)_48%,_rgb(15_23_42)_100%)] shadow-[0_30px_80px_rgba(2,6,23,0.24)]"
        titleClassName="text-white"
        subtitleClassName="text-sky-100/85"
        eyebrowClassName="border-slate-700/80 bg-slate-950/75 text-sky-200"
        metaClassName="border-slate-700/80 bg-slate-950/75 text-slate-300"
      />

      <section className="grid gap-4 lg:grid-cols-[0.92fr_1.08fr]">
        <Card className="border-slate-800 bg-slate-950 text-slate-50">
          <CardHeader>
            <CardTitle className="text-base text-white">Public club browse surface</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-6 text-slate-300">
            <p>Club pages are strong public entry points because families and coaches often follow a club before they follow an individual skater.</p>
            <p>This index should surface the most active clubs clearly and make it easy to move from club to roster pages.</p>
          </CardContent>
        </Card>
        <div className="grid gap-4 sm:grid-cols-3">
          <KpiCard label="Clubs" value={items.length} icon={Building2} href="/clubs#directory" ctaLabel="Browse clubs" />
          <KpiCard
            label="Top Visible Club"
            value={featured?.club_name ?? "—"}
            icon={Users}
            hint={featured ? nationDisplay(featured.country_code) : undefined}
            href={featured ? `/clubs/${encodeURIComponent(featured.club_name)}` : "/clubs"}
            ctaLabel="Open club page"
          />
          <KpiCard
            label="Best Visible TSS"
            value={featured ? points(featured.best_tss) : "—"}
            icon={Trophy}
            hint={featured ? `latest ${formatDate(featured.latest_seen_date)}` : undefined}
            href={featured ? `/clubs/${encodeURIComponent(featured.club_name)}` : "/clubs"}
            ctaLabel="Open featured club"
          />
        </div>
      </section>

      {featured ? (
        <Card className="border-slate-800 bg-slate-950">
          <CardHeader>
            <CardTitle className="text-base text-slate-100">Featured club</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
            <div>
              <p className="text-lg font-semibold text-slate-100">{featured.club_name}</p>
              <p className="mt-1 text-sm text-slate-400">
                {nationDisplay(featured.country_code)} · {number(featured.skaters)} skaters · {number(featured.events)} events · latest {formatDate(featured.latest_seen_date)}
              </p>
            </div>
            <Link href={`/clubs/${encodeURIComponent(featured.club_name)}`} className="inline-flex items-center gap-2 text-sm font-medium text-sky-300 hover:text-sky-200">
              Open club page <ArrowRight className="h-4 w-4" />
            </Link>
          </CardContent>
        </Card>
      ) : null}

      <Card id="directory" className="border-slate-800 bg-slate-950 scroll-mt-28">
        <CardHeader>
          <CardTitle className="text-base text-slate-100">Club directory</CardTitle>
        </CardHeader>
        <CardContent className="p-4"><ClubsTable rows={items} /></CardContent>
      </Card>
    </div>
  );
}
