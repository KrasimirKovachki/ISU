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
                className="inline-flex h-11 items-center gap-2 rounded-[10px] border border-input px-4 text-sm font-semibold text-foreground hover:bg-ice-2 hover:no-underline"
              >
            See club insights <ArrowRight className="h-4 w-4" />
          </Link>
        }
        className="ice-sheen ice-grid border-border bg-card shadow-[0_10px_30px_rgba(11,27,43,0.10)]"
        titleClassName="text-foreground"
        subtitleClassName="text-brand"
        eyebrowClassName="border-border bg-card text-brand"
        metaClassName="border-border bg-card text-ink-2"
      />

      <section className="grid gap-4">
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
        <Card className="border-border bg-card">
          <CardHeader>
            <CardTitle className="text-base text-foreground">Featured club</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
            <div>
              <p className="text-lg font-semibold text-foreground">{featured.club_name}</p>
              <p className="mt-1 text-sm text-muted-foreground">
                {nationDisplay(featured.country_code)} · {number(featured.skaters)} skaters · {number(featured.events)} events · latest {formatDate(featured.latest_seen_date)}
              </p>
            </div>
            <Link href={`/clubs/${encodeURIComponent(featured.club_name)}`} className="inline-flex items-center gap-2 text-sm font-medium text-brand hover:text-brand">
              Open club page <ArrowRight className="h-4 w-4" />
            </Link>
          </CardContent>
        </Card>
      ) : null}

      <Card id="directory" className="border-border bg-card scroll-mt-28">
        <CardHeader>
          <CardTitle className="text-base text-foreground">Club directory</CardTitle>
        </CardHeader>
        <CardContent className="p-4"><ClubsTable rows={items} /></CardContent>
      </Card>
    </div>
  );
}
