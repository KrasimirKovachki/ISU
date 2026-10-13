export const dynamic = "force-dynamic";
import Link from "next/link";
import { ArrowRight, Globe2, Trophy, Users } from "lucide-react";

import { KpiCard } from "@/components/kpi-card";
import { PageHeader } from "@/components/page-header";
import { WorldCoverageMap } from "@/components/world-coverage-map";
import { NationalitiesTable } from "@/components/tables/nationalities-table";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { listNationalities } from "@/lib/api";
import { formatDate, nationDisplay, nationFlag, number, points } from "@/lib/format";
import { buildMetadata } from "@/lib/site";

export const revalidate = 30;
export const metadata = buildMetadata({
  title: "Countries",
  description: "Browse country-level figure-skating coverage, skaters, event counts, and top visible scores.",
  path: "/nationalities",
});

export default async function Page({ searchParams }: { searchParams: Promise<{ q?: string }> }) {
  const sp = await searchParams;
  const { items } = await listNationalities(sp.q);
  const featured = items[0];
  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Country Index"
        title="Nationalities"
        subtitle={`${items.length} nation${items.length === 1 ? "" : "s"}${sp.q ? ` matching "${sp.q}"` : ""}`}
        meta={[
          `${number(items.length)} visible countries`,
          sp.q ? `Filtered by "${sp.q}"` : "Sorted by segment-score volume by default",
        ]}
        right={
          <Link
            href="/insights#top-nations"
                className="inline-flex h-11 items-center gap-2 rounded-[10px] border border-input px-4 text-sm font-semibold text-foreground hover:bg-ice-2 hover:no-underline"
              >
            See country insights <ArrowRight className="h-4 w-4" />
          </Link>
        }
        titleClassName="text-foreground"
        subtitleClassName="text-brand"
      />

      <section className="grid gap-4 lg:grid-cols-2">
        <Card className="border-border bg-card text-foreground">
          <CardHeader>
            <CardTitle className="text-base text-foreground">Global footprint map</CardTitle>
            <p className="text-sm text-muted-foreground">
              Visible countries with stronger score volume highlighted first.
            </p>
          </CardHeader>
          <CardContent>
            <WorldCoverageMap rows={items} />
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-4 sm:grid-cols-3">
        <KpiCard label="Countries" value={items.length} icon={Globe2} href="/nationalities#directory" ctaLabel="Browse countries" />
        <KpiCard
          label="Top Visible Nation"
          value={featured ? `${nationFlag(featured.nation_code)} ${featured.nation_code}` : "—"}
          icon={Users}
          href={featured ? `/nationalities/${encodeURIComponent(featured.nation_code)}` : "/nationalities"}
          ctaLabel="Open country page"
        />
        <KpiCard
          label="Best Visible TSS"
          value={featured ? points(featured.best_tss) : "—"}
          icon={Trophy}
          hint={featured ? `latest ${formatDate(featured.latest_seen_date)}` : undefined}
          href={featured ? `/nationalities/${encodeURIComponent(featured.nation_code)}` : "/nationalities"}
          ctaLabel="Open featured country"
        />
      </section>

      {featured ? (
        <Card className="border-border bg-card">
          <CardHeader>
            <CardTitle className="text-base text-foreground">Featured country view</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
            <div>
              <p className="text-lg font-semibold text-foreground">{nationDisplay(featured.nation_code)}</p>
              <p className="mt-1 text-sm text-muted-foreground">
                {number(featured.skaters)} skaters · {number(featured.events)} events · {number(featured.segment_scores)} segment scores
              </p>
            </div>
            <Link href={`/nationalities/${encodeURIComponent(featured.nation_code)}`} className="inline-flex items-center gap-2 text-sm font-medium text-brand hover:text-brand">
              Open country page <ArrowRight className="h-4 w-4" />
            </Link>
          </CardContent>
        </Card>
      ) : null}

      <Card id="directory" className="border-border bg-card scroll-mt-28">
        <CardHeader>
          <CardTitle className="text-base text-foreground">Country directory</CardTitle>
        </CardHeader>
        <CardContent className="p-4"><NationalitiesTable rows={items} /></CardContent>
      </Card>
    </div>
  );
}
