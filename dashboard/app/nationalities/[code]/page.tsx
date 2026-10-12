export const dynamic = "force-dynamic";
import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowRight, Globe2, Medal, Users } from "lucide-react";

import { KpiCard } from "@/components/kpi-card";
import { PageHeader } from "@/components/page-header";
import { MedalRibbonIcon } from "@/components/skating-icons";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { getNationality, listClubs } from "@/lib/api";
import { formatDate, nationDisplay, nationFlag, number, points } from "@/lib/format";
import { buildMetadata } from "@/lib/site";

export const revalidate = 60;

export async function generateMetadata({ params }: { params: Promise<{ code: string }> }): Promise<Metadata> {
  const { code } = await params;
  const decoded = decodeURIComponent(code);
  try {
    const nation = await getNationality(decoded);
    return buildMetadata({
      title: `${nation.nation_code} Country Page`,
      description: `Country-level figure-skating coverage for ${nation.nation_code}, including visible skaters, events, and best scores.`,
      path: `/nationalities/${encodeURIComponent(nation.nation_code)}`,
    });
  } catch {
    return buildMetadata({ title: "Country Page", path: `/nationalities/${encodeURIComponent(decoded)}` });
  }
}

export default async function Page({ params }: { params: Promise<{ code: string }> }) {
  const { code } = await params;
  const decoded = decodeURIComponent(code);
  let nation;
  try {
    nation = await getNationality(decoded);
  } catch {
    notFound();
  }

  const clubs = await listClubs().catch(() => ({ items: [] }));
  const relatedClubs = clubs.items
    .filter((row) => row.country_code === nation.nation_code)
    .sort((a, b) => (b.best_tss ?? 0) - (a.best_tss ?? 0))
    .slice(0, 4);
  const featuredSkaters = [...(nation.skaters_list ?? [])]
    .sort((a, b) => (b.best_tss ?? 0) - (a.best_tss ?? 0))
    .slice(0, 3);

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Country Page"
        icon={<MedalRibbonIcon className="h-5 w-5" />}
        title={nationDisplay(nation.nation_code)}
        subtitle={`${formatDate(nation.first_seen_date)} → ${formatDate(nation.latest_seen_date)}`}
        meta={[
          `${number(nation.skaters)} skaters`,
          `${number(nation.events)} events`,
          `${number(nation.segment_scores)} segment scores`,
        ]}
        titleClassName="text-foreground"
        subtitleClassName="text-brand"
      />

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard label="Skaters" value={nation.skaters} />
        <KpiCard label="Events" value={nation.events} />
        <KpiCard label="Segment scores" value={nation.segment_scores} />
        <KpiCard label="Best TSS" value={points(nation.best_tss)} hint={`PCS ${points(nation.best_pcs)}`} />
      </section>

      <section className="grid gap-4">
        <div className="grid gap-4 sm:grid-cols-3">
          <Card className="border-border bg-card">
            <CardContent className="p-4">
              <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground">Country</p>
              <p className="mt-2 text-base font-semibold text-foreground"><Globe2 className="mr-2 inline h-4 w-4 text-brand" />{nationFlag(nation.nation_code)} {nation.nation_code}</p>
            </CardContent>
          </Card>
          <Card className="border-border bg-card">
            <CardContent className="p-4">
              <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground">Visible skaters</p>
              <p className="mt-2 text-base font-semibold text-foreground"><Users className="mr-2 inline h-4 w-4 text-brand" />{number(nation.skaters_list?.length ?? 0)}</p>
            </CardContent>
          </Card>
          <Card className="border-border bg-card">
            <CardContent className="p-4">
              <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground">Top total</p>
              <p className="mt-2 text-base font-semibold text-foreground"><Medal className="mr-2 inline h-4 w-4 text-gold" />{points(nation.best_tss)}</p>
            </CardContent>
          </Card>
        </div>
      </section>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle className="text-base text-foreground">Visible skaters ({nation.skaters_list?.length ?? 0})</CardTitle>
          <p className="text-sm text-muted-foreground">
            Public skater profiles linked from this country page, ordered by the current backend list.
          </p>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Skater</TableHead>
                <TableHead>Club</TableHead>
                <TableHead className="text-right">Comps</TableHead>
                <TableHead className="text-right">Best TSS</TableHead>
                <TableHead>Latest</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(nation.skaters_list ?? []).slice(0, 200).map((s) => (
                <TableRow key={s.skater_key}>
                  <TableCell className="font-medium">
                    <Link href={`/skaters/${s.skater_key}`} className="hover:underline">{s.skater_name}</Link>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {s.club_name ? (
                      <Link href={`/clubs/${encodeURIComponent(s.club_name)}`} className="hover:underline">{s.club_name}</Link>
                    ) : (
                      "—"
                    )}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{s.competitions}</TableCell>
                  <TableCell className="text-right tabular-nums">{points(s.best_tss)}</TableCell>
                  <TableCell className="text-muted-foreground">{formatDate(s.latest_seen_date)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <section className="grid gap-4 lg:grid-cols-[1.05fr_0.95fr]">
        <Card className="border-border bg-card">
          <CardHeader>
            <CardTitle className="text-base text-foreground">Featured visible profiles</CardTitle>
            <p className="text-sm text-muted-foreground">
              Country pages should surface a few representative profiles instead of acting only as a raw directory.
            </p>
          </CardHeader>
          <CardContent className="space-y-3">
            {featuredSkaters.map((row) => (
              <div key={row.skater_key} className="surface-inset rounded-2xl border p-4 flex items-start justify-between gap-3">
                <div>
                  <Link href={`/skaters/${row.skater_key}`} className="font-medium text-foreground hover:underline">
                    {row.skater_name}
                  </Link>
                  <p className="text-sm text-muted-foreground">
                    {row.club_name ? row.club_name : "Club pending"} · {number(row.competitions)} comps
                  </p>
                </div>
                <span className="text-sm font-medium text-foreground">{points(row.best_tss)}</span>
              </div>
            ))}
          </CardContent>
        </Card>
        <Card className="border-border bg-card">
          <CardHeader>
            <CardTitle className="text-base text-foreground">Active clubs in this country</CardTitle>
            <p className="text-sm text-muted-foreground">
              This adds a club-layer entry point so country pages can branch into both people and organizations.
            </p>
          </CardHeader>
          <CardContent className="space-y-3">
            {relatedClubs.length > 0 ? (
              relatedClubs.map((row) => (
                <div key={row.club_name} className="surface-inset rounded-2xl border p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <Link href={`/clubs/${encodeURIComponent(row.club_name)}`} className="font-medium text-foreground hover:underline">
                        {row.club_name}
                      </Link>
                      <p className="text-sm text-muted-foreground">{number(row.skaters)} skaters · {number(row.events)} events</p>
                    </div>
                    <span className="text-sm font-medium text-foreground">{points(row.best_tss)}</span>
                  </div>
                </div>
              ))
            ) : (
              <p className="text-sm text-muted-foreground">No club coverage is currently visible for this country code.</p>
            )}
          </CardContent>
        </Card>
      </section>

      <div className="flex justify-end">
        <Link
          href="/nationalities"
          className="surface-action inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-medium transition"
        >
          Back to countries <ArrowRight className="h-4 w-4" />
        </Link>
      </div>
    </div>
  );
}
