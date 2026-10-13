export const dynamic = "force-dynamic";
import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowRight, Building2, Globe2, Users } from "lucide-react";

import { KpiCard } from "@/components/kpi-card";
import { PageHeader } from "@/components/page-header";
import { IceRinkIcon } from "@/components/skating-icons";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { getNationality, getClub, listClubs } from "@/lib/api";
import { formatDate, number, nationDisplay, points } from "@/lib/format";
import { buildMetadata } from "@/lib/site";

export const revalidate = 60;

const STATUS_LABEL: Record<string, string> = {
  national_competitor: "National",
  recreational: "Recreational",
  mixed: "Mixed",
  development: "Mlad Figurist",
};

export async function generateMetadata({ params }: { params: Promise<{ name: string }> }): Promise<Metadata> {
  const { name } = await params;
  const decoded = decodeURIComponent(name);
  try {
    const club = await getClub(decoded);
    return buildMetadata({
      title: `${club.club_name}`,
      description: `Public club page for ${club.club_name}, with skaters, competition activity, and best visible scores.`,
      path: `/clubs/${encodeURIComponent(club.club_name)}`,
    });
  } catch {
    return buildMetadata({ title: "Club Profile", path: `/clubs/${encodeURIComponent(decoded)}` });
  }
}

export default async function Page({ params }: { params: Promise<{ name: string }> }) {
  const { name } = await params;
  const decoded = decodeURIComponent(name);
  let club;
  try {
    club = await getClub(decoded);
  } catch {
    notFound();
  }

  const [nation, allClubs] = await Promise.all([
    club.country_code ? getNationality(club.country_code).catch(() => null) : Promise.resolve(null),
    listClubs().catch(() => ({ items: [] })),
  ]);

  const featuredSkaters = [...(club.skater_rows ?? [])]
    .sort((a, b) => (b.best_tss ?? 0) - (a.best_tss ?? 0))
    .slice(0, 3);

  const peerClubs = allClubs.items
    .filter((row) => row.club_name !== club.club_name && row.country_code === club.country_code)
    .sort((a, b) => (b.best_tss ?? 0) - (a.best_tss ?? 0))
    .slice(0, 3);

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Club Profile"
        icon={<IceRinkIcon className="h-5 w-5" />}
        title={club.club_name}
        subtitle={
          <>
            {nationDisplay(club.country_code)} · {formatDate(club.first_seen_date)} → {formatDate(club.latest_seen_date)}
          </>
        }
        meta={[
          `${number(club.skaters)} skaters`,
          `${number(club.events)} events`,
          `${number(club.segment_scores)} scored segments`,
        ]}
        right={
          club.country_code ? (
            <Link
              href={`/nationalities/${encodeURIComponent(club.country_code)}`}
              className="surface-action inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-medium transition"
            >
              Country page <ArrowRight className="h-4 w-4" />
            </Link>
          ) : null
        }
      />

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard label="Skaters" value={club.skaters} />
        <KpiCard label="Events" value={club.events} />
        <KpiCard label="Segment scores" value={club.segment_scores} />
        <KpiCard label="Best TSS" value={points(club.best_tss)} hint={`PCS ${points(club.best_pcs)} · TES ${points(club.best_tes)}`} />
      </section>

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard label="International" value={club.international_events} />
        <KpiCard label="Federation" value={club.federation_events} />
        <KpiCard label="Club / Open" value={club.club_or_open_events} />
        <KpiCard label="Mlad Figurist" value={club.development_skaters} hint={`${club.converted_development_skaters} converted`} />
      </section>

      <section className="grid gap-4">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Club facts</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-4 sm:grid-cols-3">
            <div>
              <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground">Country</p>
              <p className="mt-1 text-base font-semibold"><Globe2 className="mr-2 inline h-4 w-4 text-brand" />{nationDisplay(club.country_code)}</p>
            </div>
            <div>
              <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground">Activity</p>
              <p className="mt-1 text-base font-semibold"><Building2 className="mr-2 inline h-4 w-4 text-brand" />{number(club.events)} competitions</p>
            </div>
            <div>
              <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground">Roster</p>
              <p className="mt-1 text-base font-semibold"><Users className="mr-2 inline h-4 w-4 text-brand" />{number(club.skater_rows?.length ?? 0)} profiled skaters</p>
            </div>
          </CardContent>
        </Card>
      </section>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Skaters in this club ({club.skater_rows?.length ?? 0})</CardTitle>
          <p className="text-sm text-muted-foreground">
            Each row links into a public skater page and keeps a visible competitor-status label for context.
          </p>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Skater</TableHead>
                <TableHead>Nation</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Comps</TableHead>
                <TableHead className="text-right">Best TSS</TableHead>
                <TableHead>Latest</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(club.skater_rows ?? []).map((s) => (
                <TableRow key={s.skater_key}>
                  <TableCell className="font-medium">
                    <Link href={`/skaters/${s.skater_key}`} className="hover:underline">{s.skater_name}</Link>
                  </TableCell>
                  <TableCell>{nationDisplay(s.nation_code)}</TableCell>
                  <TableCell><Badge variant="outline">{STATUS_LABEL[s.competitor_status] ?? s.competitor_status}</Badge></TableCell>
                  <TableCell className="text-right tabular-nums">{s.competitions}</TableCell>
                  <TableCell className="text-right tabular-nums">{points(s.best_tss)}</TableCell>
                  <TableCell className="text-muted-foreground">{formatDate(s.latest_seen_date)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <section className="grid gap-4 lg:grid-cols-[1fr_1fr]">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Who to look at next</CardTitle>
            <p className="text-sm text-muted-foreground">
              A public club page should surface the strongest visible profiles instead of expecting visitors to scan the full table first.
            </p>
          </CardHeader>
          <CardContent className="space-y-3">
            {featuredSkaters.map((row) => (
              <div key={row.skater_key} className="surface-inset rounded-2xl border p-4">
                <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                  <div className="min-w-0 flex-1">
                    <Link
                      href={`/skaters/${row.skater_key}`}
                      className="block text-base font-semibold leading-snug tracking-tight text-foreground text-balance hover:underline"
                    >
                      {row.skater_name}
                    </Link>
                    <p className="mt-1 text-sm font-medium text-muted-foreground">
                      {STATUS_LABEL[row.competitor_status] ?? row.competitor_status} · {number(row.competitions)} comps
                    </p>
                  </div>
                  <span className="shrink-0 text-sm font-semibold text-foreground">{points(row.best_tss)}</span>
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Country network</CardTitle>
            <p className="text-sm text-muted-foreground">
              Club pages work better when they connect outward to the broader national field.
            </p>
          </CardHeader>
          <CardContent className="space-y-4">
            {nation ? (
              <div className="surface-inset-accent rounded-2xl border p-4">
                <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground">Country coverage</p>
                <p className="mt-2 text-base font-semibold text-foreground">{nationDisplay(nation.nation_code)}</p>
                <p className="mt-1 text-sm text-muted-foreground">
                  {number(nation.skaters)} skaters · {number(nation.events)} events · top total {points(nation.best_tss)}
                </p>
                <Link
                  href={`/nationalities/${encodeURIComponent(nation.nation_code)}`}
                  className="mt-3 inline-flex items-center gap-2 text-sm font-medium text-brand hover:underline"
                >
                  Open country page <ArrowRight className="h-4 w-4" />
                </Link>
              </div>
            ) : null}
            <div className="space-y-3">
              <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground">Peer clubs</p>
              {peerClubs.length > 0 ? (
                peerClubs.map((row) => (
                  <div key={row.club_name} className="surface-inset rounded-2xl border p-4 flex items-start justify-between gap-3">
                    <div>
                      <Link href={`/clubs/${encodeURIComponent(row.club_name)}`} className="font-medium text-foreground hover:underline">
                        {row.club_name}
                      </Link>
                      <p className="text-sm text-muted-foreground">{number(row.skaters)} skaters · {number(row.events)} events</p>
                    </div>
                    <span className="text-sm font-medium text-foreground">{points(row.best_tss)}</span>
                  </div>
                ))
              ) : (
                <p className="text-sm text-muted-foreground">No other same-country clubs are visible in the current index.</p>
              )}
            </div>
          </CardContent>
        </Card>
      </section>

      <div className="flex justify-end">
        <Link
          href="/clubs"
          className="surface-action inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-medium transition"
        >
          Back to clubs <ArrowRight className="h-4 w-4" />
        </Link>
      </div>
    </div>
  );
}
