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
import { getClub } from "@/lib/api";
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

      <section className="grid gap-4 lg:grid-cols-[0.92fr_1.08fr]">
        <Card className="bg-slate-950 text-slate-50">
          <CardHeader>
            <CardTitle className="text-base text-white">Club snapshot</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-6 text-slate-300">
            <p>Club pages should become shareable public summaries: who competes for the club, how active it is, and what level of events it appears in.</p>
            <p>They are a natural bridge between public skater profiles and competition pages, especially when families or coaches follow a single club.</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Club facts</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-4 sm:grid-cols-3">
            <div>
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Country</p>
              <p className="mt-1 text-base font-semibold"><Globe2 className="mr-2 inline h-4 w-4 text-sky-700" />{nationDisplay(club.country_code)}</p>
            </div>
            <div>
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Activity</p>
              <p className="mt-1 text-base font-semibold"><Building2 className="mr-2 inline h-4 w-4 text-sky-700" />{number(club.events)} competitions</p>
            </div>
            <div>
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Roster</p>
              <p className="mt-1 text-base font-semibold"><Users className="mr-2 inline h-4 w-4 text-sky-700" />{number(club.skater_rows?.length ?? 0)} profiled skaters</p>
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

      <div className="flex justify-end">
        <Link
          href="/clubs"
          className="inline-flex items-center gap-2 rounded-full border bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50"
        >
          Back to clubs <ArrowRight className="h-4 w-4" />
        </Link>
      </div>
    </div>
  );
}
