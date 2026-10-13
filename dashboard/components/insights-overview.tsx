import Link from "next/link";
import { Award, Building2, CalendarRange, Globe2, Layers, Sparkles, Trophy, Users } from "lucide-react";

import { CompetitionMixDonut } from "@/components/competition-mix-donut";
import { PageHeader } from "@/components/page-header";
import { KpiCard } from "@/components/kpi-card";
import { NationalitiesBarChart } from "@/components/nationalities-bar-chart";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { YearRangeFilter } from "@/components/year-range-filter";
import { formatDate } from "@/lib/format";
import { getDashboard } from "@/lib/api";

export async function InsightsOverview({
  searchParams,
  title = "Insights",
  subtitle = "Cross-event analytics powered by official competition results.",
}: {
  searchParams: Promise<{ from_year?: string; to_year?: string }>;
  title?: string;
  subtitle?: string;
}) {
  const sp = await searchParams;
  const from = sp.from_year ? Number(sp.from_year) : undefined;
  const to = sp.to_year ? Number(sp.to_year) : undefined;
  const data = await getDashboard({ from, to });
  const { overview, competition_mix, clubs, nationalities } = data;

  const periodLabel = data.period
    ? `${data.period.from_year}–${data.period.to_year}`
    : "all-time view";

  return (
    <div className="space-y-8">
      {/* Edited 2026-10-12 by Claude (AI assistant): redesign — standard header card with the year filter */}
      <PageHeader
        eyebrow={`Stats · ${periodLabel}`}
        title={title}
        subtitle={<>{subtitle} Results from {formatDate(overview.first_event_date)} to {formatDate(overview.latest_event_date)}.</>}
        right={<YearRangeFilter minYear={2013} maxYear={2026} />}
      />

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard label="Events" value={overview.events} icon={CalendarRange} href="/events" ctaLabel="Open events" />
        <KpiCard label="Skaters" value={overview.skaters} icon={Users} href="/skaters" ctaLabel="Open skaters" />
        <KpiCard label="Clubs" value={overview.scoring_clubs} icon={Building2} hint={`${overview.registered_clubs} registered`} href="/clubs" ctaLabel="Open clubs" />
        <KpiCard label="Segment scores" value={overview.segment_scores} icon={Layers} hint={`${overview.element_scores.toLocaleString()} elements`} href="/about" ctaLabel="Read methodology" />
      </section>

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard label="International" value={overview.international_events} icon={Globe2} href="/events" ctaLabel="Browse events" />
        <KpiCard label="Federation" value={overview.federation_events} icon={Trophy} href="/events" ctaLabel="Browse events" />
        <KpiCard label="Club / Open" value={overview.club_or_open_events} icon={Award} href="/events" ctaLabel="Browse events" />
        <KpiCard label="Mlad Figurist" value={overview.development_skaters} icon={Sparkles} hint="Development-only skaters indexed" href="/skaters" ctaLabel="Open skaters" />
      </section>

      <section className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Card id="top-nations" className="border-border bg-card">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base text-foreground">Top nations by segment scores</CardTitle>
            <Link href="/nationalities" className="text-sm font-medium text-brand hover:text-brand">Open countries</Link>
          </CardHeader>
          <CardContent>
            <NationalitiesBarChart
              rows={nationalities}
              take={12}
              hrefBase="/nationalities/"
            />
          </CardContent>
        </Card>

        <Card id="event-mix" className="border-border bg-card">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base text-foreground">Event mix by scope</CardTitle>
            <Link href="/events" className="text-sm font-medium text-brand hover:text-brand">Open events</Link>
          </CardHeader>
          <CardContent>
            <CompetitionMixDonut rows={competition_mix} />
          </CardContent>
        </Card>
      </section>

      <section>
        <Card id="active-clubs" className="border-border bg-card">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base text-foreground">Active clubs</CardTitle>
            <Link href="/clubs" className="text-sm font-medium text-brand hover:text-brand">Open clubs</Link>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Club</TableHead>
                  <TableHead>Country</TableHead>
                  <TableHead className="text-right">Skaters</TableHead>
                  <TableHead className="text-right">Events</TableHead>
                  <TableHead className="text-right">Scores</TableHead>
                  <TableHead className="text-right">Best TSS</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {clubs.slice(0, 12).map((club) => (
                  <TableRow key={club.club_name}>
                    <TableCell className="font-medium">
                      <Link href={`/clubs/${encodeURIComponent(club.club_name)}`} className="hover:underline">
                        {club.club_name}
                      </Link>
                    </TableCell>
                    <TableCell className="text-muted-foreground">{club.country_display ?? club.country_code ?? "—"}</TableCell>
                    <TableCell className="text-right tabular-nums">{club.skaters.toLocaleString()}</TableCell>
                    <TableCell className="text-right tabular-nums">{club.events.toLocaleString()}</TableCell>
                    <TableCell className="text-right tabular-nums">{club.segment_scores.toLocaleString()}</TableCell>
                    <TableCell className="text-right tabular-nums">{club.best_tss?.toFixed(2) ?? "—"}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </section>
    </div>
  );
}
