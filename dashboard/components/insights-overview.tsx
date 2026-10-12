import Link from "next/link";
import { Award, Building2, CalendarRange, Globe2, Layers, Sparkles, Trophy, Users } from "lucide-react";

import { CompetitionMixDonut } from "@/components/competition-mix-donut";
import { KpiCard } from "@/components/kpi-card";
import { NationalitiesBarChart } from "@/components/nationalities-bar-chart";
import { MedalRibbonIcon, ScoreSheetIcon, SkatingBladeIcon } from "@/components/skating-icons";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { YearRangeFilter } from "@/components/year-range-filter";
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
      <section className="ice-grid ice-sheen overflow-hidden rounded-[2rem] border border-slate-900/80 bg-[linear-gradient(145deg,_rgb(2_6_23)_0%,_rgb(8_47_73)_42%,_rgb(15_23_42)_100%)] shadow-[0_30px_90px_rgba(2,6,23,0.26)]">
        <div className="grid gap-8 px-6 py-10 md:px-10 md:py-12 lg:grid-cols-[1.15fr_0.85fr]">
          <div>
            <div className="flex flex-wrap items-center gap-3">
              <Badge variant="secondary" className="rounded-full border border-sky-400/20 bg-slate-950/85 px-3 py-1 text-sky-200">
                Club Stats
              </Badge>
              <span className="inline-flex items-center gap-2 rounded-full border border-slate-700/80 bg-slate-950/70 px-3 py-1 text-xs text-slate-300">
                <ScoreSheetIcon className="h-3.5 w-3.5 text-sky-300" />
                cross-event performance view
              </span>
            </div>
            <h1 className="mt-4 max-w-3xl text-4xl font-semibold tracking-tight text-balance text-white md:text-5xl">
              {title}
            </h1>
            <p className="mt-4 max-w-2xl text-base leading-7 text-sky-100/82 md:text-lg">
              {subtitle} Coverage window: {overview.first_event_date} to {overview.latest_event_date}. Current slice: {periodLabel}.
            </p>
            <div className="mt-5 flex flex-wrap gap-2 text-xs text-slate-200">
              <span className="inline-flex items-center gap-2 rounded-full border border-slate-700/80 bg-slate-950/72 px-3 py-1"><SkatingBladeIcon className="h-3.5 w-3.5 text-sky-300" /> skater and club stats</span>
              <span className="inline-flex items-center gap-2 rounded-full border border-slate-700/80 bg-slate-950/72 px-3 py-1"><Globe2 className="h-3.5 w-3.5 text-sky-300" /> country coverage</span>
              <span className="inline-flex items-center gap-2 rounded-full border border-slate-700/80 bg-slate-950/72 px-3 py-1"><MedalRibbonIcon className="h-3.5 w-3.5 text-sky-300" /> event mix and volume</span>
            </div>
          </div>

          <div className="grid gap-4">
            <Card className="border-slate-700/80 bg-slate-950/72 text-sky-50 shadow-sm backdrop-blur-sm">
              <CardHeader className="flex flex-row items-center justify-between">
                <CardTitle className="text-sm font-medium text-sky-100">Filter the stats window</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <p className="text-sm leading-6 text-slate-300">
                  Use the year range to shift from all-time coverage to a narrower season view without leaving the public site layer.
                </p>
                <YearRangeFilter minYear={2013} maxYear={2026} />
              </CardContent>
            </Card>
            <Card className="border-slate-700/80 bg-slate-950/80 text-sky-50 shadow-sm backdrop-blur-sm">
              <CardHeader>
                <CardTitle className="text-sm font-medium text-sky-100">What this page should do</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3 text-sm leading-6 text-slate-300">
                <p>Keep the same club-site tone as the homepage while exposing the deeper performance layer.</p>
                <p>Use these cards and charts to move into events, clubs, countries, and methodology pages.</p>
              </CardContent>
            </Card>
          </div>
        </div>
      </section>

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
        <Card id="top-nations" className="border-slate-800 bg-slate-950">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base text-slate-100">Top nations by segment scores</CardTitle>
            <Link href="/nationalities" className="text-sm font-medium text-sky-300 hover:text-sky-200">Open countries</Link>
          </CardHeader>
          <CardContent>
            <NationalitiesBarChart
              rows={nationalities}
              take={12}
              hrefBase="/nationalities/"
            />
          </CardContent>
        </Card>

        <Card id="event-mix" className="border-slate-800 bg-slate-950">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base text-slate-100">Event mix by scope</CardTitle>
            <Link href="/events" className="text-sm font-medium text-sky-300 hover:text-sky-200">Open events</Link>
          </CardHeader>
          <CardContent>
            <CompetitionMixDonut rows={competition_mix} />
          </CardContent>
        </Card>
      </section>

      <section>
        <Card id="active-clubs" className="border-slate-800 bg-slate-950">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base text-slate-100">Active clubs</CardTitle>
            <Link href="/clubs" className="text-sm font-medium text-sky-300 hover:text-sky-200">Open clubs</Link>
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
