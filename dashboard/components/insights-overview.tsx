import Link from "next/link";
import { Award, Building2, CalendarRange, Globe2, Layers, Sparkles, Trophy, Users } from "lucide-react";

import { CompetitionMixDonut } from "@/components/competition-mix-donut";
import { KpiCard } from "@/components/kpi-card";
import { NationalitiesBarChart } from "@/components/nationalities-bar-chart";
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
    <div className="space-y-6">
      <div className="flex items-end justify-between gap-4 flex-wrap">
        <div>
          <h1 className="text-3xl font-semibold tracking-tight">{title}</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            {subtitle} {overview.first_event_date} → {overview.latest_event_date} · {periodLabel}
          </p>
        </div>
        <YearRangeFilter minYear={2013} maxYear={2026} />
      </div>

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard label="Events" value={overview.events} icon={CalendarRange} />
        <KpiCard label="Skaters" value={overview.skaters} icon={Users} />
        <KpiCard label="Clubs" value={overview.scoring_clubs} icon={Building2} hint={`${overview.registered_clubs} registered`} />
        <KpiCard label="Segment scores" value={overview.segment_scores} icon={Layers} hint={`${overview.element_scores.toLocaleString()} elements`} />
      </section>

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard label="International" value={overview.international_events} icon={Globe2} />
        <KpiCard label="Federation" value={overview.federation_events} icon={Trophy} />
        <KpiCard label="Club / Open" value={overview.club_or_open_events} icon={Award} />
        <KpiCard label="Mlad Figurist" value={overview.development_skaters} icon={Sparkles} hint="Development-only skaters indexed" />
      </section>

      <section className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Top nations by segment scores</CardTitle>
          </CardHeader>
          <CardContent>
            <NationalitiesBarChart rows={nationalities} take={12} />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base">Event mix by scope</CardTitle>
          </CardHeader>
          <CardContent>
            <CompetitionMixDonut rows={competition_mix} />
          </CardContent>
        </Card>
      </section>

      <section>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Active clubs</CardTitle>
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
