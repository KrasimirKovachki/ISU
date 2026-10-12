export const dynamic = "force-dynamic";
/** Skater profile — KPI tiles, TSS progression chart, recent scores. */
import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowRight, CalendarRange, Sparkles, Trophy } from "lucide-react";

import { ComparisonRadar } from "@/components/comparison-radar";
import { KpiCard } from "@/components/kpi-card";
import { PageHeader } from "@/components/page-header";
import { SkatingBladeIcon } from "@/components/skating-icons";
import { SkaterElementStats } from "@/components/skater-element-stats";
import { SkaterProgressionChart } from "@/components/skater-progression-chart";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { getClub, getNationality, getSkater } from "@/lib/api";
import { formatDate, number, nationDisplay, points } from "@/lib/format";
import { buildMetadata } from "@/lib/site";

export const revalidate = 60;

export async function generateMetadata({ params }: { params: Promise<{ id: string }> }): Promise<Metadata> {
  const { id } = await params;
  const key = Number(id);
  if (!Number.isFinite(key)) return buildMetadata({ title: "Skater Profile", path: `/skaters/${id}` });
  try {
    const skater = await getSkater(key);
    return buildMetadata({
      title: `${skater.skater_name}`,
      description: `Public skater profile for ${skater.skater_name}${skater.club_name ? ` of ${skater.club_name}` : ""}, with competition history and score progression.`,
      path: `/skaters/${key}`,
    });
  } catch {
    return buildMetadata({ title: "Skater Profile", path: `/skaters/${id}` });
  }
}

export default async function Page({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const key = Number(id);
  if (!Number.isFinite(key)) notFound();

  let skater;
  try {
    skater = await getSkater(key);
  } catch {
    notFound();
  }

  const [club, nation] = await Promise.all([
    skater.club_name ? getClub(skater.club_name).catch(() => null) : Promise.resolve(null),
    skater.nation_code ? getNationality(skater.nation_code).catch(() => null) : Promise.resolve(null),
  ]);

  const relatedClubSkaters = (club?.skater_rows ?? [])
    .filter((row) => row.skater_key !== skater.skater_key)
    .sort((a, b) => (b.best_tss ?? 0) - (a.best_tss ?? 0))
    .slice(0, 3);

  const relatedNationSkaters = (nation?.skaters_list ?? [])
    .filter((row) => row.skater_key !== skater.skater_key)
    .sort((a, b) => (b.best_tss ?? 0) - (a.best_tss ?? 0))
    .slice(0, 3);

  const latestScores = [...skater.scores]
    .sort((a, b) => (b.event_start_date ?? "").localeCompare(a.event_start_date ?? ""))
    .slice(0, 3);

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Skater Profile"
        icon={<SkatingBladeIcon className="h-5 w-5" />}
        title={skater.skater_name}
        subtitle={
          <>
            {nationDisplay(skater.nation_code)}
            {skater.club_name ? (
              <> · <Link href={`/clubs/${encodeURIComponent(skater.club_name)}`} className="hover:underline">{skater.club_name}</Link></>
            ) : null}
            <> · seen {formatDate(skater.first_seen_date)} → {formatDate(skater.latest_seen_date)}</>
          </>
        }
        meta={[
          `${number(skater.competitions)} competitions`,
          `${number(skater.scored_competitions)} scored`,
          `Best total ${points(skater.best_score_total)}`,
        ]}
        right={
          <>
            {skater.nation_code ? (
              <Link
                href={`/nationalities/${encodeURIComponent(skater.nation_code)}`}
                className="surface-action inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-medium transition"
              >
                Country page <ArrowRight className="h-4 w-4" />
              </Link>
            ) : null}
            {skater.club_name ? (
              <Link
                href={`/clubs/${encodeURIComponent(skater.club_name)}`}
                className="surface-action inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-medium transition"
              >
                Club page <ArrowRight className="h-4 w-4" />
              </Link>
            ) : null}
          </>
        }
      />

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard label="Best TSS" value={points(skater.best_tss)} />
        <KpiCard label="Best TES" value={points(skater.best_tes)} />
        <KpiCard label="Best PCS" value={points(skater.best_pcs)} />
        <KpiCard label="Competitions" value={skater.competitions} hint={`${skater.scored_competitions} scored`} />
      </section>

      <section className="grid gap-4 lg:grid-cols-[0.95fr_1.05fr]">
        <Card className="bg-slate-950 text-slate-50">
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base text-white"><Sparkles className="h-4 w-4 text-sky-300" /> Public profile summary</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-6 text-slate-300">
            <p>
              This page should read as a public reference profile: who the skater is, where they compete, how often they appear,
              and how their scores have evolved across the indexed result set.
            </p>
            <p>
              The analytics layer stays here, but the page needs enough framing that a first-time visitor can understand the story
              without reading every table.
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">At a glance</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-4 sm:grid-cols-3">
            <div>
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Latest seen</p>
              <p className="mt-1 text-base font-semibold"><CalendarRange className="mr-2 inline h-4 w-4 text-sky-700" />{formatDate(skater.latest_seen_date)}</p>
            </div>
            <div>
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Nation</p>
              <p className="mt-1 text-base font-semibold">{nationDisplay(skater.nation_code)}</p>
            </div>
            <div>
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Top total</p>
              <p className="mt-1 text-base font-semibold"><Trophy className="mr-2 inline h-4 w-4 text-amber-600" />{points(skater.best_score_total)}</p>
            </div>
          </CardContent>
        </Card>
      </section>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card className="lg:col-span-2">
          <CardHeader><CardTitle className="text-base">TSS / TES / PCS over time</CardTitle></CardHeader>
          <CardContent>
            <SkaterProgressionChart scores={skater.scores} />
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle className="text-base">Component profile (avg)</CardTitle></CardHeader>
          <CardContent>
            <ComparisonRadar
              primaryName={skater.skater_name}
              primaryRows={skater.component_summary ?? []}
              primarySkaterKey={skater.skater_key}
            />
          </CardContent>
        </Card>
      </div>

      <SkaterElementStats elements={skater.elements} />

      <section className="grid gap-4 lg:grid-cols-[1.05fr_0.95fr]">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Continue from this profile</CardTitle>
            <p className="text-sm text-muted-foreground">
              Public profile pages should turn one skater lookup into broader exploration of club and country context.
            </p>
          </CardHeader>
          <CardContent className="grid gap-3 md:grid-cols-2">
            <div className="surface-inset rounded-2xl border p-4">
              <p className="text-xs uppercase tracking-[0.18em] text-slate-400">Same club</p>
              <div className="mt-3 space-y-3">
                {relatedClubSkaters.length > 0 ? (
                  relatedClubSkaters.map((row) => (
                    <div key={row.skater_key} className="flex items-start justify-between gap-3">
                      <div>
                        <Link href={`/skaters/${row.skater_key}`} className="font-medium text-slate-100 hover:underline">
                          {row.skater_name}
                        </Link>
                        <p className="text-sm text-slate-400">{number(row.competitions)} comps</p>
                      </div>
                      <span className="text-sm font-medium text-slate-200">{points(row.best_tss)}</span>
                    </div>
                  ))
                ) : (
                  <p className="text-sm text-slate-400">No other visible club profiles yet.</p>
                )}
              </div>
            </div>
            <div className="surface-inset-accent rounded-2xl border p-4">
              <p className="text-xs uppercase tracking-[0.18em] text-slate-400">Same country</p>
              <div className="mt-3 space-y-3">
                {relatedNationSkaters.length > 0 ? (
                  relatedNationSkaters.map((row) => (
                    <div key={row.skater_key} className="flex items-start justify-between gap-3">
                      <div>
                        <Link href={`/skaters/${row.skater_key}`} className="font-medium text-slate-100 hover:underline">
                          {row.skater_name}
                        </Link>
                        <p className="text-sm text-slate-400">{row.club_name ?? "Independent profile"}</p>
                      </div>
                      <span className="text-sm font-medium text-slate-200">{points(row.best_tss)}</span>
                    </div>
                  ))
                ) : (
                  <p className="text-sm text-slate-400">No other visible country profiles yet.</p>
                )}
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Recent scored segments</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {latestScores.map((score) => (
              <div key={score.segment_score_key} className="surface-inset rounded-2xl border p-4">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="font-medium text-slate-100">{score.segment_name ?? "Segment detail"}</p>
                    <p className="text-sm text-slate-400">
                      <Link href={`/events/${score.event_key}`} className="hover:underline">
                        {score.event_name ?? "Event page"}
                      </Link>
                      {" · "}
                      {formatDate(score.event_start_date)}
                    </p>
                  </div>
                  <span className="text-sm font-medium text-slate-200">{points(score.tss)}</span>
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
      </section>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Competition results</CardTitle>
          <p className="text-sm text-muted-foreground">
            Every scored segment linked back to the event page, with direct access to detailed score breakdowns.
          </p>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Date</TableHead>
                <TableHead>Competition</TableHead>
                <TableHead>Category</TableHead>
                <TableHead>Segment</TableHead>
                <TableHead>Source</TableHead>
                <TableHead className="text-right">Els</TableHead>
                <TableHead className="text-right">TSS</TableHead>
                <TableHead className="text-right">TES</TableHead>
                <TableHead className="text-right">PCS</TableHead>
                <TableHead></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {skater.scores.map((s) => (
                <TableRow key={s.segment_score_key}>
                  <TableCell className="text-muted-foreground">{formatDate(s.event_start_date)}</TableCell>
                  <TableCell>
                    <Link href={`/events/${s.event_key}`} className="hover:underline">{s.event_name ?? "—"}</Link>
                  </TableCell>
                  <TableCell className="text-muted-foreground">{s.category_name ?? "—"}</TableCell>
                  <TableCell className="text-muted-foreground">{s.segment_name ?? "—"}</TableCell>
                  <TableCell>
                    <Badge variant={s.score_source === "pdf_score_summary" ? "secondary" : "outline"}>
                      {s.score_source === "pdf_score_summary" ? "PDF" : "HTML"}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{s.element_count || "—"}</TableCell>
                  <TableCell className="text-right tabular-nums">{points(s.tss)}</TableCell>
                  <TableCell className="text-right tabular-nums">{points(s.tes)}</TableCell>
                  <TableCell className="text-right tabular-nums">{points(s.pcs)}</TableCell>
                  <TableCell>
                    <Link href={`/scores/${s.segment_score_key}`} className="text-xs text-primary hover:underline">detail</Link>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
