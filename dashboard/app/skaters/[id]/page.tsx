/** Skater profile — KPI tiles, TSS progression chart, recent scores. */
import Link from "next/link";
import { notFound } from "next/navigation";

import { KpiCard } from "@/components/kpi-card";
import { PageHeader } from "@/components/page-header";
import { SkaterProgressionChart } from "@/components/skater-progression-chart";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { getSkater } from "@/lib/api";
import { nationDisplay, points } from "@/lib/format";

export const revalidate = 60;

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

  return (
    <div className="space-y-6">
      <PageHeader
        title={skater.skater_name}
        subtitle={
          <>
            {nationDisplay(skater.nation_code)}
            {skater.club_name ? (
              <> · <Link href={`/clubs/${encodeURIComponent(skater.club_name)}`} className="hover:underline">{skater.club_name}</Link></>
            ) : null}
            <> · seen {skater.first_seen_date ?? "?"} → {skater.latest_seen_date ?? "?"}</>
          </>
        }
      />

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard label="Best TSS" value={points(skater.best_tss)} />
        <KpiCard label="Best TES" value={points(skater.best_tes)} />
        <KpiCard label="Best PCS" value={points(skater.best_pcs)} />
        <KpiCard label="Competitions" value={skater.competitions} hint={`${skater.scored_competitions} scored`} />
      </section>

      <Card>
        <CardHeader><CardTitle className="text-base">TSS / TES / PCS over time</CardTitle></CardHeader>
        <CardContent>
          <SkaterProgressionChart scores={skater.scores} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader><CardTitle className="text-base">Scores</CardTitle></CardHeader>
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
                  <TableCell className="text-muted-foreground">{s.event_start_date ?? "—"}</TableCell>
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
