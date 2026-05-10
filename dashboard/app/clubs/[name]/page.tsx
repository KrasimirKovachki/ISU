import Link from "next/link";
import { notFound } from "next/navigation";

import { KpiCard } from "@/components/kpi-card";
import { PageHeader } from "@/components/page-header";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { getClub } from "@/lib/api";
import { nationDisplay, points } from "@/lib/format";

export const revalidate = 60;

const STATUS_LABEL: Record<string, string> = {
  national_competitor: "National",
  recreational: "Recreational",
  mixed: "Mixed",
  development: "Mlad Figurist",
};

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
        title={club.club_name}
        subtitle={
          <>
            {nationDisplay(club.country_code)} · {club.first_seen_date ?? "?"} → {club.latest_seen_date ?? "?"}
          </>
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

      <Card>
        <CardHeader><CardTitle className="text-base">Skaters in this club ({club.skater_rows?.length ?? 0})</CardTitle></CardHeader>
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
                  <TableCell className="text-muted-foreground">{s.latest_seen_date ?? "—"}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
