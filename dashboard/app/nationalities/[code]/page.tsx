import Link from "next/link";
import { notFound } from "next/navigation";

import { KpiCard } from "@/components/kpi-card";
import { PageHeader } from "@/components/page-header";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { getNationality } from "@/lib/api";
import { nationDisplay, points } from "@/lib/format";

export const revalidate = 60;

export default async function Page({ params }: { params: Promise<{ code: string }> }) {
  const { code } = await params;
  const decoded = decodeURIComponent(code);
  let nation;
  try {
    nation = await getNationality(decoded);
  } catch {
    notFound();
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title={nationDisplay(nation.nation_code)}
        subtitle={`${nation.first_seen_date ?? "?"} → ${nation.latest_seen_date ?? "?"}`}
      />

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard label="Skaters" value={nation.skaters} />
        <KpiCard label="Events" value={nation.events} />
        <KpiCard label="Segment scores" value={nation.segment_scores} />
        <KpiCard label="Best TSS" value={points(nation.best_tss)} hint={`PCS ${points(nation.best_pcs)}`} />
      </section>

      <Card>
        <CardHeader><CardTitle className="text-base">Top skaters ({nation.skaters_list?.length ?? 0})</CardTitle></CardHeader>
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
