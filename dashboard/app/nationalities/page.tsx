import Link from "next/link";

import { PageHeader } from "@/components/page-header";
import { Card, CardContent } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { listNationalities } from "@/lib/api";
import { nationDisplay, points } from "@/lib/format";

export const revalidate = 30;

export default async function Page({ searchParams }: { searchParams: Promise<{ q?: string }> }) {
  const sp = await searchParams;
  const { items } = await listNationalities(sp.q);
  return (
    <div className="space-y-6">
      <PageHeader
        title="Nationalities"
        subtitle={`${items.length} nation${items.length === 1 ? "" : "s"}${sp.q ? ` matching "${sp.q}"` : ""}`}
      />
      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Nation</TableHead>
                <TableHead className="text-right">Skaters</TableHead>
                <TableHead className="text-right">Mlad</TableHead>
                <TableHead className="text-right">Converted</TableHead>
                <TableHead className="text-right">Events</TableHead>
                <TableHead className="text-right">Scores</TableHead>
                <TableHead className="text-right">Best TSS</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {items.map((n) => (
                <TableRow key={n.nation_code}>
                  <TableCell className="font-medium">
                    <Link href={`/nationalities/${encodeURIComponent(n.nation_code)}`} className="hover:underline">
                      {nationDisplay(n.nation_code)}
                    </Link>
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{n.skaters.toLocaleString()}</TableCell>
                  <TableCell className="text-right tabular-nums">{n.development_skaters.toLocaleString()}</TableCell>
                  <TableCell className="text-right tabular-nums">{n.converted_development_skaters.toLocaleString()}</TableCell>
                  <TableCell className="text-right tabular-nums">{n.events.toLocaleString()}</TableCell>
                  <TableCell className="text-right tabular-nums">{n.segment_scores.toLocaleString()}</TableCell>
                  <TableCell className="text-right tabular-nums">{points(n.best_tss)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
