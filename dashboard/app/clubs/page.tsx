import Link from "next/link";

import { PageHeader } from "@/components/page-header";
import { Card, CardContent } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { listClubs } from "@/lib/api";
import { nationDisplay, points } from "@/lib/format";

export const revalidate = 30;

export default async function Page({ searchParams }: { searchParams: Promise<{ q?: string }> }) {
  const sp = await searchParams;
  const { items } = await listClubs(sp.q);
  return (
    <div className="space-y-6">
      <PageHeader
        title="Clubs"
        subtitle={`${items.length} club${items.length === 1 ? "" : "s"} indexed${sp.q ? ` matching "${sp.q}"` : ""}`}
      />
      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Club</TableHead>
                <TableHead>Country</TableHead>
                <TableHead className="text-right">Skaters</TableHead>
                <TableHead className="text-right">Events</TableHead>
                <TableHead className="text-right">Scores</TableHead>
                <TableHead className="text-right">Best TSS</TableHead>
                <TableHead>Latest</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {items.map((c) => (
                <TableRow key={c.club_name}>
                  <TableCell className="font-medium">
                    <Link href={`/clubs/${encodeURIComponent(c.club_name)}`} className="hover:underline">{c.club_name}</Link>
                  </TableCell>
                  <TableCell>{nationDisplay(c.country_code)}</TableCell>
                  <TableCell className="text-right tabular-nums">{c.skaters.toLocaleString()}</TableCell>
                  <TableCell className="text-right tabular-nums">{c.events.toLocaleString()}</TableCell>
                  <TableCell className="text-right tabular-nums">{c.segment_scores.toLocaleString()}</TableCell>
                  <TableCell className="text-right tabular-nums">{points(c.best_tss)}</TableCell>
                  <TableCell className="text-muted-foreground">{c.latest_seen_date ?? "—"}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
