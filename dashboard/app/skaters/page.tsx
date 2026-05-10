/** Skaters list with search via ?q=. Server component — q comes from
 * the URL, page re-renders on search change.
 */
import Link from "next/link";

import { PageHeader } from "@/components/page-header";
import { Card, CardContent } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { searchSkaters } from "@/lib/api";
import { nationDisplay, points } from "@/lib/format";

export const revalidate = 30;

export default async function Page({ searchParams }: { searchParams: Promise<{ q?: string }> }) {
  const sp = await searchParams;
  const q = sp.q?.trim();
  const { items } = await searchSkaters(q, 200);

  return (
    <div className="space-y-6">
      <PageHeader
        title="Skaters"
        subtitle={
          q
            ? `${items.length} match${items.length === 1 ? "" : "es"} for "${q}"`
            : `Top ${items.length} by recent activity. Use the search box above to filter.`
        }
      />
      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Skater</TableHead>
                <TableHead>Nation</TableHead>
                <TableHead>Club</TableHead>
                <TableHead className="text-right">Competitions</TableHead>
                <TableHead className="text-right">Best TSS</TableHead>
                <TableHead>Latest</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {items.map((s) => (
                <TableRow key={s.skater_key}>
                  <TableCell className="font-medium">
                    <Link href={`/skaters/${s.skater_key}`} className="hover:underline">
                      {s.skater_name}
                    </Link>
                  </TableCell>
                  <TableCell>{nationDisplay(s.nation_code)}</TableCell>
                  <TableCell className="text-muted-foreground">
                    {s.club_name ? (
                      <Link href={`/clubs/${encodeURIComponent(s.club_name)}`} className="hover:underline">
                        {s.club_name}
                      </Link>
                    ) : (
                      "—"
                    )}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{s.competitions}</TableCell>
                  <TableCell className="text-right tabular-nums">{points(s.best_tss)}</TableCell>
                  <TableCell className="text-muted-foreground">{s.latest_seen_date ?? "—"}</TableCell>
                </TableRow>
              ))}
              {items.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="text-center text-muted-foreground py-12">
                    No skaters match.
                  </TableCell>
                </TableRow>
              ) : null}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
