import Link from "next/link";
import { notFound } from "next/navigation";

import { PageHeader } from "@/components/page-header";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { getEvent } from "@/lib/api";
import { nationDisplay, points } from "@/lib/format";

export const revalidate = 60;

export default async function Page({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const key = Number(id);
  if (!Number.isFinite(key)) notFound();

  let event;
  try {
    event = await getEvent(key);
  } catch {
    notFound();
  }

  // Group results by category for cleaner navigation.
  const grouped = new Map<string, typeof event.results>();
  for (const r of event.results) {
    const k = r.category_name ?? "(no category)";
    if (!grouped.has(k)) grouped.set(k, []);
    grouped.get(k)!.push(r);
  }
  const categories = [...grouped.entries()].sort(([a], [b]) => a.localeCompare(b));

  return (
    <div className="space-y-6">
      <PageHeader
        title={event.event_name ?? "(unnamed event)"}
        subtitle={
          <>
            {event.event_start_date ?? "?"} · {event.location ?? "?"}{event.venue ? ` · ${event.venue}` : ""}
            {event.root_url ? <> · <a href={event.root_url} target="_blank" rel="noreferrer" className="hover:underline">source</a></> : null}
          </>
        }
      />
      <p className="text-sm text-muted-foreground">
        {categories.length} categor{categories.length === 1 ? "y" : "ies"} · {event.results.length} entries
      </p>

      {categories.map(([category, rows]) => (
        <Card key={category}>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <span>{category}</span>
              <span className="text-xs font-normal text-muted-foreground">· {rows.length} skater{rows.length === 1 ? "" : "s"}</span>
            </CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-12 text-right">#</TableHead>
                  <TableHead>Skater</TableHead>
                  <TableHead>Nation</TableHead>
                  <TableHead>Club</TableHead>
                  <TableHead className="text-right">Points</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {[...rows]
                  .sort((a, b) => (a.final_place ?? 999) - (b.final_place ?? 999))
                  .map((r) => (
                    <TableRow key={r.competition_result_key}>
                      <TableCell className="text-right tabular-nums text-muted-foreground">{r.final_place ?? "—"}</TableCell>
                      <TableCell className="font-medium">
                        {r.skater_key ? (
                          <Link href={`/skaters/${r.skater_key}`} className="hover:underline">{r.skater_name}</Link>
                        ) : (
                          r.skater_name
                        )}
                      </TableCell>
                      <TableCell>{nationDisplay(r.nation_code)}</TableCell>
                      <TableCell className="text-muted-foreground">
                        {r.club_name ? (
                          <Link href={`/clubs/${encodeURIComponent(r.club_name)}`} className="hover:underline">{r.club_name}</Link>
                        ) : (
                          "—"
                        )}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">{points(r.points)}</TableCell>
                    </TableRow>
                  ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
