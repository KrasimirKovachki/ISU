import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowRight, Globe2, MapPin, Trophy } from "lucide-react";

import { PageHeader } from "@/components/page-header";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { getEvent } from "@/lib/api";
import { formatDate, number, nationDisplay, points } from "@/lib/format";

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
        eyebrow="Competition Page"
        title={event.event_name ?? "(unnamed event)"}
        subtitle={
          <>
            {formatDate(event.event_start_date)} · {event.location ?? "?"}{event.venue ? ` · ${event.venue}` : ""}
          </>
        }
        meta={[
          `${number(categories.length)} categories`,
          `${number(event.results.length)} entries`,
          event.competition_level ?? "level pending",
          event.competition_stream ?? "stream pending",
        ]}
        right={
          event.root_url ? (
            <a
              href={event.root_url}
              target="_blank"
              rel="noreferrer"
              className="inline-flex items-center gap-2 rounded-full border bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50"
            >
              Original source <ArrowRight className="h-4 w-4" />
            </a>
          ) : null
        }
      />

      <section className="grid gap-4 lg:grid-cols-[0.9fr_1.1fr]">
        <Card className="bg-slate-950 text-slate-50">
          <CardHeader>
            <CardTitle className="text-base text-white">Event overview</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-6 text-slate-300">
            <p>This page should work as a public results page first: what the event is, where it happened, how many categories it contains, and how visitors can navigate the result tables.</p>
            <p>Official source links matter here because event pages are the strongest bridge between public browsing and imported evidence.</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Event facts</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-4 sm:grid-cols-3">
            <div>
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500">When</p>
              <p className="mt-1 text-base font-semibold">{formatDate(event.event_start_date)}</p>
            </div>
            <div>
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Where</p>
              <p className="mt-1 text-base font-semibold"><MapPin className="mr-2 inline h-4 w-4 text-sky-700" />{event.location ?? "—"}</p>
            </div>
            <div>
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Format</p>
              <p className="mt-1 text-base font-semibold"><Globe2 className="mr-2 inline h-4 w-4 text-sky-700" />{event.event_type ?? event.competition_level ?? "—"}</p>
            </div>
          </CardContent>
        </Card>
      </section>

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

      <Card>
        <CardHeader>
          <CardTitle className="text-base">How to read this event page</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 md:grid-cols-3 text-sm leading-6 text-slate-600">
          <div>
            <p className="font-medium text-slate-900"><Trophy className="mr-2 inline h-4 w-4 text-amber-600" />Placements</p>
            <p className="mt-2">Rows are grouped by category and ordered by final place when available.</p>
          </div>
          <div>
            <p className="font-medium text-slate-900">Entity links</p>
            <p className="mt-2">Skater and club links should turn event browsing into deeper profile exploration.</p>
          </div>
          <div>
            <p className="font-medium text-slate-900">Source traceability</p>
            <p className="mt-2">The event source link should remain visible because the imported page is derived from official published results.</p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
