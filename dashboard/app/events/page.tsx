import Link from "next/link";

import { PageHeader } from "@/components/page-header";
import { Card, CardContent } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { listEvents } from "@/lib/api";

export const revalidate = 30;

export default async function Page({ searchParams }: { searchParams: Promise<{ q?: string; from_year?: string; to_year?: string }> }) {
  const sp = await searchParams;
  const from = sp.from_year ? Number(sp.from_year) : undefined;
  const to = sp.to_year ? Number(sp.to_year) : undefined;
  const { items } = await listEvents({ q: sp.q, from_year: from, to_year: to, limit: 500 });
  return (
    <div className="space-y-6">
      <PageHeader
        title="Events"
        subtitle={`${items.length} event${items.length === 1 ? "" : "s"}${from || to ? ` (${from ?? "…"}–${to ?? "…"})` : ""}${sp.q ? ` matching "${sp.q}"` : ""}`}
      />
      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Date</TableHead>
                <TableHead>Event</TableHead>
                <TableHead>Location</TableHead>
                <TableHead>Stream</TableHead>
                <TableHead>Type</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {items.map((e) => (
                <TableRow key={e.event_key}>
                  <TableCell className="text-muted-foreground">{e.event_start_date ?? "—"}</TableCell>
                  <TableCell className="font-medium">
                    <Link href={`/events/${e.event_key}`} className="hover:underline">{e.event_name ?? "—"}</Link>
                  </TableCell>
                  <TableCell className="text-muted-foreground">{e.location ?? "—"}</TableCell>
                  <TableCell className="text-muted-foreground">{e.competition_stream ?? "—"}</TableCell>
                  <TableCell className="text-muted-foreground">{e.event_type ?? "—"}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
