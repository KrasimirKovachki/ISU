"use client";
import Link from "next/link";

import { DataTable, type Column } from "@/components/data-table";
import type { EventRow } from "@/lib/types";

type Row = EventRow & Record<string, unknown>;

const columns: Column<Row>[] = [
  { key: "event_start_date", header: "Date", className: "text-muted-foreground",
    cell: (r) => r.event_start_date ?? "—" },
  { key: "event_name", header: "Event",
    cell: (r) => <Link href={`/events/${r.event_key}`} className="font-medium hover:underline">{r.event_name ?? "—"}</Link>,
    defaultDirection: "asc" },
  { key: "location", header: "Location", className: "text-muted-foreground",
    cell: (r) => r.location ?? "—", defaultDirection: "asc" },
  { key: "competition_stream", header: "Stream", className: "text-muted-foreground",
    cell: (r) => r.competition_stream ?? "—", defaultDirection: "asc" },
  { key: "event_type", header: "Type", className: "text-muted-foreground",
    cell: (r) => r.event_type ?? "—", defaultDirection: "asc" },
];

export function EventsTable({ rows }: { rows: EventRow[] }) {
  return <DataTable<Row> rows={rows as Row[]} columns={columns} defaultSort={{ key: "event_start_date", direction: "desc" }} initialPageSize={50} />;
}
