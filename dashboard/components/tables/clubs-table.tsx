"use client";
import Link from "next/link";

import { DataTable, type Column } from "@/components/data-table";
import { formatDate, nationDisplay, points } from "@/lib/format";
import type { ClubRow } from "@/lib/types";

type Row = ClubRow & Record<string, unknown>;

const columns: Column<Row>[] = [
  { key: "club_name", header: "Club",
    cell: (r) => <Link href={`/clubs/${encodeURIComponent(r.club_name)}`} className="font-medium hover:underline">{r.club_name}</Link>,
    defaultDirection: "asc" },
  { key: "country_code", header: "Country",
    cell: (r) => nationDisplay(r.country_code), defaultDirection: "asc" },
  { key: "skaters", header: "Skaters", className: "text-right tabular-nums" },
  { key: "events",  header: "Events",  className: "text-right tabular-nums" },
  { key: "segment_scores", header: "Scores", className: "text-right tabular-nums" },
  { key: "best_tss", header: "Best TSS", className: "text-right tabular-nums",
    cell: (r) => points(r.best_tss) },
  { key: "latest_seen_date", header: "Latest", className: "text-muted-foreground",
    cell: (r) => formatDate(r.latest_seen_date) },
];

export function ClubsTable({ rows }: { rows: ClubRow[] }) {
  return <DataTable<Row> rows={rows as Row[]} columns={columns} defaultSort={{ key: "segment_scores", direction: "desc" }} initialPageSize={50} />;
}
