"use client";
import Link from "next/link";

import { DataTable, type Column } from "@/components/data-table";
import { nationDisplay, points } from "@/lib/format";
import type { SkaterSearchRow } from "@/lib/types";

type Row = SkaterSearchRow & Record<string, unknown>;

const columns: Column<Row>[] = [
  {
    key: "skater_name", header: "Skater",
    cell: (r) => <Link href={`/skaters/${r.skater_key}`} className="font-medium hover:underline">{r.skater_name}</Link>,
    defaultDirection: "asc",
  },
  {
    key: "nation_code", header: "Nation",
    cell: (r) => nationDisplay(r.nation_code),
    defaultDirection: "asc",
  },
  {
    key: "club_name", header: "Club",
    cell: (r) => r.club_name ? (
      <Link href={`/clubs/${encodeURIComponent(r.club_name)}`} className="text-muted-foreground hover:underline">{r.club_name}</Link>
    ) : <span className="text-muted-foreground">—</span>,
    defaultDirection: "asc",
  },
  { key: "competitions", header: "Comps", className: "text-right tabular-nums",
    cell: (r) => r.competitions },
  { key: "best_tss", header: "Best TSS", className: "text-right tabular-nums",
    cell: (r) => points(r.best_tss) },
  { key: "latest_seen_date", header: "Latest", className: "text-muted-foreground",
    cell: (r) => r.latest_seen_date ?? "—" },
];

export function SkatersTable({ rows }: { rows: SkaterSearchRow[] }) {
  return <DataTable<Row> rows={rows as Row[]} columns={columns} defaultSort={{ key: "latest_seen_date", direction: "desc" }} initialPageSize={50} />;
}
