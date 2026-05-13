"use client";
import Link from "next/link";

import { DataTable, type Column } from "@/components/data-table";
import { formatDate, nationDisplay, points } from "@/lib/format";
import type { NationalityRow } from "@/lib/types";

type Row = NationalityRow & Record<string, unknown>;

const columns: Column<Row>[] = [
  { key: "nation_code", header: "Nation",
    cell: (r) => (
      <Link href={`/nationalities/${encodeURIComponent(r.nation_code)}`} className="font-medium hover:underline">
        {nationDisplay(r.nation_code)}
      </Link>
    ),
    defaultDirection: "asc" },
  { key: "skaters",                       header: "Skaters",   className: "text-right tabular-nums" },
  { key: "development_skaters",           header: "Mlad",      className: "text-right tabular-nums" },
  { key: "converted_development_skaters", header: "Converted", className: "text-right tabular-nums" },
  { key: "events",                        header: "Events",    className: "text-right tabular-nums" },
  { key: "segment_scores",                header: "Scores",    className: "text-right tabular-nums" },
  { key: "best_tss", header: "Best TSS", className: "text-right tabular-nums",
    cell: (r) => points(r.best_tss) },
  { key: "latest_seen_date", header: "Latest", className: "text-muted-foreground",
    cell: (r) => formatDate(r.latest_seen_date) },
];

export function NationalitiesTable({ rows }: { rows: NationalityRow[] }) {
  return <DataTable<Row> rows={rows as Row[]} columns={columns} defaultSort={{ key: "segment_scores", direction: "desc" }} initialPageSize={50} />;
}
