"use client";
/**
 * Reusable client-side sortable + paginated table. The server fetches the
 * full dataset (capped) and hands it in as a prop; we sort/page locally.
 *
 * Pass column definitions with optional `accessor` (for sort key derivation)
 * and `cell` (custom render). Active sort header gets ▲ / ▼.
 */
import { useMemo, useState, type ReactNode } from "react";

import { Button } from "@/components/ui/button";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";

export type Column<T> = {
  key: string;
  header: ReactNode;
  className?: string;
  /** Pull the sortable value out of the row. Defaults to row[key]. */
  accessor?: (row: T) => unknown;
  /** Custom cell render. Defaults to formatted accessor value. */
  cell?: (row: T) => ReactNode;
  /** When false the column is not sortable (default true). */
  sortable?: boolean;
  /** Default sort direction the first time the user clicks (default desc for numbers, asc for text). */
  defaultDirection?: "asc" | "desc";
};

function compare(a: unknown, b: unknown): number {
  if (a === b) return 0;
  if (a === null || a === undefined) return 1;
  if (b === null || b === undefined) return -1;
  if (typeof a === "number" && typeof b === "number") return a - b;
  return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: "base" });
}

export function DataTable<T extends Record<string, unknown>>({
  rows,
  columns,
  defaultSort,
  pageSizeOptions = [25, 50, 100, 250],
  initialPageSize = 50,
  emptyLabel = "No matches.",
}: {
  rows: T[];
  columns: Column<T>[];
  defaultSort?: { key: string; direction: "asc" | "desc" };
  pageSizeOptions?: number[];
  initialPageSize?: number;
  emptyLabel?: string;
}) {
  const [sortKey, setSortKey] = useState(defaultSort?.key ?? "");
  const [sortDir, setSortDir] = useState<"asc" | "desc">(defaultSort?.direction ?? "desc");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(initialPageSize);

  const sortedRows = useMemo(() => {
    if (!sortKey) return rows;
    const col = columns.find((c) => c.key === sortKey);
    const accessor = col?.accessor ?? ((r: T) => r[sortKey]);
    const sorted = [...rows].sort((a, b) => compare(accessor(a), accessor(b)));
    if (sortDir === "desc") sorted.reverse();
    return sorted;
  }, [rows, columns, sortKey, sortDir]);

  const total = sortedRows.length;
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  const safePage = Math.min(page, pageCount);
  const start = (safePage - 1) * pageSize;
  const end = Math.min(start + pageSize, total);
  const visible = sortedRows.slice(start, end);

  function toggleSort(col: Column<T>) {
    if (col.sortable === false) return;
    if (sortKey === col.key) {
      setSortDir(sortDir === "desc" ? "asc" : "desc");
    } else {
      setSortKey(col.key);
      setSortDir(col.defaultDirection ?? "desc");
    }
    setPage(1);
  }

  return (
    <div className="space-y-3">
      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              {columns.map((col) => {
                const active = sortKey === col.key;
                return (
                  <TableHead
                    key={col.key}
                    className={cn(col.className, col.sortable !== false ? "cursor-pointer select-none hover:text-foreground" : "")}
                    onClick={() => toggleSort(col)}
                  >
                    <span className="inline-flex items-center gap-1">
                      {col.header}
                      {col.sortable !== false ? (
                        <span className={cn("text-xs", active ? "text-foreground" : "text-muted-foreground/40")}>
                          {active ? (sortDir === "desc" ? "▼" : "▲") : "↕"}
                        </span>
                      ) : null}
                    </span>
                  </TableHead>
                );
              })}
            </TableRow>
          </TableHeader>
          <TableBody>
            {visible.length === 0 ? (
              <TableRow>
                <TableCell colSpan={columns.length} className="text-center text-muted-foreground py-12">{emptyLabel}</TableCell>
              </TableRow>
            ) : (
              visible.map((row, i) => (
                <TableRow key={(row.id as string | number) ?? i}>
                  {columns.map((col) => {
                    const value = col.cell
                      ? col.cell(row)
                      : (() => {
                          const v = (col.accessor ?? ((r: T) => r[col.key]))(row);
                          return v === null || v === undefined || v === "" ? "—" : String(v);
                        })();
                    return (
                      <TableCell key={col.key} className={col.className}>{value}</TableCell>
                    );
                  })}
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>
      <div className="flex items-center justify-between text-sm flex-wrap gap-2">
        <div className="text-muted-foreground">
          {total === 0 ? "0 of 0" : `${start + 1}-${end} of ${total}`}
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-muted-foreground">Rows</span>
          <Select value={String(pageSize)} onValueChange={(v) => { setPageSize(Number(v)); setPage(1); }}>
            <SelectTrigger className="w-20 h-8 text-xs"><SelectValue /></SelectTrigger>
            <SelectContent>
              {pageSizeOptions.map((s) => (<SelectItem key={s} value={String(s)}>{s}</SelectItem>))}
            </SelectContent>
          </Select>
          <Button size="sm" variant="outline" onClick={() => setPage(safePage - 1)} disabled={safePage <= 1}>Prev</Button>
          <Button size="sm" variant="outline" onClick={() => setPage(safePage + 1)} disabled={safePage >= pageCount}>Next</Button>
        </div>
      </div>
    </div>
  );
}
