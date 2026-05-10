"use client";
/**
 * Reusable client-side sortable + paginated table with URL-persisted state.
 *
 * Optional `urlPrefix` makes sort/page share the page (multiple tables on
 * one page get distinct prefixes — e.g. urlPrefix="club"). Without a prefix
 * we use bare keys (sort, dir, page, size) so refreshing or sharing a
 * sorted/paged list works as expected.
 */
import { useRouter, useSearchParams, usePathname } from "next/navigation";
import { useMemo, useCallback, type ReactNode } from "react";

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
  accessor?: (row: T) => unknown;
  cell?: (row: T) => ReactNode;
  sortable?: boolean;
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
  urlPrefix,
}: {
  rows: T[];
  columns: Column<T>[];
  defaultSort?: { key: string; direction: "asc" | "desc" };
  pageSizeOptions?: number[];
  initialPageSize?: number;
  emptyLabel?: string;
  /** Optional disambiguator when more than one DataTable lives on a page. */
  urlPrefix?: string;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const k = (name: string) => (urlPrefix ? `${urlPrefix}_${name}` : name);

  const sortKey = searchParams.get(k("sort")) ?? defaultSort?.key ?? "";
  const sortDir = (searchParams.get(k("dir")) as "asc" | "desc" | null) ?? defaultSort?.direction ?? "desc";
  const page = Math.max(1, Number(searchParams.get(k("page")) ?? "1") || 1);
  const pageSize = Math.max(1, Number(searchParams.get(k("size")) ?? initialPageSize) || initialPageSize);

  const update = useCallback(
    (updates: Record<string, string | null>) => {
      const params = new URLSearchParams(searchParams.toString());
      for (const [name, value] of Object.entries(updates)) {
        if (value === null || value === "") params.delete(name);
        else params.set(name, value);
      }
      const qs = params.toString();
      router.replace(qs ? `${pathname}?${qs}` : pathname, { scroll: false });
    },
    [router, pathname, searchParams],
  );

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
      update({ [k("sort")]: col.key, [k("dir")]: sortDir === "desc" ? "asc" : "desc", [k("page")]: "1" });
    } else {
      update({ [k("sort")]: col.key, [k("dir")]: col.defaultDirection ?? "desc", [k("page")]: "1" });
    }
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
          <Select
            value={String(pageSize)}
            onValueChange={(v) => update({ [k("size")]: v === String(initialPageSize) ? null : v, [k("page")]: "1" })}
          >
            <SelectTrigger className="w-20 h-8 text-xs"><SelectValue /></SelectTrigger>
            <SelectContent>
              {pageSizeOptions.map((s) => (<SelectItem key={s} value={String(s)}>{s}</SelectItem>))}
            </SelectContent>
          </Select>
          <Button size="sm" variant="outline" onClick={() => update({ [k("page")]: String(safePage - 1) })} disabled={safePage <= 1}>Prev</Button>
          <Button size="sm" variant="outline" onClick={() => update({ [k("page")]: String(safePage + 1) })} disabled={safePage >= pageCount}>Next</Button>
        </div>
      </div>
    </div>
  );
}
