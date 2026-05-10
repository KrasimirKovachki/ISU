"use client";
/**
 * Year-range filter that writes to URL state (?from_year=&to_year=). Server
 * pages downstream re-render with the new searchParams.
 *
 * Pending values are local until "Go" — avoids two re-fetches when both
 * From and To are changed.
 */
import { useRouter, useSearchParams, usePathname } from "next/navigation";
import { useEffect, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";

export function YearRangeFilter({ minYear, maxYear }: { minYear: number; maxYear: number }) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const initialFrom = searchParams.get("from_year") ?? "";
  const initialTo = searchParams.get("to_year") ?? "";
  const [from, setFrom] = useState(initialFrom);
  const [to, setTo] = useState(initialTo);

  // If the URL changes externally (e.g. browser back), sync the inputs.
  useEffect(() => {
    setFrom(searchParams.get("from_year") ?? "");
    setTo(searchParams.get("to_year") ?? "");
  }, [searchParams]);

  const isFiltered = Boolean(from || to);

  function apply() {
    const params = new URLSearchParams(searchParams.toString());
    if (from) params.set("from_year", from);
    else params.delete("from_year");
    if (to) params.set("to_year", to);
    else params.delete("to_year");
    const qs = params.toString();
    router.push(qs ? `${pathname}?${qs}` : pathname);
  }

  function clear() {
    const params = new URLSearchParams(searchParams.toString());
    params.delete("from_year");
    params.delete("to_year");
    const qs = params.toString();
    router.push(qs ? `${pathname}?${qs}` : pathname);
  }

  const years: string[] = [];
  for (let y = maxYear; y >= minYear; y--) years.push(String(y));

  return (
    <div className="flex items-center gap-2 flex-wrap">
      <span className="text-xs text-muted-foreground">From</span>
      <Select value={from || "all"} onValueChange={(v) => setFrom(v === "all" ? "" : v)}>
        <SelectTrigger className="w-24 h-8 text-xs"><SelectValue /></SelectTrigger>
        <SelectContent>
          <SelectItem value="all">All</SelectItem>
          {years.map((y) => <SelectItem key={y} value={y}>{y}</SelectItem>)}
        </SelectContent>
      </Select>
      <span className="text-xs text-muted-foreground">to</span>
      <Select value={to || "all"} onValueChange={(v) => setTo(v === "all" ? "" : v)}>
        <SelectTrigger className="w-24 h-8 text-xs"><SelectValue /></SelectTrigger>
        <SelectContent>
          <SelectItem value="all">All</SelectItem>
          {years.map((y) => <SelectItem key={y} value={y}>{y}</SelectItem>)}
        </SelectContent>
      </Select>
      <Button size="sm" onClick={apply}>Go</Button>
      {isFiltered ? (
        <Button size="sm" variant="ghost" onClick={clear}>Clear</Button>
      ) : null}
    </div>
  );
}
