"use client";
/**
 * Skater-vs-skater comparison radar.
 *
 * Use the search input to find a second skater (debounced live search via
 * TanStack Query against /api/skaters?q=). Pick a result and we fetch
 * /api/skaters/{key} for its component_summary, then overlay it on the
 * primary radar.
 */
import { useQuery } from "@tanstack/react-query";
import { X } from "lucide-react";
import { useState, useDeferredValue } from "react";
import { PolarAngleAxis, PolarGrid, PolarRadiusAxis, Radar, RadarChart, ResponsiveContainer, Tooltip, Legend } from "recharts";

import { ChartFrame } from "@/components/chart-frame";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { searchSkaters, getSkater } from "@/lib/api";
import type { SkaterComponentSummary } from "@/lib/types";

type Props = {
  primaryName: string;
  primaryRows: SkaterComponentSummary[];
  primarySkaterKey: number;
};

export function ComparisonRadar({ primaryName, primaryRows, primarySkaterKey }: Props) {
  const [query, setQuery] = useState("");
  const deferredQuery = useDeferredValue(query);
  const [pickedKey, setPickedKey] = useState<number | null>(null);

  const search = useQuery({
    queryKey: ["skater-search", deferredQuery],
    queryFn: () => searchSkaters(deferredQuery, 8),
    enabled: deferredQuery.trim().length >= 2 && !pickedKey,
    staleTime: 30_000,
  });

  const second = useQuery({
    queryKey: ["skater", pickedKey],
    queryFn: () => getSkater(pickedKey!),
    enabled: pickedKey !== null,
    staleTime: 60_000,
  });

  // Build a unified data set keyed by component name.
  const allComponents = new Set<string>();
  for (const r of primaryRows) allComponents.add(r.component_name);
  for (const r of second.data?.component_summary ?? []) allComponents.add(r.component_name);
  const data = [...allComponents].map((name) => ({
    name,
    primary: Number(primaryRows.find((r) => r.component_name === name)?.avg_score ?? 0),
    other: Number(second.data?.component_summary?.find((r) => r.component_name === name)?.avg_score ?? 0),
  }));

  return (
    <div className="space-y-3">
      <ChartFrame>
        <ResponsiveContainer width="100%" height="100%" initialDimension={{ width: 400, height: 288 }}>
          <RadarChart data={data}>
            <PolarGrid stroke="var(--border)" />
            <PolarAngleAxis dataKey="name" tick={{ fontSize: 11, fill: "var(--muted-foreground)" }} />
            <PolarRadiusAxis stroke="var(--muted-foreground)" tick={{ fontSize: 10 }} domain={[0, 10]} />
            <Radar name={primaryName} dataKey="primary" stroke="var(--chart-1)" fill="var(--chart-1)" fillOpacity={0.35} />
            {second.data ? (
              <Radar name={second.data.skater_name} dataKey="other" stroke="var(--chart-2)" fill="var(--chart-2)" fillOpacity={0.25} />
            ) : null}
            <Legend wrapperStyle={{ fontSize: "12px" }} />
            <Tooltip
              contentStyle={{
                background: "var(--popover)",
                border: "1px solid var(--border)",
                borderRadius: "var(--radius-md)",
                color: "var(--popover-foreground)",
                fontSize: "12px",
              }}
              formatter={(value: number) => value.toFixed(2)}
            />
          </RadarChart>
        </ResponsiveContainer>
      </ChartFrame>
      <div className="border-t pt-3">
        {pickedKey && second.data ? (
          <div className="flex items-center gap-2">
            <span className="text-xs text-muted-foreground">Comparing with</span>
            <span className="text-sm font-medium">{second.data.skater_name}</span>
            <Button size="sm" variant="ghost" onClick={() => { setPickedKey(null); setQuery(""); }} className="h-6 w-6 p-0">
              <X className="h-3 w-3" />
            </Button>
          </div>
        ) : (
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <Input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search a skater to overlay…"
                className="h-8 text-sm"
              />
            </div>
            {search.data && deferredQuery.trim().length >= 2 ? (
              <div className="flex flex-wrap gap-1">
                {search.data.items
                  .filter((s) => s.skater_key !== primarySkaterKey)
                  .slice(0, 6)
                  .map((s) => (
                    <Button
                      key={s.skater_key}
                      size="sm"
                      variant="outline"
                      className="h-7 text-xs"
                      onClick={() => setPickedKey(s.skater_key)}
                    >
                      {s.skater_name} <span className="ml-1 text-muted-foreground">({s.nation_code})</span>
                    </Button>
                  ))}
                {search.data.items.length === 0 ? (
                  <span className="text-xs text-muted-foreground">No matches.</span>
                ) : null}
              </div>
            ) : (
              <p className="text-xs text-muted-foreground">Type at least 2 letters to search.</p>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
