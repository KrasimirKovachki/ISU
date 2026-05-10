"use client";
/** Radar chart of average vs best per program component. */
import { PolarAngleAxis, PolarGrid, PolarRadiusAxis, Radar, RadarChart, ResponsiveContainer, Tooltip, Legend } from "recharts";

import type { SkaterComponentSummary } from "@/lib/types";

export function ComponentRadar({ rows }: { rows: SkaterComponentSummary[] }) {
  if (!rows || rows.length === 0) {
    return <div className="text-sm text-muted-foreground py-8 text-center">No component scores parsed yet.</div>;
  }
  const data = rows.map((r) => ({
    name: r.component_name,
    avg: Number(r.avg_score),
    best: Number(r.best_score),
  }));
  return (
    <div className="h-72 w-full">
      <ResponsiveContainer>
        <RadarChart data={data}>
          <PolarGrid stroke="var(--border)" />
          <PolarAngleAxis dataKey="name" tick={{ fontSize: 11, fill: "var(--muted-foreground)" }} />
          <PolarRadiusAxis stroke="var(--muted-foreground)" tick={{ fontSize: 10 }} domain={[0, 10]} />
          <Radar name="Average" dataKey="avg" stroke="var(--chart-1)" fill="var(--chart-1)" fillOpacity={0.35} />
          <Radar name="Best" dataKey="best" stroke="var(--chart-2)" fill="var(--chart-2)" fillOpacity={0.15} />
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
    </div>
  );
}
