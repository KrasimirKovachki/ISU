"use client";
/** Recharts horizontal bar — top N nations by segment_scores. */
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { ChartFrame } from "@/components/chart-frame";
import type { NationalityRow } from "@/lib/types";

export function NationalitiesBarChart({ rows, take = 12 }: { rows: NationalityRow[]; take?: number }) {
  const data = [...rows]
    .sort((a, b) => b.segment_scores - a.segment_scores)
    .slice(0, take)
    .map((n) => ({ name: n.nation_code, scores: n.segment_scores, skaters: n.skaters }))
    .reverse();
  return (
    <ChartFrame>
      <ResponsiveContainer width="100%" height="100%" initialDimension={{ width: 600, height: 288 }}>
        <BarChart data={data} layout="vertical" margin={{ left: 8, right: 16, top: 4, bottom: 4 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" horizontal={false} />
          <XAxis type="number" stroke="var(--muted-foreground)" fontSize={12} tickLine={false} axisLine={false} />
          <YAxis dataKey="name" type="category" width={48} stroke="var(--muted-foreground)" fontSize={12} tickLine={false} axisLine={false} />
          <Tooltip
            cursor={{ fill: "var(--accent)" }}
            contentStyle={{
              background: "var(--popover)",
              border: "1px solid var(--border)",
              borderRadius: "var(--radius-md)",
              color: "var(--popover-foreground)",
              fontSize: "12px",
            }}
            formatter={(value, key) =>
              key === "scores"
                ? [typeof value === "number" ? value.toLocaleString() : String(value ?? "—"), "Segment scores"]
                : [typeof value === "number" ? value.toLocaleString() : String(value ?? "—"), "Skaters"]
            }
          />
          <Bar dataKey="scores" fill="var(--chart-1)" radius={[0, 4, 4, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </ChartFrame>
  );
}
