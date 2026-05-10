"use client";
/** Donut chart of events by scope (international / federation / club_or_open / unknown). */
import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from "recharts";

import { ChartFrame } from "@/components/chart-frame";
import type { CompetitionMixRow } from "@/lib/types";

const COLORS: Record<string, string> = {
  international: "var(--chart-1)",
  federation: "var(--chart-2)",
  club_or_open: "var(--chart-3)",
  unknown: "var(--chart-4)",
};

export function CompetitionMixDonut({ rows }: { rows: CompetitionMixRow[] }) {
  const grouped = new Map<string, number>();
  for (const r of rows) {
    const key = r.scope_hint ?? "unknown";
    grouped.set(key, (grouped.get(key) ?? 0) + r.events);
  }
  const data = [...grouped.entries()].map(([name, value]) => ({ name, value }));
  return (
    <ChartFrame>
      <ResponsiveContainer>
        <PieChart>
          <Pie
            data={data}
            dataKey="value"
            nameKey="name"
            innerRadius={50}
            outerRadius={90}
            paddingAngle={2}
            stroke="var(--background)"
          >
            {data.map((d) => (
              <Cell key={d.name} fill={COLORS[d.name] ?? "var(--muted)"} />
            ))}
          </Pie>
          <Tooltip
            contentStyle={{
              background: "var(--popover)",
              border: "1px solid var(--border)",
              borderRadius: "var(--radius-md)",
              color: "var(--popover-foreground)",
              fontSize: "12px",
            }}
            formatter={(value: number, name: string) => [`${value} events`, name.replace("_", " ")]}
          />
        </PieChart>
      </ResponsiveContainer>
    </ChartFrame>
  );
}
