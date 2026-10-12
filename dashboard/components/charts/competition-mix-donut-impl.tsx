"use client";
/** Donut chart of events by scope (international / federation / club_or_open / unknown). */
import { useRouter } from "next/navigation";
import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from "recharts";

import { ChartFrame } from "@/components/chart-frame";
import type { CompetitionMixRow } from "@/lib/types";

const COLORS: Record<string, string> = {
  international: "var(--chart-1)",
  federation: "var(--chart-2)",
  club_or_open: "var(--chart-3)",
  unknown: "var(--chart-4)",
};

export function CompetitionMixDonut({
  rows,
  href,
}: {
  rows: CompetitionMixRow[];
  href?: string;
}) {
  const router = useRouter();
  const grouped = new Map<string, number>();
  for (const r of rows) {
    const key = r.scope_hint ?? "unknown";
    grouped.set(key, (grouped.get(key) ?? 0) + r.events);
  }
  const data = [...grouped.entries()].map(([name, value]) => ({ name, value, href }));
  return (
    <ChartFrame>
      <ResponsiveContainer width="100%" height="100%" initialDimension={{ width: 600, height: 288 }}>
        <PieChart>
          <Pie
            data={data}
            dataKey="value"
            nameKey="name"
            innerRadius={50}
            outerRadius={90}
            paddingAngle={2}
            stroke="var(--background)"
            onClick={(state) => {
              const href = state?.href;
              if (typeof href === "string") router.push(href);
            }}
            style={{ cursor: href ? "pointer" : "default" }}
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
            formatter={(value, name) => [
              `${typeof value === "number" ? value : String(value ?? "—")} events`,
              typeof name === "string" ? name.replace("_", " ") : String(name ?? "—"),
            ]}
          />
        </PieChart>
      </ResponsiveContainer>
    </ChartFrame>
  );
}
