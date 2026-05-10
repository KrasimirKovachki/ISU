"use client";
/** Line chart of TSS / TES / PCS over time for a single skater. */
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { ChartFrame } from "@/components/chart-frame";
import type { SkaterScoreRow } from "@/lib/types";

export function SkaterProgressionChart({ scores }: { scores: SkaterScoreRow[] }) {
  // Filter to scored rows only and order chronologically.
  const data = scores
    .filter((s) => s.event_start_date && (s.tss !== null || s.tes !== null || s.pcs !== null))
    .sort((a, b) => (a.event_start_date! < b.event_start_date! ? -1 : 1))
    .map((s) => ({
      date: s.event_start_date!.slice(0, 10),
      tss: s.tss,
      tes: s.tes,
      pcs: s.pcs,
      label: `${s.event_name ?? ""} · ${s.segment_name ?? ""}`,
    }));

  if (data.length === 0) {
    return <div className="text-sm text-muted-foreground py-8 text-center">No scored segments yet.</div>;
  }

  return (
    <ChartFrame>
      <ResponsiveContainer width="100%" height="100%" initialDimension={{ width: 600, height: 288 }}>
        <LineChart data={data} margin={{ left: 4, right: 16, top: 8, bottom: 4 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
          <XAxis dataKey="date" stroke="var(--muted-foreground)" fontSize={11} tickLine={false} axisLine={false} minTickGap={24} />
          <YAxis stroke="var(--muted-foreground)" fontSize={11} tickLine={false} axisLine={false} />
          <Tooltip
            contentStyle={{
              background: "var(--popover)",
              border: "1px solid var(--border)",
              borderRadius: "var(--radius-md)",
              color: "var(--popover-foreground)",
              fontSize: "12px",
            }}
            labelFormatter={(v: string, payload) => {
              const item = payload?.[0]?.payload as { label?: string } | undefined;
              return item?.label ?? v;
            }}
          />
          <Legend wrapperStyle={{ fontSize: "12px" }} />
          <Line type="monotone" dataKey="tss" stroke="var(--chart-1)" strokeWidth={2} dot={{ r: 2 }} name="TSS" />
          <Line type="monotone" dataKey="tes" stroke="var(--chart-2)" strokeWidth={2} dot={{ r: 2 }} name="TES" />
          <Line type="monotone" dataKey="pcs" stroke="var(--chart-3)" strokeWidth={2} dot={{ r: 2 }} name="PCS" />
        </LineChart>
      </ResponsiveContainer>
    </ChartFrame>
  );
}
