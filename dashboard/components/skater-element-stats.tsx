"use client";
/**
 * Element analytics for a skater:
 *  - Top chart: bar of count per base element code, colour-shifted by avg
 *    GOE (red = negative, green = positive).
 *  - Bottom chart: scatter of every executed element on (date × GOE), useful
 *    for spotting consistency drift and outliers per element type.
 */
import {
  Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Scatter, ScatterChart,
  Tooltip, XAxis, YAxis, ZAxis,
} from "recharts";
import { useMemo, useState } from "react";

import { ChartFrame } from "@/components/chart-frame";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import type { SkaterElementRow } from "@/lib/types";

function strip<T>(value: T | null | undefined, fallback: T): T {
  return value === null || value === undefined ? fallback : value;
}

function colorForGoe(avg: number): string {
  if (avg > 0.5) return "var(--chart-2)";   // strong positive
  if (avg > 0) return "var(--chart-3)";     // mild positive
  if (avg > -0.5) return "var(--chart-4)";  // mild negative
  return "var(--chart-5)";                   // strong negative
}

export function SkaterElementStats({ elements }: { elements: SkaterElementRow[] }) {
  // Aggregate per base element code.
  const byCode = useMemo(() => {
    const map = new Map<string, { code: string; count: number; goeSum: number; bestGoe: number; basePoints: number; panel: number }>();
    for (const el of elements) {
      const code = el.base_element_code || el.element_code;
      if (!code) continue;
      const goe = strip(el.goe, 0);
      const panel = strip(el.panel_score, 0);
      const base = strip(el.base_value, 0);
      const cur = map.get(code) ?? { code, count: 0, goeSum: 0, bestGoe: -Infinity, basePoints: 0, panel: 0 };
      cur.count += 1;
      cur.goeSum += goe;
      cur.bestGoe = Math.max(cur.bestGoe, goe);
      cur.basePoints += base;
      cur.panel += panel;
      map.set(code, cur);
    }
    return [...map.values()].map((e) => ({
      ...e,
      avgGoe: e.count ? e.goeSum / e.count : 0,
    }));
  }, [elements]);

  const topByCount = [...byCode].sort((a, b) => b.count - a.count).slice(0, 18).reverse();

  // Selectable element-code filter for the scatter (defaults to "all").
  const codeOptions = ["all", ...[...byCode].sort((a, b) => b.count - a.count).map((c) => c.code)];
  const [selectedCode, setSelectedCode] = useState<string>("all");
  const scatterData = useMemo(() => {
    return elements
      .filter((el) => {
        const code = el.base_element_code || el.element_code;
        return el.date_key && (selectedCode === "all" || code === selectedCode);
      })
      .map((el) => ({
        x: el.date_key!.slice(0, 10),
        y: strip(el.goe, 0),
        code: el.base_element_code || el.element_code,
        label: `${el.element_code} · GOE ${strip(el.goe, 0).toFixed(2)}`,
      }));
  }, [elements, selectedCode]);

  if (elements.length === 0) {
    return (
      <Card>
        <CardContent className="text-sm text-muted-foreground py-8 text-center">
          No element data parsed yet for this skater.
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="text-base">
            Element bank
            <span className="ml-2 text-xs font-normal text-muted-foreground">
              · {elements.length} executed elements across {byCode.length} types
            </span>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <ChartFrame>
            <ResponsiveContainer width="100%" height="100%" initialDimension={{ width: 600, height: 288 }}>
              <BarChart data={topByCount} layout="vertical" margin={{ left: 8, right: 16, top: 4, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" horizontal={false} />
                <XAxis type="number" stroke="var(--muted-foreground)" fontSize={11} tickLine={false} axisLine={false} />
                <YAxis dataKey="code" type="category" width={70} stroke="var(--muted-foreground)" fontSize={11} tickLine={false} axisLine={false} />
                <Tooltip
                  cursor={{ fill: "var(--accent)" }}
                  contentStyle={{ background: "var(--popover)", border: "1px solid var(--border)", borderRadius: "var(--radius-md)", color: "var(--popover-foreground)", fontSize: "12px" }}
                  formatter={(value: number, _name, p) => [
                    `${value} time${value === 1 ? "" : "s"}, avg GOE ${(p.payload.avgGoe).toFixed(2)}`,
                    p.payload.code,
                  ]}
                />
                <Bar dataKey="count" radius={[0, 4, 4, 0]}>
                  {topByCount.map((c, i) => (
                    <Cell key={i} fill={colorForGoe(c.avgGoe)} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartFrame>
          <div className="mt-2 flex flex-wrap items-center gap-3 text-xs text-muted-foreground">
            <span>Bar colour = average GOE for that element.</span>
            <span className="inline-flex items-center gap-1"><span className="inline-block w-3 h-3 rounded-sm" style={{ background: "var(--chart-2)" }} /> &gt; +0.5</span>
            <span className="inline-flex items-center gap-1"><span className="inline-block w-3 h-3 rounded-sm" style={{ background: "var(--chart-3)" }} /> 0 to +0.5</span>
            <span className="inline-flex items-center gap-1"><span className="inline-block w-3 h-3 rounded-sm" style={{ background: "var(--chart-4)" }} /> -0.5 to 0</span>
            <span className="inline-flex items-center gap-1"><span className="inline-block w-3 h-3 rounded-sm" style={{ background: "var(--chart-5)" }} /> &lt; -0.5</span>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle className="text-base">GOE over time</CardTitle>
            <Select value={selectedCode} onValueChange={setSelectedCode}>
              <SelectTrigger className="w-44 h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {codeOptions.map((c) => (
                  <SelectItem key={c} value={c}>{c === "all" ? "All elements" : c}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </CardHeader>
        <CardContent>
          <ChartFrame>
            <ResponsiveContainer width="100%" height="100%" initialDimension={{ width: 600, height: 288 }}>
              <ScatterChart margin={{ left: 4, right: 16, top: 8, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis dataKey="x" type="category" stroke="var(--muted-foreground)" fontSize={11} tickLine={false} axisLine={false} minTickGap={32} />
                <YAxis dataKey="y" type="number" domain={[-5, 5]} stroke="var(--muted-foreground)" fontSize={11} tickLine={false} axisLine={false} />
                <ZAxis range={[40, 40]} />
                <Tooltip
                  contentStyle={{ background: "var(--popover)", border: "1px solid var(--border)", borderRadius: "var(--radius-md)", color: "var(--popover-foreground)", fontSize: "12px" }}
                  cursor={{ strokeDasharray: "3 3" }}
                  labelFormatter={(_, payload) => (payload?.[0]?.payload as { x: string })?.x}
                  formatter={(value: number, _name, p) => [(p.payload as { label: string }).label, ""]}
                />
                <Scatter data={scatterData} fill="var(--chart-1)" />
              </ScatterChart>
            </ResponsiveContainer>
          </ChartFrame>
          <p className="mt-2 text-xs text-muted-foreground">
            Each dot is one executed element. GOE range -5 to +5 (ISU scale). Use the dropdown to focus on a specific element type.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}
