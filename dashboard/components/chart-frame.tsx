"use client";
/**
 * Fixed-height slot for Recharts charts.
 *
 * Recharts v3 renders the SVG only client-side (after hydration); during
 * SSR the wrapper is empty. ChartFrame:
 *  1. Reserves the layout space so the page doesn't reflow when the chart
 *     hydrates.
 *  2. Shows a subtle "Loading chart…" placeholder until the first client
 *     paint, so an empty wrapper isn't mistaken for a missing chart.
 *  3. Makes sure the parent has explicit width/height for ResponsiveContainer
 *     to measure correctly.
 *
 * Charts must additionally pass `initialDimension={{ width, height }}` to
 * their ResponsiveContainer so Recharts has fallback dimensions before its
 * ResizeObserver fires.
 */
import { useEffect, useState, type ReactNode } from "react";

import { cn } from "@/lib/utils";

export function ChartFrame({
  children,
  className,
  height = "h-72",
}: {
  children: ReactNode;
  className?: string;
  height?: string;
}) {
  const [hydrated, setHydrated] = useState(false);
  useEffect(() => setHydrated(true), []);
  return (
    <div className={cn("w-full relative", height, className)}>
      {children}
      {!hydrated ? (
        <div className="absolute inset-0 flex items-center justify-center text-xs text-muted-foreground bg-muted/20 rounded-md pointer-events-none">
          Loading chart…
        </div>
      ) : null}
    </div>
  );
}
