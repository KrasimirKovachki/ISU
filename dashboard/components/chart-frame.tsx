"use client";
/**
 * Recharts ResponsiveContainer needs a real DOM parent to measure. During
 * server-render the parent is just an HTML string, so ResponsiveContainer
 * reports `width(-1) height(-1)` and renders nothing. ChartFrame holds the
 * fixed-height slot during SSR + first render, then mounts the chart on
 * the client tick after hydration.
 *
 * Charts: just wrap your <ResponsiveContainer> in <ChartFrame>.
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
  /** Tailwind height utility for the slot. Default h-72 (~288px). */
  height?: string;
}) {
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  return (
    <div className={cn("w-full", height, className)}>
      {mounted ? children : null}
    </div>
  );
}
