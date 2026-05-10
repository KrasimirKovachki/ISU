/**
 * Reusable KPI tile. Optional icon + delta + sub-label.
 * Server-component friendly (no hooks, no event handlers).
 */
import type { LucideIcon } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";

type Props = {
  label: string;
  value: string | number | null | undefined;
  icon?: LucideIcon;
  hint?: string;
  className?: string;
};

export function KpiCard({ label, value, icon: Icon, hint, className }: Props) {
  const display =
    value === null || value === undefined
      ? "—"
      : typeof value === "number"
        ? value.toLocaleString()
        : value;
  return (
    <Card className={cn("transition-shadow hover:shadow-sm", className)}>
      <CardContent className="p-4">
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <span className="uppercase tracking-wide font-medium">{label}</span>
          {Icon ? <Icon className="h-3.5 w-3.5" /> : null}
        </div>
        <div className="mt-1 text-2xl font-semibold tabular-nums">{display}</div>
        {hint ? <div className="mt-1 text-xs text-muted-foreground">{hint}</div> : null}
      </CardContent>
    </Card>
  );
}
