/**
 * Reusable KPI tile. Optional icon + delta + sub-label.
 * Server-component friendly (no hooks, no event handlers).
 */
import type { LucideIcon } from "lucide-react";
import Link from "next/link";
import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";

type Props = {
  label: string;
  value: string | number | null | undefined;
  icon?: LucideIcon;
  hint?: string;
  className?: string;
  href?: string;
  ctaLabel?: string;
};

export function KpiCard({ label, value, icon: Icon, hint, className, href, ctaLabel = "Open" }: Props) {
  const display =
    value === null || value === undefined
      ? "—"
      : typeof value === "number"
        ? value.toLocaleString()
        : value;
  const content = (
    <CardContent className="p-4">
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span className="uppercase tracking-wide font-medium">{label}</span>
        {Icon ? <Icon className="h-3.5 w-3.5" /> : null}
      </div>
      <div className="mt-1 text-2xl font-semibold tabular-nums">{display}</div>
      {hint ? <div className="mt-1 text-xs text-muted-foreground">{hint}</div> : null}
      {href ? <div className="mt-3 text-xs font-medium text-sky-300">{ctaLabel}</div> : null}
    </CardContent>
  );

  if (href) {
    return (
      <Card
        className={cn(
          "border-slate-800 bg-slate-950 text-slate-100 transition-colors transition-shadow hover:border-slate-700 hover:bg-slate-900/90 hover:shadow-sm",
          className,
        )}
      >
        <Link href={href} className="block">
          {content}
        </Link>
      </Card>
    );
  }

  return (
    <Card
      className={cn(
        "border-slate-800 bg-slate-950 text-slate-100 transition-colors transition-shadow hover:border-slate-700 hover:bg-slate-900/90 hover:shadow-sm",
        className,
      )}
    >
      {content}
    </Card>
  );
}
