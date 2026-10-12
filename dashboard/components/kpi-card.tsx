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
    <CardContent className="p-[18px]">
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span className="text-[13px] uppercase tracking-[0.08em]">{label}</span>
        {Icon ? <Icon className="h-3.5 w-3.5" /> : null}
      </div>
      {/* numbers in the score font, names (e.g. a club) as normal bold text */}
      <div className={cn("mt-1 leading-tight", /^[\d\s.,:+\-—%/]+$/.test(String(display)) ? "num text-[30px] font-semibold" : "text-xl font-bold")}>{display}</div>
      {hint ? <div className="mt-1 text-xs text-muted-foreground">{hint}</div> : null}
      {href ? <div className="mt-3 text-xs font-medium text-brand">{ctaLabel}</div> : null}
    </CardContent>
  );

  if (href) {
    return (
      <Card
        className={cn(
          "rounded-2xl border-0 bg-card text-foreground shadow-none transition-shadow hover:shadow-md",
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
        "rounded-2xl border-0 bg-card text-foreground shadow-none",
        className,
      )}
    >
      {content}
    </Card>
  );
}
