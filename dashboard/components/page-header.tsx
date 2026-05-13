/** Slim page header — title + optional subtitle + right-side slot. */
import type { ReactNode } from "react";

import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

export function PageHeader({
  eyebrow,
  title,
  subtitle,
  meta,
  right,
  className,
}: {
  eyebrow?: ReactNode;
  title: ReactNode;
  subtitle?: ReactNode;
  meta?: ReactNode[];
  right?: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("rounded-[1.75rem] border bg-gradient-to-br from-sky-50 via-white to-slate-50 p-6 md:p-8", className)}>
      <div className="flex flex-wrap items-start justify-between gap-5">
        <div className="min-w-0 max-w-3xl">
          {eyebrow ? (
            <div className="mb-3">
              <Badge variant="secondary" className="rounded-full bg-sky-700/10 px-3 py-1 text-sky-800">
                {eyebrow}
              </Badge>
            </div>
          ) : null}
          <h1 className="text-3xl font-semibold tracking-tight text-balance md:text-4xl">{title}</h1>
          {subtitle ? <div className="mt-3 text-sm leading-6 text-muted-foreground md:text-base">{subtitle}</div> : null}
          {meta && meta.length > 0 ? (
            <div className="mt-4 flex flex-wrap gap-2">
              {meta.map((item, index) => (
                <Badge key={index} variant="outline" className="rounded-full bg-white/75 px-3 py-1 text-xs text-slate-600">
                  {item}
                </Badge>
              ))}
            </div>
          ) : null}
        </div>
        {right ? <div className="flex flex-wrap items-center gap-2">{right}</div> : null}
      </div>
    </div>
  );
}
