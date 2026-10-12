/** Slim page header — title + optional subtitle + right-side slot. */
import type { ReactNode } from "react";

import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

export function PageHeader({
  eyebrow,
  icon,
  title,
  subtitle,
  meta,
  right,
  className,
  titleClassName,
  subtitleClassName,
  metaClassName,
  eyebrowClassName,
}: {
  eyebrow?: ReactNode;
  icon?: ReactNode;
  title: ReactNode;
  subtitle?: ReactNode;
  meta?: ReactNode[];
  right?: ReactNode;
  className?: string;
  titleClassName?: string;
  subtitleClassName?: string;
  metaClassName?: string;
  eyebrowClassName?: string;
}) {
  return (
    <div
      className={cn(
        "ice-grid ice-sheen overflow-hidden rounded-[1.75rem] border border-slate-900/80 bg-[linear-gradient(145deg,_rgb(2_6_23)_0%,_rgb(8_47_73)_42%,_rgb(15_23_42)_100%)] p-6 shadow-[0_24px_72px_rgba(2,6,23,0.24)] md:p-8",
        className,
      )}
    >
      <div className="flex flex-wrap items-start justify-between gap-5">
        <div className="min-w-0 max-w-3xl">
          {eyebrow ? (
            <div className="mb-3 flex items-center gap-3">
              {icon ? (
                <div className="ice-emblem flex h-11 w-11 items-center justify-center rounded-2xl text-slate-950">
                  {icon}
                </div>
              ) : null}
              <Badge variant="secondary" className={cn("rounded-full border border-white/10 bg-slate-900/80 px-3 py-1 text-sky-200", eyebrowClassName)}>
                {eyebrow}
              </Badge>
            </div>
          ) : null}
          <h1 className={cn("text-3xl font-semibold tracking-tight text-balance text-slate-100 md:text-4xl", titleClassName)}>{title}</h1>
          {subtitle ? <div className={cn("mt-3 text-sm leading-6 text-sky-100/85 md:text-base", subtitleClassName)}>{subtitle}</div> : null}
          {meta && meta.length > 0 ? (
            <div className="mt-4 flex flex-wrap gap-2">
              {meta.map((item, index) => (
                <Badge key={index} variant="outline" className={cn("rounded-full border border-white/10 bg-slate-900/72 px-3 py-1 text-xs text-slate-300", metaClassName)}>
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
