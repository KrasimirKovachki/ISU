/** Page header — white card that overlaps the navy band (redesign 2026-10-12).
 * Edited 2026-10-12 by Claude (AI assistant): same props as before, so every page restyles at once. */
import type { ReactNode } from "react";

import { cn } from "@/lib/utils";

export function PageHeader({
  eyebrow,
  icon,
  title,
  subtitle,
  meta,
  right,
  stats,
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
  /** small number tiles on the right (e.g. categories / skaters / countries) */
  stats?: { label: string; value: ReactNode; dark?: boolean }[];
  className?: string;
  titleClassName?: string;
  subtitleClassName?: string;
  metaClassName?: string;
  eyebrowClassName?: string;
}) {
  return (
    <div
      className={cn(
        "min-w-0 rounded-2xl bg-card px-5 py-5 shadow-[0_10px_30px_rgba(11,27,43,0.10)] sm:px-6 sm:py-6 md:px-8 md:py-7",
        className,
      )}
    >
      <div className="flex flex-wrap items-end gap-6">
        <div className="min-w-0 flex-[999_1_300px]">
          {eyebrow ? (
            <div className={cn("eyebrow flex items-center gap-2", eyebrowClassName)}>
              {icon ? <span className="text-brand [&_svg]:h-4 [&_svg]:w-4">{icon}</span> : null}
              {eyebrow}
            </div>
          ) : null}
          <h1 className={cn("mt-2 break-words text-[28px] font-extrabold leading-tight text-balance sm:text-3xl md:text-[40px]", titleClassName)}>{title}</h1>
          {subtitle ? <div className={cn("mt-1.5 text-base text-ink-2", subtitleClassName)}>{subtitle}</div> : null}
          {meta && meta.length > 0 ? (
            <div className="mt-4 flex flex-wrap gap-2">
              {meta.map((item, index) => (
                <span key={index} className={cn("rounded-full bg-background px-3 py-1 text-[13px] text-ink-2", metaClassName)}>
                  {item}
                </span>
              ))}
            </div>
          ) : null}
        </div>
        {stats && stats.length > 0 ? (
          <div className="grid min-w-0 flex-[1_1_260px] gap-2.5 sm:gap-3 md:min-w-[340px]" style={{ gridTemplateColumns: `repeat(${stats.length}, minmax(0, 1fr))` }}>
            {stats.map((s) => (
              <div key={s.label} className={cn("rounded-xl px-3.5 py-3", s.dark ? "bg-navy text-white" : "bg-background")}>
                <div className="num text-[26px] font-semibold leading-tight">{s.value}</div>
                <div className={cn("text-[13px]", s.dark ? "text-[#9FB3C8]" : "text-muted-foreground")}>{s.label}</div>
              </div>
            ))}
          </div>
        ) : null}
        {right ? <div className="flex flex-[1_1_100%] flex-wrap items-center gap-2.5">{right}</div> : null}
      </div>
    </div>
  );
}
