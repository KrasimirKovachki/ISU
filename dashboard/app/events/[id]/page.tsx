export const dynamic = "force-dynamic";
/** Event page — redesign 2026-10-12 (owner-approved mockup): header card, category list, podium + results of the
 * selected category; a skater's name opens their score sheet in that category.
 * Edited 2026-10-12 by Claude (AI assistant). */
import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowUpRight } from "lucide-react";

import { PageHeader } from "@/components/page-header";
import { getEvent, listEvents } from "@/lib/api";
import { formatDate, number, points } from "@/lib/format";
import { buildMetadata } from "@/lib/site";
import type { EventResultRow } from "@/lib/types";
import { cn } from "@/lib/utils";

export const revalidate = 60;

const STREAM_LABEL: Record<string, string> = {
  ISU: "ISU", ISUCS: "ISU Challenger", NonISU: "Non-ISU", national_club: "National", club: "Club",
  club_amateur: "Recreational", club_test: "Club test", club_control: "Club control", FRP: "National (ROU)", DP: "Development",
};

const MEDAL = [
  { bar: "border-t-gold", ink: "text-gold-ink", label: "1st" },
  { bar: "border-t-silver", ink: "text-silver-ink", label: "2nd" },
  { bar: "border-t-bronze", ink: "text-bronze-ink", label: "3rd" },
];

// segments in skating order, unknown codes after
const SEGMENT_ORDER = ["QR", "PP", "SP", "RD", "PD", "FS", "FD", "FP"];
function segmentRank(code: string) {
  const i = SEGMENT_ORDER.indexOf(code.toUpperCase());
  return i < 0 ? 99 : i;
}

/** "CUBS II. GIRLS (1.7.2016 - 30.6.2017)" -> name + the bracketed birth-date range on its own line */
function splitCategory(name: string): { name: string; detail: string | null } {
  const m = name.match(/^(.*?)\s*\(([^)]*\d[^)]*)\)\s*$/);
  return m ? { name: m[1], detail: m[2] } : { name, detail: null };
}

function resultHref(r: EventResultRow): string | null {
  if (r.segments?.length) return `/scores/${r.segments[0].segment_score_key}`;
  if (r.skater_key) return `/skaters/${r.skater_key}`;
  return null;
}

export async function generateMetadata({ params }: { params: Promise<{ id: string }> }): Promise<Metadata> {
  const { id } = await params;
  const key = Number(id);
  if (!Number.isFinite(key)) return buildMetadata({ title: "Competition", path: `/events/${id}` });
  try {
    const event = await getEvent(key);
    return buildMetadata({
      title: `${event.event_name ?? "Competition"}`,
      description: `Results of ${event.event_name ?? "this competition"} by category, with score sheets and the original result pages.`,
      path: `/events/${key}`,
    });
  } catch {
    return buildMetadata({ title: "Competition", path: `/events/${id}` });
  }
}

export default async function Page({
  params,
  searchParams,
}: {
  params: Promise<{ id: string }>;
  searchParams: Promise<{ category?: string }>;
}) {
  const { id } = await params;
  const { category } = await searchParams;
  const key = Number(id);
  if (!Number.isFinite(key)) notFound();

  let event;
  try {
    event = await getEvent(key);
  } catch {
    notFound();
  }

  // categories in import order (= the event's own schedule order)
  const byCategory = new Map<number, { name: string; rows: EventResultRow[] }>();
  for (const r of event.results) {
    const entry = byCategory.get(r.category_key) ?? { name: r.category_name ?? "(no category)", rows: [] };
    entry.rows.push(r);
    byCategory.set(r.category_key, entry);
  }
  const categories = [...byCategory.entries()].sort(([a], [b]) => a - b);
  const selectedKey = categories.find(([k]) => String(k) === category)?.[0] ?? categories[0]?.[0];
  const selected = selectedKey !== undefined ? byCategory.get(selectedKey) : undefined;
  const rows = [...(selected?.rows ?? [])].sort((a, b) => (a.final_place ?? 999) - (b.final_place ?? 999));
  const podium = rows.filter((r) => r.final_place !== null && r.final_place <= 3).slice(0, 3);
  const segmentCodes = [...new Set(rows.flatMap((r) => Object.keys(r.segment_places ?? {})))].sort((a, b) => segmentRank(a) - segmentRank(b));
  const hasClubs = rows.some((r) => r.club_name);
  const selectedName = splitCategory(selected?.name ?? "Results");
  const countries = new Set(event.results.map((r) => r.nation_code).filter(Boolean)).size;

  const eventYear = event.event_start_date ? new Date(event.event_start_date).getUTCFullYear() : undefined;
  const nearby = eventYear
    ? await listEvents({ from_year: eventYear, to_year: eventYear, limit: 250 }).catch(() => ({ items: [] }))
    : { items: [] };
  const relatedEvents = nearby.items
    .filter((row) => row.event_key !== event.event_key)
    .sort((a, b) => (b.event_start_date ?? "").localeCompare(a.event_start_date ?? ""))
    .slice(0, 4);

  const stream = event.competition_stream ? STREAM_LABEL[event.competition_stream] ?? event.competition_stream : null;

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow={[stream, event.date_range ?? formatDate(event.event_start_date)].filter(Boolean).join(" · ")}
        title={event.event_name ?? "(unnamed event)"}
        subtitle={[event.location, event.venue].filter(Boolean).join(" · ") || null}
        stats={[
          { label: "categories", value: number(categories.length) },
          { label: "skaters", value: number(event.results.length) },
          { label: "countries", value: number(countries) },
        ]}
        right={
          <>
            {/* Edited 2026-10-13 by Claude (AI assistant): the competition's parts (ISU / Non-ISU / national championship) */}
            {(event.parts?.length ?? 0) > 1 ? (
              <nav aria-label="Parts of this competition" className="mr-auto flex flex-wrap gap-2">
                {event.parts!.map((p) =>
                  p.current ? (
                    <span key={p.event_key} aria-current="page" className="inline-flex h-11 items-center rounded-full bg-navy px-4 text-sm font-semibold text-white">{p.part}</span>
                  ) : (
                    <Link key={p.event_key} href={`/events/${p.event_key}`} className="inline-flex h-11 items-center rounded-full border border-input px-4 text-sm text-foreground hover:bg-ice-2 hover:no-underline">{p.part}</Link>
                  ),
                )}
              </nav>
            ) : null}
            {event.root_url ? (
              <a
                href={event.root_url}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex h-11 items-center gap-2 rounded-[10px] border border-input px-4 text-sm font-semibold text-foreground hover:bg-ice-2 hover:no-underline"
              >
                <ArrowUpRight className="h-4 w-4" /> Original results
              </a>
            ) : null}
            <Link
              href="/events"
              className="inline-flex h-11 items-center rounded-[10px] border border-input px-4 text-sm font-semibold text-foreground hover:bg-ice-2 hover:no-underline"
            >
              All competitions
            </Link>
          </>
        }
      />

      <div className="flex flex-wrap items-start gap-6">
        {/* phones: categories as a sideways row of chips (mockup); larger screens: the list on the left */}
        <nav aria-label="Categories" className="-mx-4 flex w-[calc(100%+2rem)] gap-2 overflow-x-auto px-4 pb-1 lg:hidden">
          {categories.map(([k, c]) => (
            <Link
              key={k}
              href={`/events/${event.event_key}?category=${k}`}
              scroll={false}
              aria-current={k === selectedKey ? "page" : undefined}
              className={cn(
                "inline-flex h-10 shrink-0 items-center rounded-full px-3.5 text-sm hover:no-underline",
                k === selectedKey ? "bg-navy font-bold text-white" : "border border-input bg-card text-foreground",
              )}
            >
              {splitCategory(c.name).name}
            </Link>
          ))}
        </nav>
        <aside aria-label="Categories" className="hidden flex-[1_1_260px] rounded-2xl bg-card p-4 lg:block">
          <div className="eyebrow px-2 pb-2.5">Categories</div>
          <nav className="flex flex-col gap-0.5 text-[15px]">
            {categories.map(([k, c]) => (
              <Link
                key={k}
                href={`/events/${event.event_key}?category=${k}`}
                scroll={false}
                aria-current={k === selectedKey ? "page" : undefined}
                className={cn(
                  "flex justify-between gap-3 rounded-[10px] px-3 py-2.5 text-foreground hover:bg-background hover:no-underline",
                  k === selectedKey && "bg-ice-2 font-bold text-brand-strong hover:bg-ice-2",
                )}
              >
                <span className="min-w-0">
                  {splitCategory(c.name).name}
                  {splitCategory(c.name).detail ? (
                    <span className={cn("block text-[13px] font-normal", k === selectedKey ? "text-brand-strong/80" : "text-muted-foreground")}>{splitCategory(c.name).detail}</span>
                  ) : null}
                </span>
                <span className={cn("num", k !== selectedKey && "text-muted-foreground")}>{c.rows.length}</span>
              </Link>
            ))}
          </nav>
        </aside>

        <section className="flex min-w-0 flex-[999_1_320px] flex-col gap-5">
          <div className="flex flex-wrap items-baseline gap-3">
            <h2 className="text-[28px] font-extrabold">{selectedName.name}</h2>
            <span className="text-[15px] text-muted-foreground">
              {selectedName.detail ? `born ${selectedName.detail} · ` : ""}
              {rows.length} skater{rows.length === 1 ? "" : "s"}
              {segmentCodes.length ? ` · ${segmentCodes.join(" + ")}` : ""}
            </span>
          </div>

          {podium.length > 0 ? (
            <div className="grid gap-3.5 sm:grid-cols-3">
              {podium.map((r, i) => {
                const href = resultHref(r);
                const body = (
                  <>
                    <span className={cn("num text-sm font-semibold", MEDAL[i].ink)}>{MEDAL[i].label}</span>
                    <span className="text-lg font-bold">{r.skater_name}</span>
                    <span className="text-sm text-muted-foreground">{[r.nation_code, r.club_name].filter(Boolean).join(" · ") || "—"}</span>
                    <span className="num text-[30px] font-semibold">{points(r.points)}</span>
                  </>
                );
                const cls = cn("flex flex-col gap-1.5 rounded-2xl border-t-[5px] bg-card p-[18px] text-foreground", MEDAL[i].bar);
                return href ? (
                  <Link key={r.competition_result_key} href={href} className={cn(cls, "hover:no-underline hover:shadow-md")}>{body}</Link>
                ) : (
                  <div key={r.competition_result_key} className={cls}>{body}</div>
                );
              })}
            </div>
          ) : null}

          <div className="overflow-hidden rounded-2xl bg-card">
            <div className="flex flex-wrap items-center gap-2 border-b px-[18px] py-3.5">
              <span className="font-bold">Final result</span>
              <span className="text-sm text-muted-foreground">Select a name for the full score sheet</span>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full min-w-[480px] border-collapse text-[15px]">
                <thead>
                  <tr className="text-left text-xs uppercase tracking-[0.08em] text-muted-foreground">
                    <th scope="col" className="w-14 px-[18px] py-3 text-right font-medium">Pl.</th>
                    <th scope="col" className="px-2 py-3 font-medium">Skater</th>
                    {hasClubs ? <th scope="col" className="px-2 py-3 font-medium">Club</th> : null}
                    {segmentCodes.map((s) => (
                      <th key={s} scope="col" className="px-2 py-3 text-right font-medium">{s}</th>
                    ))}
                    <th scope="col" className="px-[18px] py-3 text-right font-medium">Total</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r) => {
                    const href = resultHref(r);
                    const top = r.final_place !== null && r.final_place <= 3;
                    return (
                      <tr key={r.competition_result_key} className="border-t">
                        <td className={cn("num px-[18px] py-3.5 text-right", top && "font-semibold")}>{r.final_place ?? "—"}</td>
                        <td className="px-2 py-3.5">
                          {href ? (
                            <Link href={href} className={cn("text-brand", top ? "font-bold" : "font-semibold")}>{r.skater_name}</Link>
                          ) : (
                            <span className="font-semibold">{r.skater_name}</span>
                          )}{" "}
                          <span className="text-[13px] text-muted-foreground">{r.nation_code ?? ""}</span>
                        </td>
                        {hasClubs ? (
                          <td className="px-2 py-3.5 text-ink-2">
                            {r.club_name ? (
                              <Link href={`/clubs/${encodeURIComponent(r.club_name)}`} className="text-ink-2 hover:text-brand">{r.club_name}</Link>
                            ) : "—"}
                          </td>
                        ) : null}
                        {segmentCodes.map((s) => (
                          <td key={s} className="num px-2 py-3.5 text-right text-ink-2">{r.segment_places?.[s] ?? "—"}</td>
                        ))}
                        <td className={cn("num px-[18px] py-3.5 text-right", top && "font-semibold")}>{points(r.points)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      </div>

      {relatedEvents.length > 0 ? (
        <section className="rounded-2xl bg-card p-5">
          <h2 className="text-xl font-extrabold">More competitions in {eventYear}</h2>
          <div className="mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            {relatedEvents.map((row) => (
              <Link
                key={row.event_key}
                href={`/events/${row.event_key}`}
                className="rounded-xl bg-background p-4 text-foreground hover:bg-ice-2 hover:no-underline"
              >
                <span className="block font-bold">{row.event_name ?? "Competition"}</span>
                <span className="mt-1 block text-sm text-muted-foreground">
                  {formatDate(row.event_start_date)} · {row.location ?? "—"}
                </span>
              </Link>
            ))}
          </div>
        </section>
      ) : null}
    </div>
  );
}
