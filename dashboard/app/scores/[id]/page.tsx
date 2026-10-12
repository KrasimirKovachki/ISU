export const dynamic = "force-dynamic";
import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowUpRight } from "lucide-react";

import { PageHeader } from "@/components/page-header";
import { getEvent, getScore, getSkater } from "@/lib/api";
import { describeElement, elementLegendEntries } from "@/lib/element-codes";
import { formatDate, points } from "@/lib/format";
import { buildMetadata } from "@/lib/site";
import { cn } from "@/lib/utils";

export const revalidate = 60;

// Edited 2026-10-12 by Claude (AI assistant): redesign — markers that cost points are orange, notes are grey
const MARKER_TONE: Record<string, string> = {
  "<<": "bg-[#FCE3C8] text-[#7A3E06]", "<": "bg-[#FCE3C8] text-[#7A3E06]", e: "bg-[#FCE3C8] text-[#7A3E06]",
  F: "bg-[#FCE3C8] text-[#7A3E06]", "*": "bg-[#FCE3C8] text-[#7A3E06]", q: "bg-[#FCE3C8] text-[#7A3E06]",
  "!": "bg-background text-ink-2", REP: "bg-background text-ink-2", b: "bg-background text-ink-2", x: "bg-ice-2 text-brand-strong",
};
const BUTTON = "inline-flex h-11 items-center justify-center gap-2 rounded-[10px] px-4 text-[15px] font-semibold hover:no-underline";

/** half-width bar: GOE as a share of the base value (ISU GOE range is ±50 % of base) */
function goeBar(goe: number | null, base: number | null) {
  const g = goe ?? 0;
  const width = Math.min(50, (Math.abs(g) / Math.max(base ?? 0, 0.01)) * 100);
  return { width: `${width}%`, positive: g >= 0 };
}

function judgeAvgTrim(marks: { score: number }[]): { avg: number; high: number | null; low: number | null } {
  const values = marks.map((m) => m.score).filter((v): v is number => typeof v === "number");
  if (values.length < 5) {
    const sum = values.reduce((a, b) => a + b, 0);
    return { avg: values.length ? sum / values.length : 0, high: null, low: null };
  }
  const sorted = [...values].sort((a, b) => a - b);
  const trimmedSum = sorted.slice(1, -1).reduce((a, b) => a + b, 0);
  return { avg: trimmedSum / (values.length - 2), high: sorted[sorted.length - 1], low: sorted[0] };
}

export async function generateMetadata({ params }: { params: Promise<{ id: string }> }): Promise<Metadata> {
  const { id } = await params;
  const key = Number(id);
  if (!Number.isFinite(key)) return buildMetadata({ title: "Score Detail", path: `/scores/${id}` });
  try {
    const score = await getScore(key);
    return buildMetadata({
      title: `${score.skater_name} · ${score.segment_name ?? "Score"}`,
      description: `Source-backed score detail for ${score.skater_name} in ${score.segment_name ?? "a segment"}, including judges, elements, components, and linked evidence.`,
      path: `/scores/${key}`,
    });
  } catch {
    return buildMetadata({ title: "Score Detail", path: `/scores/${id}` });
  }
}

export default async function Page({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const key = Number(id);
  if (!Number.isFinite(key)) notFound();

  let score;
  try {
    score = await getScore(key);
  } catch {
    notFound();
  }

  const [skater, event] = await Promise.all([
    score.skater_key ? getSkater(score.skater_key).catch(() => null) : Promise.resolve(null),
    getEvent(score.event_key).catch(() => null),
  ]);

  const categoryRows = (event?.results ?? []).filter((r) => r.category_name === score.category_name);
  const moreFromSkater = (skater?.scores ?? [])
    .filter((row) => row.segment_score_key !== score.segment_score_key)
    .sort((a, b) => (b.event_start_date ?? "").localeCompare(a.event_start_date ?? ""))
    .slice(0, 4);
  const moreFromCategory = categoryRows
    .filter((row) => row.skater_name !== score.skater_name)
    .sort((a, b) => (a.final_place ?? 999) - (b.final_place ?? 999))
    .slice(0, 5);
  const seasonBest = skater?.best_tss ?? null;
  const pdf = score.evidence.find((e) => (e.document_type ?? "").toLowerCase().includes("pdf") || e.source_url.toLowerCase().endsWith(".pdf"));
  const initials = score.skater_name.split(/\s+/).map((w) => w[0]).slice(0, 2).join("").toUpperCase();
  const legend = elementLegendEntries(score.elements);

  return (
    <div className="space-y-5">
      <nav aria-label="Breadcrumb" className="flex flex-wrap gap-2 text-sm text-[#9FB3C8]">
        <Link href={`/events/${score.event_key}`} className="text-[#C9D6E3]">{score.event_name ?? "Competition"}</Link>
        <span aria-hidden="true">/</span>
        <Link href={`/events/${score.event_key}`} className="text-[#C9D6E3]">{score.category_name ?? "Category"}</Link>
        <span aria-hidden="true">/</span>
        <span className="text-white">{score.skater_name}</span>
      </nav>

      <PageHeader
        eyebrow={`Score sheet · ${score.category_name ?? ""}`}
        title={
          <span className="flex items-center gap-5">
            <span aria-hidden="true" className="hidden h-[72px] w-[72px] shrink-0 items-center justify-center rounded-full bg-ice-2 text-[26px] font-extrabold text-brand-strong sm:flex">{initials}</span>
            {score.skater_name}
          </span>
        }
        subtitle={
          <>
            {[score.nation_code, score.club_name].filter(Boolean).join(" · ")}
            {" · "}
            <Link href={`/events/${score.event_key}`}>{score.event_name ?? "—"}</Link>, {formatDate(score.event_start_date)}
          </>
        }
        right={
          <>
            {(score.category_segments ?? []).length > 0 ? (
              <nav aria-label="Segments" className="flex flex-wrap gap-2">
                {score.category_segments!.map((s) =>
                  s.segment_score_key === score.segment_score_key ? (
                    <span key={s.segment_score_key} aria-current="page" className="inline-flex h-10 items-center rounded-full bg-navy px-4 text-sm font-semibold text-white">
                      {s.segment_name ?? "Segment"} · <span className="num ml-1">{points(s.tss ?? null)}</span>
                    </span>
                  ) : (
                    <Link key={s.segment_score_key} href={`/scores/${s.segment_score_key}`} className="inline-flex h-10 items-center rounded-full border border-input px-4 text-sm text-foreground hover:bg-ice-2 hover:no-underline">
                      {s.segment_name ?? "Segment"} · <span className="num ml-1">{points(s.tss ?? null)}</span>
                    </Link>
                  ),
                )}
              </nav>
            ) : null}
            <span className="ml-auto flex flex-wrap gap-2.5">
              {score.skater_key ? (
                <Link href={`/skaters/${score.skater_key}`} className={cn(BUTTON, "bg-brand text-white hover:bg-brand-strong")}>Skater profile</Link>
              ) : null}
              {pdf ? (
                <a href={pdf.source_url} target="_blank" rel="noopener noreferrer" className={cn(BUTTON, "border border-input text-foreground hover:bg-ice-2")}>
                  <ArrowUpRight className="h-4 w-4" /> Original PDF
                </a>
              ) : score.root_url ? (
                <a href={score.root_url} target="_blank" rel="noopener noreferrer" className={cn(BUTTON, "border border-input text-foreground hover:bg-ice-2")}>
                  <ArrowUpRight className="h-4 w-4" /> Original results
                </a>
              ) : null}
            </span>
          </>
        }
      />

      <div className="grid grid-cols-2 gap-3.5 md:grid-cols-5">
        <div className="rounded-2xl bg-navy p-[18px] text-white">
          <div className="text-[13px] uppercase tracking-[0.08em] text-[#9FB3C8]">Place</div>
          <div className="num text-[34px] font-semibold leading-tight">
            {score.place ?? "—"}
            {categoryRows.length ? <span className="text-base text-[#9FB3C8]"> / {categoryRows.length}</span> : null}
          </div>
        </div>
        {[
          ["Total", score.tss],
          ["Elements", score.tes],
          ["Components", score.pcs],
          ["Deductions", score.deduction],
        ].map(([label, value]) => (
          <div key={label as string} className="rounded-2xl bg-card p-[18px]">
            <div className="text-[13px] uppercase tracking-[0.08em] text-muted-foreground">{label}</div>
            <div className="num text-[34px] font-semibold leading-tight">{points(value as number | null)}</div>
          </div>
        ))}
      </div>

      <div className="flex flex-wrap items-start gap-5">
        <section className="min-w-0 flex-[999_1_320px] overflow-hidden rounded-2xl bg-card">
          <div className="flex flex-wrap items-baseline gap-3 border-b px-5 py-4">
            <h2 className="text-xl font-extrabold">Elements</h2>
            <span className="text-sm text-muted-foreground">
              base value <span className="num">{points(score.base_value_total)}</span> · {score.judges.length} judges
            </span>
          </div>
          {score.elements.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full min-w-[640px] border-collapse text-[15px]">
                <thead>
                  <tr className="text-left text-xs uppercase tracking-[0.08em] text-muted-foreground">
                    <th scope="col" className="w-8 px-5 py-2.5 font-medium">#</th>
                    <th scope="col" className="px-2 py-2.5 font-medium">Element</th>
                    <th scope="col" className="px-2 py-2.5 text-right font-medium">Base</th>
                    <th scope="col" className="w-[190px] px-2 py-2.5 font-medium">GOE</th>
                    <th scope="col" className="px-2 py-2.5 text-center font-medium">Judges</th>
                    <th scope="col" className="px-5 py-2.5 text-right font-medium">Score</th>
                  </tr>
                </thead>
                <tbody className="num">
                  {score.elements.map((e) => {
                    const trim = judgeAvgTrim(e.judge_marks);
                    const bar = goeBar(e.goe, e.base_value);
                    const warn = e.markers.some((m) => (MARKER_TONE[m] ?? "").includes("FCE3C8"));
                    return (
                      <tr key={e.element_score_key} className={cn("border-t", warn && "bg-[#FFF7EE]")}>
                        <td className="px-5 py-3 text-muted-foreground">{e.element_no}</td>
                        <td className="px-2 py-3 font-semibold" title={e.raw_element ?? undefined}>
                          {e.element_code}{" "}
                          {e.markers.map((m) => (
                            <span key={m} className={cn("ml-1 inline-block rounded-full px-2 py-0.5 font-sans text-xs font-bold", MARKER_TONE[m] ?? "bg-background text-ink-2")}>{m}</span>
                          ))}
                          <span className="block font-sans text-[13px] font-normal text-muted-foreground">{describeElement(e.element_code) ?? ""}</span>
                        </td>
                        <td className="px-2 py-3 text-right">{points(e.base_value)}</td>
                        <td className="px-2 py-3">
                          <div className="flex items-center gap-2">
                            <div className="relative h-2 flex-1 rounded bg-[#EDF2F7]" aria-hidden="true">
                              <div
                                className={cn("absolute h-2", bar.positive ? "left-1/2 rounded-r bg-[#1F8A5B]" : "right-1/2 rounded-l bg-[#C2541B]")}
                                style={{ width: bar.width }}
                              />
                            </div>
                            <span className={cn("w-12 text-right", (e.goe ?? 0) > 0 ? "text-good" : (e.goe ?? 0) < 0 ? "text-bad" : "")}>
                              {(e.goe ?? 0) > 0 ? "+" : ""}{points(e.goe)}
                            </span>
                          </div>
                        </td>
                        <td className="px-2 py-3 text-center text-ink-2">
                          {score.judges.map((j, i) => {
                            const v = e.judge_marks.find((m) => m.judge_number === j.judge_number)?.score;
                            const trimmed = v !== undefined && trim.high !== null && (v === trim.high || v === trim.low);
                            return (
                              <span key={j.judge_number} className={cn(trimmed && "italic text-muted-foreground")} title={trimmed ? "highest/lowest, not counted" : `Judge ${j.judge_number}`}>
                                {i > 0 ? " · " : ""}{v ?? "—"}
                              </span>
                            );
                          })}
                        </td>
                        <td className="px-5 py-3 text-right font-semibold">{points(e.panel_score)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="px-5 py-8 text-center text-sm text-muted-foreground">No element details were published for this segment.</p>
          )}
          {legend.length > 0 ? (
            <div className="flex flex-wrap gap-x-4 gap-y-1 border-t px-5 py-3 text-xs text-muted-foreground">
              {legend.map(({ code, description }) => (
                <span key={code}><span className="num font-semibold text-foreground">{code}</span> · {description}</span>
              ))}
            </div>
          ) : null}
        </section>

        <aside className="flex flex-[1_1_300px] flex-col min-w-0 gap-5">
          {score.components.length > 0 ? (
            <section className="rounded-2xl bg-card px-5 py-[18px]">
              <h2 className="mb-3.5 text-xl font-extrabold">Components</h2>
              <div className="flex flex-col gap-3.5 text-[15px]">
                {score.components.map((c) => {
                  return (
                    <div key={c.component_score_key} title={c.judge_marks.map((m) => `J${m.judge_number}: ${Number(m.score).toFixed(2)}`).join("  ")}>
                      <div className="flex justify-between gap-3">
                        <span>{c.component_name}</span>
                        <span className="num font-semibold">{points(c.score)}</span>
                      </div>
                      <div className="mt-1.5 h-2 rounded bg-[#EDF2F7]" aria-hidden="true">
                        <div className="h-2 rounded bg-brand" style={{ width: `${Math.min(100, Math.max(0, c.score * 10))}%` }} />
                      </div>
                      <div className="num mt-1 text-xs text-muted-foreground">
                        × {Number(c.factor).toFixed(2)} · judges {c.judge_marks.map((m) => Number(m.score).toFixed(2)).join(" · ")}
                      </div>
                    </div>
                  );
                })}
              </div>
            </section>
          ) : null}

          {score.judges.length > 0 ? (
            <section className="rounded-2xl bg-card px-5 py-[18px]">
              <h2 className="mb-3 text-xl font-extrabold">Panel</h2>
              <div className="flex flex-col gap-2.5 text-[15px]">
                {score.judges.map((j) => (
                  <div key={j.judge_number} className="flex justify-between gap-3">
                    <span>Judge {j.judge_number}</span>
                    <span className="text-right text-muted-foreground">{[j.name, j.nation_code].filter(Boolean).join(" · ") || "—"}</span>
                  </div>
                ))}
              </div>
            </section>
          ) : null}

          {seasonBest !== null && score.tss !== null ? (
            <section className="rounded-2xl bg-ice-2 px-5 py-[18px] text-brand-strong">
              <div className="text-[13px] font-bold uppercase tracking-[0.1em]">Compared with the best</div>
              <div className="num mt-1.5 text-[26px] font-semibold">
                {score.tss - seasonBest >= 0 ? "+" : ""}{points(score.tss - seasonBest)}
              </div>
              <div className="text-sm">Best segment score <span className="num">{points(seasonBest)}</span></div>
            </section>
          ) : null}
        </aside>
      </div>

      <div className="grid gap-5 lg:grid-cols-2">
        <section className="rounded-2xl bg-card p-5">
          <h2 className="text-xl font-extrabold">Also in {score.category_name ?? "this category"}</h2>
          <div className="mt-3 flex flex-col">
            {moreFromCategory.length > 0 ? moreFromCategory.map((r) => (
              <Link
                key={r.competition_result_key}
                href={r.segments?.length ? `/scores/${r.segments[0].segment_score_key}` : r.skater_key ? `/skaters/${r.skater_key}` : `/events/${score.event_key}`}
                className="flex items-center gap-3 border-t py-3 text-foreground first:border-t-0 hover:no-underline hover:text-brand"
              >
                <span className="num w-8 text-muted-foreground">{r.final_place ?? "—"}</span>
                <span className="min-w-0 flex-1 font-semibold">{r.skater_name}</span>
                <span className="num">{points(r.points)}</span>
              </Link>
            )) : <p className="text-sm text-muted-foreground">No other skaters in this category.</p>}
          </div>
        </section>
        <section className="rounded-2xl bg-card p-5">
          <h2 className="text-xl font-extrabold">More from {score.skater_name.split(/\s+/)[0]}</h2>
          <div className="mt-3 flex flex-col">
            {moreFromSkater.length > 0 ? moreFromSkater.map((row) => (
              <Link key={row.segment_score_key} href={`/scores/${row.segment_score_key}`} className="flex items-center gap-3 border-t py-3 text-foreground first:border-t-0 hover:no-underline hover:text-brand">
                <span className="num w-[110px] shrink-0 whitespace-nowrap text-sm text-muted-foreground">{formatDate(row.event_start_date)}</span>
                <span className="min-w-0 flex-1"><span className="font-semibold">{row.event_name ?? "Competition"}</span><span className="block text-sm text-muted-foreground">{row.segment_name}</span></span>
                <span className="num">{points(row.tss)}</span>
              </Link>
            )) : <p className="text-sm text-muted-foreground">No other scored segments yet.</p>}
          </div>
        </section>
      </div>

      {score.evidence.length > 0 ? (
        <section className="rounded-2xl bg-card p-5">
          <h2 className="text-xl font-extrabold">Sources</h2>
          <p className="mt-1 text-sm text-muted-foreground">The published documents this score sheet was read from.</p>
          <ul className="mt-3 space-y-1.5 text-sm">
            {score.evidence.map((e) => (
              <li key={e.source_document_key} className="flex items-center gap-2">
                <span className="rounded-full bg-background px-2 py-0.5 text-xs uppercase text-ink-2">{e.document_type ?? "doc"}</span>
                <a href={e.source_url} target="_blank" rel="noopener noreferrer" className="truncate">{e.source_url}</a>
              </li>
            ))}
          </ul>
        </section>
      ) : null}
    </div>
  );
}
