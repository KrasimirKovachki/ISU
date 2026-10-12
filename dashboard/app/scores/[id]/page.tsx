export const dynamic = "force-dynamic";
import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowRight, Gavel, ShieldCheck, Trophy } from "lucide-react";

import { KpiCard } from "@/components/kpi-card";
import { PageHeader } from "@/components/page-header";
import { ScoreSheetIcon } from "@/components/skating-icons";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { getEvent, getScore, getSkater } from "@/lib/api";
import { describeElement, elementLegendEntries } from "@/lib/element-codes";
import { formatDate, nationDisplay, points } from "@/lib/format";
import { buildMetadata } from "@/lib/site";

export const revalidate = 60;

const MARKER_VARIANT: Record<string, "destructive" | "secondary" | "outline"> = {
  "<<": "destructive", "<": "destructive", e: "destructive", F: "destructive", "*": "destructive",
  "!": "secondary", q: "secondary", REP: "secondary", b: "secondary",
  x: "outline",
};

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

  const moreFromSkater = (skater?.scores ?? [])
    .filter((row) => row.segment_score_key !== score.segment_score_key)
    .sort((a, b) => (b.event_start_date ?? "").localeCompare(a.event_start_date ?? ""))
    .slice(0, 3);

  const moreFromEvent = (event?.results ?? [])
    .filter((row) => row.skater_name !== score.skater_name)
    .sort((a, b) => (a.final_place ?? 999) - (b.final_place ?? 999))
    .slice(0, 4);

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Score Detail"
        icon={<ScoreSheetIcon className="h-5 w-5" />}
        title={`${score.skater_name} · ${score.segment_name ?? ""}`}
        subtitle={
          <>
            <Link href={`/events/${score.event_key}`} className="hover:underline">
              {score.event_name ?? "—"}
            </Link>
            {" · "}{score.category_name ?? "—"}
            {" · "}{formatDate(score.event_start_date)}
            {" · "}{nationDisplay(score.nation_code)}
          </>
        }
        meta={[
          score.club_name ?? "club pending",
          `${score.judges.length} judges`,
          `${score.elements.length} elements`,
          `${score.components.length} components`,
        ]}
        right={
          <>
            {score.skater_key ? (
              <Link
                href={`/skaters/${score.skater_key}`}
                className="surface-action inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-medium transition"
              >
                Skater page <ArrowRight className="h-4 w-4" />
              </Link>
            ) : null}
            <Link
              href={`/events/${score.event_key}`}
              className="surface-action inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-medium transition"
            >
              Event page <ArrowRight className="h-4 w-4" />
            </Link>
            {/* Edited 2026-10-12 by Claude (AI assistant): the organizer's own results page */}
            {score.root_url ? (
              <a
                href={score.root_url}
                target="_blank"
                rel="noopener noreferrer"
                className="surface-action inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-medium transition"
              >
                Original results <ArrowRight className="h-4 w-4" />
              </a>
            ) : null}
            <Badge variant={score.score_source === "pdf_score_summary" ? "secondary" : "outline"}>
              {score.score_source === "pdf_score_summary" ? "PDF score summary" : "HTML fallback"}
            </Badge>
          </>
        }
      />

      {/* Edited 2026-10-12 by Claude (AI assistant): switch between this skater's segments in the category */}
      {(score.category_segments?.length ?? 0) > 1 ? (
        <nav className="flex flex-wrap gap-2" aria-label="Segments">
          {score.category_segments!.map((s) =>
            s.segment_score_key === score.segment_score_key ? (
              <span key={s.segment_score_key} className="rounded-full border border-sky-400 bg-sky-400/15 px-4 py-1.5 text-sm font-medium">
                {s.segment_name ?? "Segment"} · {points(s.tss ?? null)}
              </span>
            ) : (
              <Link
                key={s.segment_score_key}
                href={`/scores/${s.segment_score_key}`}
                className="surface-action rounded-full border px-4 py-1.5 text-sm transition hover:underline"
              >
                {s.segment_name ?? "Segment"} · {points(s.tss ?? null)}
              </Link>
            ),
          )}
        </nav>
      ) : null}

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard label="TSS" value={points(score.tss)} />
        <KpiCard label="TES" value={points(score.tes)} />
        <KpiCard label="PCS" value={points(score.pcs)} />
        <KpiCard label="Deduction" value={points(score.deduction)} />
      </section>

      <section className="grid gap-4 lg:grid-cols-[0.92fr_1.08fr]">
        <Card className="bg-slate-950 text-slate-50">
          <CardHeader>
            <CardTitle className="text-base text-white">Why this page matters</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-6 text-slate-300">
            <p>This is the closest thing to a source-backed evidence sheet in the public site: one score, one segment, one skater, with parsed judging detail where available.</p>
            <p>It should feel trustworthy and readable even for visitors who are not living inside skating scoring terminology every day.</p>
          </CardContent>
        </Card>
        <div className="grid gap-4 sm:grid-cols-3">
          <Card>
            <CardContent className="p-4">
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Skater</p>
              <p className="mt-2 text-base font-semibold">{score.skater_name}</p>
              <p className="mt-1 text-sm text-muted-foreground">{nationDisplay(score.nation_code)}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Event</p>
              <p className="mt-2 text-base font-semibold">{score.event_name ?? "—"}</p>
              <p className="mt-1 text-sm text-muted-foreground">{formatDate(score.event_start_date)}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Segment</p>
              <p className="mt-2 text-base font-semibold">{score.segment_name ?? "—"}</p>
              <p className="mt-1 text-sm text-muted-foreground">{score.category_name ?? "—"}</p>
            </CardContent>
          </Card>
        </div>
      </section>

      <Card>
        <CardHeader><CardTitle className="text-base">Judges</CardTitle></CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-12">#</TableHead>
                <TableHead>Name</TableHead>
                <TableHead>Nation</TableHead>
                <TableHead>Function</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {score.judges.map((j) => (
                <TableRow key={j.judge_number}>
                  <TableCell className="tabular-nums">J{j.judge_number}</TableCell>
                  <TableCell>{j.name ?? "—"}</TableCell>
                  <TableCell>{nationDisplay(j.nation_code)}</TableCell>
                  <TableCell className="text-muted-foreground">{j.function_text ?? "—"}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {score.elements.length > 0 ? (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">
              Elements <span className="text-xs font-normal text-muted-foreground">· {score.elements.length}</span>
            </CardTitle>
            <p className="text-sm text-muted-foreground">
              Parsed technical content with markers, bonus, panel score, and per-judge GOE where available.
            </p>
          </CardHeader>
          <CardContent className="p-0">
            <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-10">#</TableHead>
                  <TableHead>Element</TableHead>
                  <TableHead>Markers</TableHead>
                  <TableHead className="text-right">Base</TableHead>
                  <TableHead className="text-right">GOE</TableHead>
                  <TableHead className="text-right">Bonus</TableHead>
                  <TableHead className="text-right">Score</TableHead>
                  {score.judges.map((j) => (
                    <TableHead key={j.judge_number} className="text-center w-12">J{j.judge_number}</TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {score.elements.map((e) => {
                  const trim = judgeAvgTrim(e.judge_marks);
                  const byJudge = new Map(e.judge_marks.map((m) => [m.judge_number, m.score]));
                  return (
                    <TableRow key={e.element_score_key}>
                      <TableCell className="tabular-nums text-muted-foreground">{e.element_no}</TableCell>
                      <TableCell className="font-medium" title={[describeElement(e.element_code), e.raw_element].filter(Boolean).join(" — ")}>{e.element_code}</TableCell>
                      <TableCell>
                        {e.markers.length > 0 ? (
                          <div className="flex gap-1">
                            {e.markers.map((m) => (
                              <Badge key={m} variant={MARKER_VARIANT[m] ?? "outline"} className="px-1.5 py-0 text-[10px]">{m}</Badge>
                            ))}
                          </div>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">{points(e.base_value)}</TableCell>
                      <TableCell className={`text-right tabular-nums ${(e.goe ?? 0) > 0 ? "text-emerald-600" : (e.goe ?? 0) < 0 ? "text-red-600" : ""}`}>{points(e.goe)}</TableCell>
                      <TableCell className="text-right tabular-nums">{e.bonus ? `+${points(e.bonus)}` : "—"}</TableCell>
                      <TableCell className="text-right tabular-nums font-medium">{points(e.panel_score)}</TableCell>
                      {score.judges.map((j) => {
                        const v = byJudge.get(j.judge_number);
                        const isTrimmed = v !== undefined && (v === trim.high || v === trim.low) && trim.high !== null;
                        return (
                          <TableCell
                            key={j.judge_number}
                            className={`text-center tabular-nums text-xs ${isTrimmed ? "text-muted-foreground italic" : ""}`}
                            title={isTrimmed ? "Trimmed (highest/lowest excluded from panel score)" : ""}
                          >
                            {v ?? "—"}
                          </TableCell>
                        );
                      })}
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
            </div>
            {(() => {
              const legend = elementLegendEntries(score.elements);
              if (legend.length === 0) return null;
              return (
                <div className="px-4 pt-3 pb-4 text-xs flex flex-wrap gap-x-4 gap-y-1 text-muted-foreground border-t">
                  {legend.map(({ code, description }) => (
                    <span key={code}>
                      <span className="font-mono font-medium text-foreground">{code}</span>
                      <span> · {description}</span>
                    </span>
                  ))}
                </div>
              );
            })()}
          </CardContent>
        </Card>
      ) : null}

      {score.components.length > 0 ? (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Components</CardTitle>
            <p className="text-sm text-muted-foreground">
              Program component scoring by factor, final panel score, and individual judge marks.
            </p>
          </CardHeader>
          <CardContent className="p-0 overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Component</TableHead>
                  <TableHead className="text-right">Factor</TableHead>
                  <TableHead className="text-right">Score</TableHead>
                  {score.judges.map((j) => (
                    <TableHead key={j.judge_number} className="text-center w-14">J{j.judge_number}</TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {score.components.map((c) => {
                  const trim = judgeAvgTrim(c.judge_marks);
                  const byJudge = new Map(c.judge_marks.map((m) => [m.judge_number, m.score]));
                  return (
                    <TableRow key={c.component_score_key}>
                      <TableCell className="font-medium">{c.component_name}</TableCell>
                      <TableCell className="text-right tabular-nums">× {Number(c.factor).toFixed(2)}</TableCell>
                      <TableCell className="text-right tabular-nums font-medium">{points(c.score)}</TableCell>
                      {score.judges.map((j) => {
                        const v = byJudge.get(j.judge_number);
                        const isTrimmed = v !== undefined && (v === trim.high || v === trim.low) && trim.high !== null;
                        return (
                          <TableCell
                            key={j.judge_number}
                            className={`text-center tabular-nums text-xs ${isTrimmed ? "text-muted-foreground italic" : ""}`}
                            title={isTrimmed ? "Trimmed (highest/lowest excluded from panel score)" : ""}
                          >
                            {v !== undefined ? Number(v).toFixed(2) : "—"}
                          </TableCell>
                        );
                      })}
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      ) : null}

      {score.elements.length === 0 && score.components.length === 0 ? (
        <Card><CardContent className="text-sm text-muted-foreground py-8 text-center">
          No parsed PDF elements/components for this segment. Score summary + evidence below.
        </CardContent></Card>
      ) : null}

      <section className="grid gap-4 lg:grid-cols-[0.95fr_1.05fr]">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Continue from this score</CardTitle>
            <p className="text-sm text-muted-foreground">
              A score page should guide visitors toward the skater timeline and the wider competition, not trap them on one protocol row.
            </p>
          </CardHeader>
          <CardContent className="space-y-3">
            {moreFromSkater.length > 0 ? (
              moreFromSkater.map((row) => (
                <div key={row.segment_score_key} className="surface-inset rounded-2xl border p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <Link href={`/scores/${row.segment_score_key}`} className="font-medium text-slate-100 hover:underline">
                        {row.segment_name ?? "Segment detail"}
                      </Link>
                      <p className="text-sm text-slate-400">
                        {row.event_name ?? "Event page"} · {formatDate(row.event_start_date)}
                      </p>
                    </div>
                    <span className="text-sm font-medium text-slate-200">{points(row.tss)}</span>
                  </div>
                </div>
              ))
            ) : (
              <p className="text-sm text-slate-500">No additional scored segments are visible for this skater.</p>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">More from this competition</CardTitle>
            <p className="text-sm text-muted-foreground">
              Event-linked exploration helps the evidence page stay connected to the public results archive.
            </p>
          </CardHeader>
          <CardContent className="space-y-3">
            {moreFromEvent.length > 0 ? (
              moreFromEvent.map((row) => (
                <div key={row.competition_result_key} className="surface-inset rounded-2xl border p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <p className="font-medium text-slate-100">
                        {row.skater_key ? (
                          <Link href={`/skaters/${row.skater_key}`} className="hover:underline">
                            {row.skater_name}
                          </Link>
                        ) : (
                          row.skater_name
                        )}
                      </p>
                      <p className="text-sm text-slate-400">
                        {row.category_name ?? "Category pending"} · place {row.final_place ?? "—"}
                      </p>
                    </div>
                    <span className="text-sm font-medium text-slate-200">{points(row.points)}</span>
                  </div>
                </div>
              ))
            ) : (
              <p className="text-sm text-slate-500">No related competition entries are available from the event detail.</p>
            )}
          </CardContent>
        </Card>
      </section>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Evidence</CardTitle>
          <p className="text-sm text-muted-foreground">
            Linked source documents that support this imported score view.
          </p>
        </CardHeader>
        <CardContent>
          {score.evidence.length === 0 ? (
            <div className="text-sm text-muted-foreground">No source documents linked.</div>
          ) : (
            <ul className="space-y-1 text-sm">
              {score.evidence.map((e) => (
                <li key={e.source_document_key} className="flex items-center gap-2">
                  <Badge variant="outline">{e.document_type ?? "doc"}</Badge>
                  <a href={e.source_url} target="_blank" rel="noreferrer" className="truncate hover:underline text-primary">
                    {e.source_url}
                  </a>
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>

      <Card className="border-slate-800 bg-slate-950">
        <CardHeader>
          <CardTitle className="text-base text-slate-100">How to read this page</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 text-sm leading-6 text-slate-400 md:grid-cols-3">
          <div>
            <p className="font-medium text-slate-100"><Trophy className="mr-2 inline h-4 w-4 text-amber-400" />Totals first</p>
            <p className="mt-2">The KPI strip gives the quickest reading of the segment outcome: total, technical, components, and deductions.</p>
          </div>
          <div>
            <p className="font-medium text-slate-100"><Gavel className="mr-2 inline h-4 w-4 text-sky-300" />Judge detail</p>
            <p className="mt-2">Judge tables and trimmed marks help explain how panel scoring produced the visible totals.</p>
          </div>
          <div>
            <p className="font-medium text-slate-100"><ShieldCheck className="mr-2 inline h-4 w-4 text-emerald-300" />Source traceability</p>
            <p className="mt-2">Evidence links keep the public page anchored to original published competition documents.</p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
