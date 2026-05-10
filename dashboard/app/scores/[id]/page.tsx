import Link from "next/link";
import { notFound } from "next/navigation";

import { KpiCard } from "@/components/kpi-card";
import { PageHeader } from "@/components/page-header";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { getScore } from "@/lib/api";
import { describeElement, elementLegendEntries } from "@/lib/element-codes";
import { nationDisplay, points } from "@/lib/format";

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

  return (
    <div className="space-y-6">
      <PageHeader
        title={`${score.skater_name} · ${score.segment_name ?? ""}`}
        subtitle={
          <>
            <Link href={`/events/${score.event_key}`} className="hover:underline">
              {score.event_name ?? "—"}
            </Link>
            {" · "}{score.category_name ?? "—"}
            {" · "}{score.event_start_date ?? "—"}
            {" · "}{nationDisplay(score.nation_code)}
          </>
        }
        right={
          <Badge variant={score.score_source === "pdf_score_summary" ? "secondary" : "outline"}>
            {score.score_source === "pdf_score_summary" ? "PDF score summary" : "HTML fallback"}
          </Badge>
        }
      />

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard label="TSS" value={points(score.tss)} />
        <KpiCard label="TES" value={points(score.tes)} />
        <KpiCard label="PCS" value={points(score.pcs)} />
        <KpiCard label="Deduction" value={points(score.deduction)} />
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

      <Card>
        <CardHeader><CardTitle className="text-base">Evidence</CardTitle></CardHeader>
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
    </div>
  );
}
