export const dynamic = "force-dynamic";
/** Skater profile — KPI tiles, TSS progression chart, recent scores. */
import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ComparisonRadar } from "@/components/comparison-radar";
import { PageHeader } from "@/components/page-header";
import { SkaterElementStats } from "@/components/skater-element-stats";
import { SkaterProgressionChart } from "@/components/skater-progression-chart";
import { getClub, getNationality, getSkater } from "@/lib/api";
import { formatDate, number, points } from "@/lib/format";
import { buildMetadata } from "@/lib/site";
import type { SkaterScoreRow } from "@/lib/types";
import { cn } from "@/lib/utils";

// Edited 2026-10-12 by Claude (AI assistant): redesign — seasons run July to June
function seasonOf(date: string | null): string {
  if (!date) return "Undated";
  const d = new Date(date);
  const y = d.getUTCMonth() >= 6 ? d.getUTCFullYear() : d.getUTCFullYear() - 1;
  return `${y}/${String((y + 1) % 100).padStart(2, "0")}`;
}
const PLACE_TONE: Record<number, string> = { 1: "bg-[#F7EBCB] text-gold-ink", 2: "bg-[#EEF1F4] text-silver-ink", 3: "bg-[#F4E3D6] text-bronze-ink" };
function ordinal(n: number) {
  return `${n}${n % 10 === 1 && n % 100 !== 11 ? "st" : n % 10 === 2 && n % 100 !== 12 ? "nd" : n % 10 === 3 && n % 100 !== 13 ? "rd" : "th"}`;
}

export const revalidate = 60;

export async function generateMetadata({ params }: { params: Promise<{ id: string }> }): Promise<Metadata> {
  const { id } = await params;
  const key = Number(id);
  if (!Number.isFinite(key)) return buildMetadata({ title: "Skater Profile", path: `/skaters/${id}` });
  try {
    const skater = await getSkater(key);
    return buildMetadata({
      title: `${skater.skater_name}`,
      description: `Public skater profile for ${skater.skater_name}${skater.club_name ? ` of ${skater.club_name}` : ""}, with competition history and score progression.`,
      path: `/skaters/${key}`,
    });
  } catch {
    return buildMetadata({ title: "Skater Profile", path: `/skaters/${id}` });
  }
}

export default async function Page({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const key = Number(id);
  if (!Number.isFinite(key)) notFound();

  let skater;
  try {
    skater = await getSkater(key);
  } catch {
    notFound();
  }

  const [club, nation] = await Promise.all([
    skater.club_name ? getClub(skater.club_name).catch(() => null) : Promise.resolve(null),
    skater.nation_code ? getNationality(skater.nation_code).catch(() => null) : Promise.resolve(null),
  ]);

  const relatedClubSkaters = (club?.skater_rows ?? [])
    .filter((row) => row.skater_key !== skater.skater_key)
    .sort((a, b) => (b.best_tss ?? 0) - (a.best_tss ?? 0))
    .slice(0, 4);
  const relatedNationSkaters = (nation?.skaters_list ?? [])
    .filter((row) => row.skater_key !== skater.skater_key)
    .sort((a, b) => (b.best_tss ?? 0) - (a.best_tss ?? 0))
    .slice(0, 4);

  const scores = [...skater.scores].sort((a, b) => (b.event_start_date ?? "").localeCompare(a.event_start_date ?? ""));
  const seasons = new Map<string, SkaterScoreRow[]>();
  for (const s of scores) {
    const k = seasonOf(s.event_start_date);
    seasons.set(k, [...(seasons.get(k) ?? []), s]);
  }
  const topThree = new Set(scores.filter((s) => s.place !== null && s.place <= 3).map((s) => s.event_key)).size;

  // best scored version of each element (by base code), top 5
  const bestByElement = new Map<string, { code: string; score: number; key: number }>();
  for (const e of skater.elements) {
    const code = e.base_element_code ?? e.element_code;
    if (e.panel_score === null || !code) continue;
    const cur = bestByElement.get(code);
    if (!cur || e.panel_score > cur.score) bestByElement.set(code, { code, score: e.panel_score, key: e.segment_score_key });
  }
  const bestElements = [...bestByElement.values()].sort((a, b) => b.score - a.score).slice(0, 5);
  const initials = skater.skater_name.split(/\s+/).map((w) => w[0]).slice(0, 2).join("").toUpperCase();

  return (
    <div className="space-y-5">
      <PageHeader
        eyebrow="Skater"
        title={
          <span className="flex items-center gap-5">
            <span aria-hidden="true" className="hidden h-[84px] w-[84px] shrink-0 items-center justify-center rounded-full bg-ice-2 text-3xl font-extrabold text-brand-strong sm:flex">{initials}</span>
            {skater.skater_name}
          </span>
        }
        subtitle={
          <>
            {skater.nation_code ? <Link href={`/nationalities/${encodeURIComponent(skater.nation_code)}`}>{skater.nation_code}</Link> : null}
            {skater.club_name ? <> · <Link href={`/clubs/${encodeURIComponent(skater.club_name)}`}>{skater.club_name}</Link></> : null}
            {skater.first_seen_date ? <> · competing since {formatDate(skater.first_seen_date)}</> : null}
          </>
        }
        stats={[
          { label: "best total", value: points(skater.best_score_total ?? skater.best_tss), dark: true },
          { label: "competitions", value: number(skater.competitions) },
          { label: "top-3 places", value: number(topThree) },
        ]}
      />

      <div className="flex flex-wrap items-start gap-5">
        <section className="flex min-w-0 flex-[999_1_320px] flex-col gap-5">
          <div className="rounded-2xl bg-card p-5">
            <div className="mb-3 flex flex-wrap items-baseline gap-2.5">
              <h2 className="text-xl font-extrabold">Progress</h2>
              <span className="text-sm text-muted-foreground">total, elements and components per scored segment</span>
            </div>
            <SkaterProgressionChart scores={skater.scores} />
          </div>

          <div className="overflow-hidden rounded-2xl bg-card">
            <div className="border-b px-5 py-4"><h2 className="text-xl font-extrabold">Competitions</h2></div>
            {[...seasons.entries()].map(([season, rows]) => (
              <div key={season}>
                <div className="eyebrow border-t px-5 pb-1 pt-3 first:border-t-0">Season {season}</div>
                {rows.map((s) => (
                  <Link
                    key={s.segment_score_key}
                    href={`/scores/${s.segment_score_key}`}
                    className="flex flex-wrap items-center gap-3 border-t px-5 py-3.5 text-foreground hover:bg-background hover:no-underline"
                  >
                    <span className="num w-[110px] shrink-0 whitespace-nowrap text-sm text-muted-foreground">{formatDate(s.event_start_date)}</span>
                    <span className="min-w-0 flex-[1_1_260px]">
                      <span className="font-bold">{s.event_name ?? "Competition"}</span>
                      <span className="block text-sm text-muted-foreground">{[s.category_name, s.segment_name].filter(Boolean).join(" · ")}</span>
                    </span>
                    {s.place !== null ? (
                      <span className={cn("rounded-full px-2.5 py-1 text-[13px] font-bold", PLACE_TONE[s.place] ?? "bg-background text-ink-2")}>{ordinal(s.place)}</span>
                    ) : null}
                    <span className="num w-16 text-right font-semibold">{points(s.tss)}</span>
                  </Link>
                ))}
              </div>
            ))}
            {scores.length === 0 ? <p className="px-5 py-6 text-sm text-muted-foreground">No scored segments yet.</p> : null}
          </div>
        </section>

        <aside className="flex flex-[1_1_300px] flex-col min-w-0 gap-5">
          {bestElements.length > 0 ? (
            <section className="rounded-2xl bg-card px-5 py-[18px]">
              <h2 className="mb-3 text-xl font-extrabold">Best elements</h2>
              <div className="flex flex-col gap-2.5 text-[15px]">
                {bestElements.map((e) => (
                  <Link key={e.code} href={`/scores/${e.key}`} className="flex justify-between text-foreground hover:text-brand hover:no-underline">
                    <span className="num font-semibold">{e.code}</span>
                    <span className="num">{points(e.score)}</span>
                  </Link>
                ))}
              </div>
            </section>
          ) : null}

          <section className="rounded-2xl bg-card px-5 py-[18px]">
            <h2 className="mb-2 text-xl font-extrabold">Component profile</h2>
            <ComparisonRadar primaryName={skater.skater_name} primaryRows={skater.component_summary ?? []} primarySkaterKey={skater.skater_key} />
          </section>

          {relatedClubSkaters.length > 0 ? (
            <section className="rounded-2xl bg-card px-5 py-[18px]">
              <h2 className="mb-3 text-xl font-extrabold">Same club</h2>
              <div className="flex flex-col gap-2.5 text-[15px]">
                {relatedClubSkaters.map((row) => (
                  <Link key={row.skater_key} href={`/skaters/${row.skater_key}`} className="flex justify-between gap-3">
                    <span>{row.skater_name}</span>
                    <span className="num text-muted-foreground">{points(row.best_tss)}</span>
                  </Link>
                ))}
              </div>
            </section>
          ) : null}

          {relatedNationSkaters.length > 0 ? (
            <section className="rounded-2xl bg-card px-5 py-[18px]">
              <h2 className="mb-3 text-xl font-extrabold">Same country</h2>
              <div className="flex flex-col gap-2.5 text-[15px]">
                {relatedNationSkaters.map((row) => (
                  <Link key={row.skater_key} href={`/skaters/${row.skater_key}`} className="flex justify-between gap-3">
                    <span>{row.skater_name}{row.club_name ? <span className="text-muted-foreground"> · {row.club_name}</span> : null}</span>
                    <span className="num text-muted-foreground">{points(row.best_tss)}</span>
                  </Link>
                ))}
              </div>
            </section>
          ) : null}
        </aside>
      </div>

      <SkaterElementStats elements={skater.elements} />
    </div>
  );
}
