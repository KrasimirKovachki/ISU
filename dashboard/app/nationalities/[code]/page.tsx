import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowRight, Globe2, Medal, Users } from "lucide-react";

import { KpiCard } from "@/components/kpi-card";
import { PageHeader } from "@/components/page-header";
import { MedalRibbonIcon } from "@/components/skating-icons";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { getNationality } from "@/lib/api";
import { formatDate, nationDisplay, number, points } from "@/lib/format";
import { buildMetadata } from "@/lib/site";

export const revalidate = 60;

export async function generateMetadata({ params }: { params: Promise<{ code: string }> }): Promise<Metadata> {
  const { code } = await params;
  const decoded = decodeURIComponent(code);
  try {
    const nation = await getNationality(decoded);
    return buildMetadata({
      title: `${nation.nation_code} Country Page`,
      description: `Country-level figure-skating coverage for ${nation.nation_code}, including visible skaters, events, and best scores.`,
      path: `/nationalities/${encodeURIComponent(nation.nation_code)}`,
    });
  } catch {
    return buildMetadata({ title: "Country Page", path: `/nationalities/${encodeURIComponent(decoded)}` });
  }
}

export default async function Page({ params }: { params: Promise<{ code: string }> }) {
  const { code } = await params;
  const decoded = decodeURIComponent(code);
  let nation;
  try {
    nation = await getNationality(decoded);
  } catch {
    notFound();
  }

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Country Page"
        icon={<MedalRibbonIcon className="h-5 w-5" />}
        title={nationDisplay(nation.nation_code)}
        subtitle={`${formatDate(nation.first_seen_date)} → ${formatDate(nation.latest_seen_date)}`}
        meta={[
          `${number(nation.skaters)} skaters`,
          `${number(nation.events)} events`,
          `${number(nation.segment_scores)} segment scores`,
        ]}
      />

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard label="Skaters" value={nation.skaters} />
        <KpiCard label="Events" value={nation.events} />
        <KpiCard label="Segment scores" value={nation.segment_scores} />
        <KpiCard label="Best TSS" value={points(nation.best_tss)} hint={`PCS ${points(nation.best_pcs)}`} />
      </section>

      <section className="grid gap-4 lg:grid-cols-[0.92fr_1.08fr]">
        <Card className="bg-slate-950 text-slate-50">
          <CardHeader>
            <CardTitle className="text-base text-white">Country-level public view</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-6 text-slate-300">
            <p>Country pages should help visitors understand the breadth of representation in the dataset without needing to start from a specific competition.</p>
            <p>They work best when they summarize visible depth and then link down into public skater profiles and clubs.</p>
          </CardContent>
        </Card>
        <div className="grid gap-4 sm:grid-cols-3">
          <Card>
            <CardContent className="p-4">
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Country</p>
              <p className="mt-2 text-base font-semibold"><Globe2 className="mr-2 inline h-4 w-4 text-sky-700" />{nationDisplay(nation.nation_code)}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Visible skaters</p>
              <p className="mt-2 text-base font-semibold"><Users className="mr-2 inline h-4 w-4 text-sky-700" />{number(nation.skaters_list?.length ?? 0)}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Top total</p>
              <p className="mt-2 text-base font-semibold"><Medal className="mr-2 inline h-4 w-4 text-amber-600" />{points(nation.best_tss)}</p>
            </CardContent>
          </Card>
        </div>
      </section>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Visible skaters ({nation.skaters_list?.length ?? 0})</CardTitle>
          <p className="text-sm text-muted-foreground">
            Public skater profiles linked from this country page, ordered by the current backend list.
          </p>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Skater</TableHead>
                <TableHead>Club</TableHead>
                <TableHead className="text-right">Comps</TableHead>
                <TableHead className="text-right">Best TSS</TableHead>
                <TableHead>Latest</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(nation.skaters_list ?? []).slice(0, 200).map((s) => (
                <TableRow key={s.skater_key}>
                  <TableCell className="font-medium">
                    <Link href={`/skaters/${s.skater_key}`} className="hover:underline">{s.skater_name}</Link>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {s.club_name ? (
                      <Link href={`/clubs/${encodeURIComponent(s.club_name)}`} className="hover:underline">{s.club_name}</Link>
                    ) : (
                      "—"
                    )}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{s.competitions}</TableCell>
                  <TableCell className="text-right tabular-nums">{points(s.best_tss)}</TableCell>
                  <TableCell className="text-muted-foreground">{formatDate(s.latest_seen_date)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <div className="flex justify-end">
        <Link
          href="/nationalities"
          className="inline-flex items-center gap-2 rounded-full border bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50"
        >
          Back to countries <ArrowRight className="h-4 w-4" />
        </Link>
      </div>
    </div>
  );
}
