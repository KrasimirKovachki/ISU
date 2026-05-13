import { ArrowRight, Database, Globe2, ShieldCheck } from "lucide-react";
import Link from "next/link";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { getDashboard } from "@/lib/api";
import { formatDate, number } from "@/lib/format";
import { buildMetadata } from "@/lib/site";

export const revalidate = 60;
export const metadata = buildMetadata({
  title: "About",
  description: "What the Skating Data site covers, how the public information product is positioned, and how source-backed competition data is used.",
  path: "/about",
});

export default async function Page() {
  const data = await getDashboard();
  const { overview } = data;

  return (
    <div className="space-y-8">
      <section className="rounded-3xl border bg-gradient-to-br from-sky-50 via-white to-blue-50 p-8 md:p-10">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-sky-700">About The Project</p>
        <h1 className="mt-3 max-w-3xl text-4xl font-semibold tracking-tight text-balance">
          A public figure-skating data site built from verified competition sources.
        </h1>
        <p className="mt-4 max-w-2xl text-base leading-7 text-slate-600">
          This site turns structured results, score sheets, and source evidence into searchable public pages for skaters,
          clubs, competitions, and countries. It is designed as an information product first: transparent coverage,
          explainable metrics, and direct links back to original sources.
        </p>
      </section>

      <section className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base"><Database className="h-4 w-4 text-sky-700" /> Data Coverage</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm text-slate-600">
            <p>{number(overview.events)} events and {number(overview.segment_scores)} scored segments are currently indexed.</p>
            <p>Coverage spans from {formatDate(overview.first_event_date)} to {formatDate(overview.latest_event_date)}.</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base"><Globe2 className="h-4 w-4 text-sky-700" /> Public Structure</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm text-slate-600">
            <p>Each skater, club, event, and country page is generated from the same shared data model.</p>
            <p>The goal is a browsable site, not only an internal dashboard.</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base"><ShieldCheck className="h-4 w-4 text-sky-700" /> Source Transparency</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm text-slate-600">
            <p>Pages are backed by official result sources and imported score evidence whenever available.</p>
            <p>Known gaps and evolving source coverage should remain visible, not hidden.</p>
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">What This Site Should Feel Like</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-6 text-slate-600">
            <p>Editorial and informative rather than app-heavy. Charts support the story, but clear explanations, rankings, and profile pages should lead.</p>
            <p>Search, deep links, and source-backed pages matter more than account flows in the first public version.</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Next Public Layers</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-6 text-slate-600">
            <p>Homepage storytelling, richer skater profiles, club snapshots, event pages, and country overviews should become the main public surface.</p>
            <Link href="/insights" className="inline-flex items-center gap-2 font-medium text-sky-700 hover:text-sky-900">
              See the current analytics layer <ArrowRight className="h-4 w-4" />
            </Link>
          </CardContent>
        </Card>
      </section>
    </div>
  );
}
