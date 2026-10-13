export const dynamic = "force-dynamic";
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
      <section className="ice-grid ice-sheen rounded-3xl border border-border bg-card p-8 text-foreground shadow-[0_10px_30px_rgba(11,27,43,0.10)] md:p-10">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-brand">About The Project</p>
        <h1 className="mt-3 max-w-3xl text-4xl font-semibold tracking-tight text-balance text-foreground">
          A public figure-skating data site built from verified competition sources.
        </h1>
        <p className="mt-4 max-w-2xl text-base leading-7 text-ink-2">
          This site turns structured results, score sheets, and source evidence into searchable public pages for skaters,
          clubs, competitions, and countries. It is designed as an information product first: transparent coverage,
          explainable metrics, and direct links back to original sources.
        </p>
      </section>

      <section className="grid gap-4 md:grid-cols-3">
        <Card className="border-border bg-card text-foreground">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="flex items-center gap-2 text-base text-foreground"><Database className="h-4 w-4 text-brand" /> Data Coverage</CardTitle>
            <Link href="/events" className="text-sm font-medium text-brand hover:text-brand">Open events</Link>
          </CardHeader>
          <CardContent className="space-y-2 text-sm text-ink-2">
            <p>{number(overview.events)} events and {number(overview.segment_scores)} scored segments are currently indexed.</p>
            <p>Coverage spans from {formatDate(overview.first_event_date)} to {formatDate(overview.latest_event_date)}.</p>
          </CardContent>
        </Card>
        <Card className="border-border bg-card text-foreground">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="flex items-center gap-2 text-base text-foreground"><Globe2 className="h-4 w-4 text-brand" /> Public Structure</CardTitle>
            <Link href="/skaters" className="text-sm font-medium text-brand hover:text-brand">Open profiles</Link>
          </CardHeader>
          <CardContent className="space-y-2 text-sm text-ink-2">
            <p>Each skater, club, event, and country page is generated from the same shared data model.</p>
            <p>The goal is a browsable site, not only an internal dashboard.</p>
          </CardContent>
        </Card>
        <Card className="border-border bg-card text-foreground">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="flex items-center gap-2 text-base text-foreground"><ShieldCheck className="h-4 w-4 text-brand" /> Source Transparency</CardTitle>
            <Link href="/insights" className="text-sm font-medium text-brand hover:text-brand">Open insights</Link>
          </CardHeader>
          <CardContent className="space-y-2 text-sm text-ink-2">
            <p>Pages are backed by official result sources and imported score evidence whenever available.</p>
            <p>Known gaps and evolving source coverage should remain visible, not hidden.</p>
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-4 md:grid-cols-2">
        <Card className="border-border bg-card text-foreground">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base text-foreground">What This Site Should Feel Like</CardTitle>
            <Link href="/" className="text-sm font-medium text-brand hover:text-brand">Open homepage</Link>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-6 text-ink-2">
            <p>Editorial and informative rather than app-heavy. Charts support the story, but clear explanations, rankings, and profile pages should lead.</p>
            <p>Search, deep links, and source-backed pages matter more than account flows in the first public version.</p>
          </CardContent>
        </Card>
        <Card className="border-border bg-card text-foreground">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base text-foreground">Next Public Layers</CardTitle>
            <Link href="/clubs" className="text-sm font-medium text-brand hover:text-brand">Open clubs</Link>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-6 text-ink-2">
            <p>Homepage storytelling, richer skater profiles, club snapshots, event pages, and country overviews should become the main public surface.</p>
            <Link href="/insights" className="inline-flex items-center gap-2 font-medium text-brand hover:text-brand">
              See the current analytics layer <ArrowRight className="h-4 w-4" />
            </Link>
          </CardContent>
        </Card>
      </section>
    </div>
  );
}
