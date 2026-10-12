export const dynamic = "force-dynamic";
import Link from "next/link";
import { ArrowRight, Building2, CalendarRange, Globe2, Layers3, LineChart, Search, Sparkles } from "lucide-react";

import { CompetitionMixDonut } from "@/components/competition-mix-donut";
import { KpiCard } from "@/components/kpi-card";
import { NationalitiesBarChart } from "@/components/nationalities-bar-chart";
import { IceRinkIcon, MedalRibbonIcon, ScoreSheetIcon, SkateSparkIcon, SkatingBladeIcon } from "@/components/skating-icons";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { getDashboard, listEvents } from "@/lib/api";
import { formatDate, number, points } from "@/lib/format";
import { buildMetadata } from "@/lib/site";

export const revalidate = 30;
export const metadata = buildMetadata({
  title: "Home",
  description: "Homepage for an ice-skating club website with skaters, club pages, competition coverage, and skating stats built from official result sources.",
  path: "/",
});

const EXPLORE_LINKS = [
  {
    href: "/skaters",
    title: "Skater Profiles",
    body: "Search public profiles with score progression, element history, nationality, and club links.",
    Icon: SkatingBladeIcon,
  },
  {
    href: "/clubs",
    title: "Club Pages",
    body: "Browse active clubs, their skaters, event presence, and best performances.",
    Icon: IceRinkIcon,
  },
  {
    href: "/events",
    title: "Competition Pages",
    body: "Follow events, participants, results, and source documents from official protocols.",
    Icon: ScoreSheetIcon,
  },
  {
    href: "/insights",
    title: "Club Stats",
    body: "Follow score trends, country coverage, competition mix, and club-level performance views.",
    Icon: MedalRibbonIcon,
  },
];

export default async function Page() {
  const [dashboard, eventsResponse] = await Promise.all([
    getDashboard(),
    listEvents({ limit: 6 }),
  ]);

  const { overview, clubs, nationalities, competition_mix } = dashboard;
  const latestEvents = eventsResponse.items
    .filter((event) => event.event_start_date)
    .sort((a, b) => (a.event_start_date! < b.event_start_date! ? 1 : -1))
    .slice(0, 4);

  const coverageNote = `${formatDate(overview.first_event_date)} to ${formatDate(overview.latest_event_date)}`;
  const topNation = nationalities[0];
  const topClub = clubs[0];

  return (
    <div className="space-y-10">
      <section className="ice-grid ice-sheen overflow-hidden rounded-[2rem] border border-slate-900/80 bg-[linear-gradient(145deg,_rgb(2_6_23)_0%,_rgb(8_47_73)_42%,_rgb(15_23_42)_100%)] shadow-[0_30px_90px_rgba(2,6,23,0.26)]">
        <div className="grid gap-8 px-6 py-10 md:px-10 md:py-12 lg:grid-cols-[1.25fr_0.85fr]">
          <div>
            <div className="flex flex-wrap items-center gap-3">
              <Badge variant="secondary" className="rounded-full border border-sky-400/20 bg-slate-950/85 px-3 py-1 text-sky-200">
                Ice Skating Club Website
              </Badge>
              <span className="inline-flex items-center gap-2 rounded-full border border-slate-700/80 bg-slate-950/70 px-3 py-1 text-xs text-slate-300">
                <SkateSparkIcon className="h-3.5 w-3.5 text-sky-300" />
                club-first website with source-backed skating stats
              </span>
            </div>
            <h1 className="mt-4 max-w-3xl text-4xl font-semibold tracking-tight text-white md:text-5xl text-balance">
              Club profiles, skating results, and competition stats.
            </h1>
            <p className="mt-4 max-w-2xl text-base leading-7 text-sky-100/82 md:text-lg">
              Browse skaters, clubs, competitions, countries, and performance data built from official result sources.
              The site should feel like a public-facing ice-skating club website first, with stats supporting the story.
            </p>
            <div className="mt-5 flex flex-wrap gap-2 text-xs text-slate-200">
              <span className="inline-flex items-center gap-2 rounded-full border border-slate-700/80 bg-slate-950/72 px-3 py-1"><SkatingBladeIcon className="h-3.5 w-3.5 text-sky-300" /> public profiles</span>
              <span className="inline-flex items-center gap-2 rounded-full border border-slate-700/80 bg-slate-950/72 px-3 py-1"><ScoreSheetIcon className="h-3.5 w-3.5 text-sky-300" /> source-backed score pages</span>
              <span className="inline-flex items-center gap-2 rounded-full border border-slate-700/80 bg-slate-950/72 px-3 py-1"><IceRinkIcon className="h-3.5 w-3.5 text-sky-300" /> club and event browse</span>
            </div>
            <div className="mt-6 flex flex-wrap gap-3">
              <Link
                href="/skaters"
                className="inline-flex items-center gap-2 rounded-full border border-sky-400/25 bg-sky-500/18 px-5 py-3 text-sm font-medium text-sky-100 transition hover:bg-sky-500/24"
              >
                Explore skaters <ArrowRight className="h-4 w-4" />
              </Link>
              <Link
                href="/insights"
                className="inline-flex items-center gap-2 rounded-full border border-slate-700/80 bg-slate-950/72 px-5 py-3 text-sm font-medium text-slate-100 transition hover:bg-slate-900/88"
              >
                Open club stats <LineChart className="h-4 w-4" />
              </Link>
            </div>
            <div className="mt-8 grid gap-3 sm:grid-cols-3">
              <KpiCard
                label="Coverage"
                value={coverageNote}
                hint="Browse the latest indexed competitions"
                href="/events"
                ctaLabel="Open events"
                className="border-slate-700/80 bg-slate-950/72 text-white [&_div]:text-inherit [&_.text-muted-foreground]:text-slate-400"
              />
              <KpiCard
                label="Events Indexed"
                value={number(overview.events)}
                hint="Open the competition archive"
                href="/events"
                ctaLabel="View archive"
                className="border-slate-700/80 bg-slate-950/72 text-white [&_div]:text-inherit [&_.text-muted-foreground]:text-slate-400"
              />
              <KpiCard
                label="Scored Skaters"
                value={number(overview.skaters)}
                hint="Search public skater profiles"
                href="/skaters"
                ctaLabel="Open skaters"
                className="border-slate-700/80 bg-slate-950/72 text-white [&_div]:text-inherit [&_.text-muted-foreground]:text-slate-400"
              />
            </div>
          </div>

          <div className="grid gap-4">
            <Card className="border-slate-700/80 bg-slate-950/72 text-sky-50 shadow-sm backdrop-blur-sm">
              <CardHeader>
                <CardTitle className="text-sm font-medium text-sky-100 flex items-center gap-2">
                  <SkatingBladeIcon className="h-4 w-4 text-sky-300" />
                  What the club site should communicate
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3 text-sm leading-6 text-sky-100/76">
                <p>Who is covered, how much data exists, and where a visitor should go next.</p>
                <p>Profiles, competition pages, and club pages should lead, with skating stats as supporting evidence.</p>
              </CardContent>
            </Card>
            <Card className="border-slate-700/80 bg-slate-950/80 text-sky-50 shadow-sm backdrop-blur-sm">
              <CardHeader className="flex flex-row items-center justify-between">
                <CardTitle className="text-sm font-medium text-sky-100 flex items-center gap-2">
                  <MedalRibbonIcon className="h-4 w-4 text-sky-300" />
                  Club and stats anchors
                </CardTitle>
                <Link href="/insights" className="text-xs font-medium text-sky-200 hover:text-white">
                  Open insights
                </Link>
              </CardHeader>
              <CardContent className="grid gap-3 text-sm">
                <div className="flex items-start gap-3">
                  <Globe2 className="mt-0.5 h-4 w-4 shrink-0 text-sky-300" />
                  <p>
                    {number(nationalities.length)} visible countries in the dashboard slice, led by{" "}
                    {topNation ? (
                      <Link href={`/nationalities/${encodeURIComponent(topNation.nation_code)}`} className="font-medium text-white underline-offset-4 hover:underline">
                        {topNation.nation_code}
                      </Link>
                    ) : (
                      "—"
                    )}
                    .
                  </p>
                </div>
                <div className="flex items-start gap-3">
                  <Building2 className="mt-0.5 h-4 w-4 shrink-0 text-sky-300" />
                  <p>
                    <Link href="/clubs" className="font-medium text-white underline-offset-4 hover:underline">
                      {number(overview.scoring_clubs)} scoring clubs
                    </Link>{" "}
                    with public-facing club pages and skater rosters to build on.
                  </p>
                </div>
                <div className="flex items-start gap-3">
                  <Layers3 className="mt-0.5 h-4 w-4 shrink-0 text-sky-300" />
                  <p>
                    <Link href="/insights" className="font-medium text-white underline-offset-4 hover:underline">
                      {number(overview.element_scores)} parsed elements
                    </Link>{" "}
                    available for richer club, coach, and skater storytelling.
                  </p>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </section>

      <section className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {EXPLORE_LINKS.map(({ href, title, body, Icon }) => (
          <Link
            key={href}
            href={href}
            className="rounded-3xl border border-slate-900/80 bg-slate-950 p-5 text-slate-200 shadow-sm"
          >
            <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-slate-900 text-sky-300 ring-1 ring-white/8">
              <Icon className="h-5 w-5" />
            </div>
            <h2 className="mt-4 text-lg font-semibold tracking-tight text-slate-100">{title}</h2>
            <p className="mt-2 text-sm leading-6 text-slate-400">{body}</p>
            <span className="mt-4 inline-flex items-center gap-2 text-sm font-medium text-sky-300">
              Open <ArrowRight className="h-4 w-4" />
            </span>
          </Link>
        ))}
      </section>

      <section className="grid gap-4 lg:grid-cols-[1.15fr_0.85fr]">
        <Card className="border-slate-900/80 bg-slate-950 text-slate-200">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base flex items-center gap-2 text-slate-100"><ScoreSheetIcon className="h-4 w-4 text-sky-300" />Latest competition coverage</CardTitle>
            <Link href="/events" className="text-sm font-medium text-sky-300 hover:text-sky-200">Open events</Link>
          </CardHeader>
          <CardContent className="grid gap-3">
            {latestEvents.map((event) => (
              <Link
                key={event.event_key}
                href={`/events/${event.event_key}`}
                className="flex items-start justify-between gap-4 rounded-2xl border border-white/8 bg-slate-900/70 p-4"
              >
                <div>
                  <p className="font-medium text-slate-100">{event.event_name ?? "Unnamed event"}</p>
                  <p className="mt-1 text-sm text-slate-400">
                    {event.location ?? "Location pending"} · {formatDate(event.event_start_date)}
                  </p>
                </div>
                <span className="shrink-0 text-sm font-medium text-sky-300">View</span>
              </Link>
            ))}
          </CardContent>
        </Card>

        <Card className="border-slate-900/80 bg-slate-950 text-slate-200">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base flex items-center gap-2 text-slate-100"><SkateSparkIcon className="h-4 w-4 text-sky-300" />Why the dashboard is not enough</CardTitle>
            <Link href="/about" className="text-sm font-medium text-sky-300 hover:text-sky-200">Open brief</Link>
          </CardHeader>
          <CardContent className="space-y-4 text-sm leading-6 text-slate-400">
            <p>A raw dashboard is useful for exploration, but a public site needs entry points, explanations, and page-level context.</p>
            <p>That means homepage narrative, entity pages, source transparency, and stable links that people can share and discover.</p>
            <Link href="/about" className="inline-flex items-center gap-2 font-medium text-sky-300 hover:text-sky-200">
              Read project positioning <ArrowRight className="h-4 w-4" />
            </Link>
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <Card className="border-slate-900/80 bg-slate-950 text-slate-200">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base flex items-center gap-2 text-slate-100"><Globe2 className="h-4 w-4 text-sky-300" />Coverage by country</CardTitle>
            <Link href="/nationalities" className="text-sm font-medium text-sky-300 hover:text-sky-200">Open countries</Link>
          </CardHeader>
          <CardContent>
            <NationalitiesBarChart
              rows={nationalities}
              take={10}
              hrefBase="/nationalities/"
            />
          </CardContent>
        </Card>
        <Card className="border-slate-900/80 bg-slate-950 text-slate-200">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base flex items-center gap-2 text-slate-100"><MedalRibbonIcon className="h-4 w-4 text-sky-300" />Competition mix</CardTitle>
            <Link href="/insights" className="text-sm font-medium text-sky-300 hover:text-sky-200">Open insights</Link>
          </CardHeader>
          <CardContent>
            <CompetitionMixDonut rows={competition_mix} href="/insights#event-mix" />
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-4 lg:grid-cols-[0.85fr_1.15fr]">
        <Card className="bg-slate-950 text-slate-50">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base text-white flex items-center gap-2"><ScoreSheetIcon className="h-4 w-4 text-sky-300" />Data confidence</CardTitle>
            <Link href="/about" className="text-sm font-medium text-sky-200 hover:text-white">
              Open about
            </Link>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-6 text-slate-300">
            <p>The public site should stay explicit about source-backed coverage and about what is still incomplete.</p>
            <p>Official result links, parser coverage, and last-seen dates should become part of the experience rather than hidden implementation details.</p>
          </CardContent>
        </Card>

        <Card className="border-slate-900/80 bg-slate-950 text-slate-200">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base flex items-center gap-2 text-slate-100"><IceRinkIcon className="h-4 w-4 text-sky-300" />Featured club signal</CardTitle>
            {topClub ? (
              <Link href={`/clubs/${encodeURIComponent(topClub.club_name)}`} className="text-sm font-medium text-sky-300 hover:text-sky-200">
                Open club
              </Link>
            ) : null}
          </CardHeader>
          <CardContent>
            {topClub ? (
              <Link href={`/clubs/${encodeURIComponent(topClub.club_name)}`} className="block rounded-2xl bg-slate-900/70 p-4">
                <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4 rounded-2xl">
                <div>
                  <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Club</p>
                  <p className="mt-1 text-base font-semibold text-slate-100">{topClub.club_name}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Skaters</p>
                  <p className="mt-1 text-base font-semibold text-slate-100">{number(topClub.skaters)}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Events</p>
                  <p className="mt-1 text-base font-semibold text-slate-100">{number(topClub.events)}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Best TSS</p>
                  <p className="mt-1 text-base font-semibold text-slate-100">{points(topClub.best_tss)}</p>
                </div>
                </div>
              </Link>
            ) : (
              <p className="text-sm text-muted-foreground">Club summary will appear here once club rows are available.</p>
            )}
          </CardContent>
        </Card>
      </section>

      <section className="rounded-3xl border border-slate-900/80 bg-slate-950 p-6 text-slate-200 md:p-8">
        <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Next Layer</p>
            <h2 className="mt-2 text-2xl font-semibold tracking-tight text-slate-100">Build profile pages and competition pages as the main public surface.</h2>
          </div>
          <div className="flex flex-wrap gap-3">
            <Link href="/events" className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-slate-900 px-4 py-2 text-sm font-medium text-slate-200">
              Browse events <CalendarRange className="h-4 w-4" />
            </Link>
            <Link href="/skaters" className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-slate-900 px-4 py-2 text-sm font-medium text-slate-200">
              Search skaters <Search className="h-4 w-4" />
            </Link>
            <Link href="/about" className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-slate-900 px-4 py-2 text-sm font-medium text-slate-200">
              Project brief <Sparkles className="h-4 w-4" />
            </Link>
          </div>
        </div>
      </section>
    </div>
  );
}
