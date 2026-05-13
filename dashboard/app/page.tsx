import Link from "next/link";
import { ArrowRight, Building2, CalendarRange, Globe2, Layers3, LineChart, Search, Sparkles } from "lucide-react";

import { CompetitionMixDonut } from "@/components/competition-mix-donut";
import { NationalitiesBarChart } from "@/components/nationalities-bar-chart";
import { IceRinkIcon, MedalRibbonIcon, ScoreSheetIcon, SkateSparkIcon, SkatingBladeIcon } from "@/components/skating-icons";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { getDashboard, listEvents } from "@/lib/api";
import { formatDate, number, points } from "@/lib/format";

export const revalidate = 30;

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
    title: "Analytics",
    body: "See the structured dashboard views as one layer inside a broader information site.",
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
      <section className="ice-grid overflow-hidden rounded-[2rem] border bg-gradient-to-br from-sky-100 via-white to-blue-50">
        <div className="grid gap-8 px-6 py-10 md:px-10 md:py-12 lg:grid-cols-[1.25fr_0.85fr]">
          <div>
            <div className="flex flex-wrap items-center gap-3">
              <Badge variant="secondary" className="rounded-full bg-sky-700/10 px-3 py-1 text-sky-800">
                Public Figure-Skating Data
              </Badge>
              <span className="inline-flex items-center gap-2 rounded-full border bg-white/80 px-3 py-1 text-xs text-slate-600">
                <SkateSparkIcon className="h-3.5 w-3.5 text-sky-700" />
                inspired by public sports-data products, adapted for your stack
              </span>
            </div>
            <h1 className="mt-4 max-w-3xl text-4xl font-semibold tracking-tight text-slate-950 md:text-5xl text-balance">
              Turn the dashboards into a real information website.
            </h1>
            <p className="mt-4 max-w-2xl text-base leading-7 text-slate-600 md:text-lg">
              Browse skaters, clubs, competitions, countries, and analytics built from official result sources. This
              front page is meant to explain the dataset, not just expose internal widgets.
            </p>
            <div className="mt-5 flex flex-wrap gap-2 text-xs text-slate-600">
              <span className="inline-flex items-center gap-2 rounded-full bg-white/70 px-3 py-1"><SkatingBladeIcon className="h-3.5 w-3.5 text-sky-700" /> public profiles</span>
              <span className="inline-flex items-center gap-2 rounded-full bg-white/70 px-3 py-1"><ScoreSheetIcon className="h-3.5 w-3.5 text-sky-700" /> source-backed score pages</span>
              <span className="inline-flex items-center gap-2 rounded-full bg-white/70 px-3 py-1"><IceRinkIcon className="h-3.5 w-3.5 text-sky-700" /> club and event browse</span>
            </div>
            <div className="mt-6 flex flex-wrap gap-3">
              <Link
                href="/skaters"
                className="inline-flex items-center gap-2 rounded-full bg-sky-800 px-5 py-3 text-sm font-medium text-white transition hover:bg-sky-900"
              >
                Explore skaters <ArrowRight className="h-4 w-4" />
              </Link>
              <Link
                href="/insights"
                className="inline-flex items-center gap-2 rounded-full border bg-white px-5 py-3 text-sm font-medium text-slate-700 transition hover:bg-slate-50"
              >
                Open analytics layer <LineChart className="h-4 w-4" />
              </Link>
            </div>
            <div className="mt-8 grid gap-3 sm:grid-cols-3">
              <div className="rounded-2xl border bg-white/75 p-4">
                <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Coverage</p>
                <p className="mt-2 text-sm font-medium text-slate-900">{coverageNote}</p>
              </div>
              <div className="rounded-2xl border bg-white/75 p-4">
                <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Events Indexed</p>
                <p className="mt-2 text-sm font-medium text-slate-900">{number(overview.events)}</p>
              </div>
              <div className="rounded-2xl border bg-white/75 p-4">
                <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Scored Skaters</p>
                <p className="mt-2 text-sm font-medium text-slate-900">{number(overview.skaters)}</p>
              </div>
            </div>
          </div>

          <div className="grid gap-4">
            <Card className="ice-sheen border-white/70 bg-white/80 shadow-sm">
              <CardHeader>
                <CardTitle className="text-sm font-medium text-slate-600 flex items-center gap-2">
                  <SkatingBladeIcon className="h-4 w-4 text-sky-700" />
                  What the site should communicate
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3 text-sm leading-6 text-slate-600">
                <p>Who is covered, how much data exists, and where a visitor should go next.</p>
                <p>Profiles and competition pages should feel like public reference pages, with analytics as supporting evidence.</p>
              </CardContent>
            </Card>
            <Card className="border-sky-200 bg-sky-950 text-sky-50 shadow-sm">
              <CardHeader>
                <CardTitle className="text-sm font-medium text-sky-100 flex items-center gap-2">
                  <MedalRibbonIcon className="h-4 w-4 text-sky-300" />
                  Current public anchors
                </CardTitle>
              </CardHeader>
              <CardContent className="grid gap-3 text-sm">
                <div className="flex items-start gap-3">
                  <Globe2 className="mt-0.5 h-4 w-4 shrink-0 text-sky-300" />
                  <p>{number(nationalities.length)} visible countries in the dashboard slice, led by {topNation?.nation_code ?? "—"}.</p>
                </div>
                <div className="flex items-start gap-3">
                  <Building2 className="mt-0.5 h-4 w-4 shrink-0 text-sky-300" />
                  <p>{number(overview.scoring_clubs)} scoring clubs with profile-style pages to build on.</p>
                </div>
                <div className="flex items-start gap-3">
                  <Layers3 className="mt-0.5 h-4 w-4 shrink-0 text-sky-300" />
                  <p>{number(overview.element_scores)} parsed elements available for deeper skater and club storytelling.</p>
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
            className="group ice-sheen rounded-3xl border bg-card p-5 transition hover:-translate-y-0.5 hover:shadow-md"
          >
            <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-sky-100 text-sky-800">
              <Icon className="h-5 w-5" />
            </div>
            <h2 className="mt-4 text-lg font-semibold tracking-tight">{title}</h2>
            <p className="mt-2 text-sm leading-6 text-muted-foreground">{body}</p>
            <span className="mt-4 inline-flex items-center gap-2 text-sm font-medium text-sky-800">
              Open <ArrowRight className="h-4 w-4 transition group-hover:translate-x-0.5" />
            </span>
          </Link>
        ))}
      </section>

      <section className="grid gap-4 lg:grid-cols-[1.15fr_0.85fr]">
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2"><ScoreSheetIcon className="h-4 w-4 text-sky-700" />Latest competition coverage</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-3">
            {latestEvents.map((event) => (
              <Link
                key={event.event_key}
                href={`/events/${event.event_key}`}
                className="flex items-start justify-between gap-4 rounded-2xl border p-4 transition hover:bg-slate-50"
              >
                <div>
                  <p className="font-medium text-slate-900">{event.event_name ?? "Unnamed event"}</p>
                  <p className="mt-1 text-sm text-slate-500">
                    {event.location ?? "Location pending"} · {formatDate(event.event_start_date)}
                  </p>
                </div>
                <span className="shrink-0 text-sm font-medium text-sky-800">View</span>
              </Link>
            ))}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2"><SkateSparkIcon className="h-4 w-4 text-sky-700" />Why the dashboard is not enough</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4 text-sm leading-6 text-slate-600">
            <p>A raw dashboard is useful for exploration, but a public site needs entry points, explanations, and page-level context.</p>
            <p>That means homepage narrative, entity pages, source transparency, and stable links that people can share and discover.</p>
            <Link href="/about" className="inline-flex items-center gap-2 font-medium text-sky-800 hover:text-sky-950">
              Read project positioning <ArrowRight className="h-4 w-4" />
            </Link>
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2"><Globe2 className="h-4 w-4 text-sky-700" />Coverage by country</CardTitle>
          </CardHeader>
          <CardContent>
            <NationalitiesBarChart rows={nationalities} take={10} />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2"><MedalRibbonIcon className="h-4 w-4 text-sky-700" />Competition mix</CardTitle>
          </CardHeader>
          <CardContent>
            <CompetitionMixDonut rows={competition_mix} />
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-4 lg:grid-cols-[0.85fr_1.15fr]">
        <Card className="bg-slate-950 text-slate-50">
          <CardHeader>
            <CardTitle className="text-base text-white flex items-center gap-2"><ScoreSheetIcon className="h-4 w-4 text-sky-300" />Data confidence</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-6 text-slate-300">
            <p>The public site should stay explicit about source-backed coverage and about what is still incomplete.</p>
            <p>Official result links, parser coverage, and last-seen dates should become part of the experience rather than hidden implementation details.</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base flex items-center gap-2"><IceRinkIcon className="h-4 w-4 text-sky-700" />Featured club signal</CardTitle>
            {topClub ? (
              <Link href={`/clubs/${encodeURIComponent(topClub.club_name)}`} className="text-sm font-medium text-sky-800 hover:text-sky-950">
                Open club
              </Link>
            ) : null}
          </CardHeader>
          <CardContent>
            {topClub ? (
              <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
                <div>
                  <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Club</p>
                  <p className="mt-1 text-base font-semibold">{topClub.club_name}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Skaters</p>
                  <p className="mt-1 text-base font-semibold">{number(topClub.skaters)}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Events</p>
                  <p className="mt-1 text-base font-semibold">{number(topClub.events)}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Best TSS</p>
                  <p className="mt-1 text-base font-semibold">{points(topClub.best_tss)}</p>
                </div>
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">Club summary will appear here once club rows are available.</p>
            )}
          </CardContent>
        </Card>
      </section>

      <section className="rounded-3xl border bg-slate-50 p-6 md:p-8">
        <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Next Layer</p>
            <h2 className="mt-2 text-2xl font-semibold tracking-tight">Build profile pages and competition pages as the main public surface.</h2>
          </div>
          <div className="flex flex-wrap gap-3">
            <Link href="/events" className="inline-flex items-center gap-2 rounded-full border bg-white px-4 py-2 text-sm font-medium hover:bg-slate-100">
              Browse events <CalendarRange className="h-4 w-4" />
            </Link>
            <Link href="/skaters" className="inline-flex items-center gap-2 rounded-full border bg-white px-4 py-2 text-sm font-medium hover:bg-slate-100">
              Search skaters <Search className="h-4 w-4" />
            </Link>
            <Link href="/about" className="inline-flex items-center gap-2 rounded-full border bg-white px-4 py-2 text-sm font-medium hover:bg-slate-100">
              Project brief <Sparkles className="h-4 w-4" />
            </Link>
          </div>
        </div>
      </section>
    </div>
  );
}
