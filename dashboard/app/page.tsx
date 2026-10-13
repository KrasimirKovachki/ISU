export const dynamic = "force-dynamic";
import Link from "next/link";
import { Search } from "lucide-react";

import { CompetitionMixDonut } from "@/components/competition-mix-donut";
import { NationalitiesBarChart } from "@/components/nationalities-bar-chart";
import { IceRinkIcon, MedalRibbonIcon, ScoreSheetIcon, SkatingBladeIcon } from "@/components/skating-icons";
import { getDashboard, listEvents } from "@/lib/api";
import { formatDate, number, points } from "@/lib/format";
import { buildMetadata } from "@/lib/site";

export const revalidate = 30;
export const metadata = buildMetadata({
  title: "Home",
  description: "Figure skating results from official result pages: competitions, score sheets, skaters, clubs and countries.",
  path: "/",
});

const EXPLORE_LINKS = [
  {
    href: "/skaters",
    title: "Skaters",
    body: "Search public profiles with score progression, element history, nationality, and club links.",
    Icon: SkatingBladeIcon,
  },
  {
    href: "/clubs",
    title: "Clubs",
    body: "Browse active clubs, their skaters, event presence, and best performances.",
    Icon: IceRinkIcon,
  },
  {
    href: "/events",
    title: "Competitions",
    body: "Follow events, participants, results, and source documents from official protocols.",
    Icon: ScoreSheetIcon,
  },
  {
    href: "/insights",
    title: "Stats",
    body: "Follow score trends, country coverage, competition mix, and club-level performance views.",
    Icon: MedalRibbonIcon,
  },
];

export default async function Page() {
  // Edited 2026-10-12 by Claude (AI assistant): redesign — search first, then the latest competitions; the
  // design-note panels of the old page are gone, the data sections stay
  const [dashboard, eventsResponse] = await Promise.all([getDashboard(), listEvents({ limit: 12 })]);

  const { overview, clubs, nationalities, competition_mix } = dashboard;
  const latestEvents = eventsResponse.items
    .filter((event) => event.event_start_date)
    .sort((a, b) => (a.event_start_date! < b.event_start_date! ? 1 : -1))
    .slice(0, 6);
  const topClubs = [...clubs].sort((a, b) => (b.skaters ?? 0) - (a.skaters ?? 0)).slice(0, 6);

  return (
    <div className="space-y-6">
      <section className="rounded-2xl bg-card px-6 py-7 shadow-[0_10px_30px_rgba(11,27,43,0.10)] md:px-10 md:py-10">
        <div className="flex flex-wrap items-end gap-8">
          <div className="min-w-0 flex-[999_1_420px]">
            <div className="eyebrow">Figure skating results</div>
            <h1 className="mt-2 text-[32px] font-extrabold leading-[1.1] text-balance md:text-5xl">
              Every competition, score sheet and skater in one place.
            </h1>
            <p className="mt-3 max-w-2xl text-base text-ink-2 md:text-lg">
              Read from the official result pages and judges&apos; PDFs, with a link back to every original.
            </p>
            <form action="/skaters" method="get" role="search" className="mt-6 flex max-w-xl gap-2">
              <label htmlFor="home-search" className="sr-only">Search skaters</label>
              <div className="relative flex-1">
                <Search className="pointer-events-none absolute left-3.5 top-1/2 h-5 w-5 -translate-y-1/2 text-muted-foreground" />
                <input
                  id="home-search"
                  name="q"
                  placeholder="Skater name, e.g. Petrova"
                  className="h-12 w-full rounded-xl border border-input bg-background pl-11 pr-3 text-base outline-none focus:border-brand focus:ring-2 focus:ring-brand/20"
                />
              </div>
              <button type="submit" className="h-12 rounded-xl bg-brand px-5 font-semibold text-white hover:bg-brand-strong">Search</button>
            </form>
          </div>
          <div className="grid flex-[1_1_300px] grid-cols-2 gap-3">
            {[
              ["competitions", overview.events, "/events"],
              ["skaters", overview.skaters, "/skaters"],
              ["clubs", overview.scoring_clubs, "/clubs"],
              ["countries", nationalities.length, "/nationalities"],
            ].map(([label, value, href]) => (
              <Link key={label as string} href={href as string} className="rounded-xl bg-background px-4 py-3.5 text-foreground hover:bg-ice-2 hover:no-underline">
                <div className="num text-[28px] font-semibold leading-tight">{number(value as number)}</div>
                <div className="text-sm text-muted-foreground">{label}</div>
              </Link>
            ))}
          </div>
        </div>
      </section>

      <section className="rounded-2xl bg-card">
        <div className="flex flex-wrap items-baseline justify-between gap-3 border-b px-5 py-4">
          <h2 className="text-xl font-extrabold">Latest competitions</h2>
          <Link href="/events" className="text-sm font-semibold">All competitions</Link>
        </div>
        <div className="grid sm:grid-cols-2 lg:grid-cols-3">
          {latestEvents.map((event) => (
            <Link
              key={event.event_key}
              href={`/events/${event.event_key}`}
              className="flex flex-col gap-1 border-b px-5 py-4 text-foreground hover:bg-background hover:no-underline sm:border-r"
            >
              <span className="num text-sm text-muted-foreground">{formatDate(event.event_start_date)}</span>
              <span className="font-bold">{event.event_name ?? "Competition"}</span>
              <span className="text-sm text-ink-2">{event.location ?? "—"}</span>
            </Link>
          ))}
        </div>
      </section>

      <section className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {EXPLORE_LINKS.map(({ href, title, body, Icon }) => (
          <Link key={href} href={href} className="flex flex-col gap-2 rounded-2xl bg-card p-5 text-foreground hover:shadow-md hover:no-underline">
            <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-ice-2 text-brand-strong"><Icon className="h-5 w-5" /></span>
            <span className="text-lg font-bold">{title}</span>
            <span className="text-sm text-ink-2">{body}</span>
          </Link>
        ))}
      </section>

      <section className="grid gap-5 lg:grid-cols-2">
        <div className="rounded-2xl bg-card p-5">
          <div className="mb-3 flex items-baseline justify-between gap-3">
            <h2 className="text-xl font-extrabold">Skaters by country</h2>
            <Link href="/nationalities" className="text-sm font-semibold">All countries</Link>
          </div>
          <NationalitiesBarChart rows={nationalities} take={10} hrefBase="/nationalities/" />
        </div>
        <div className="rounded-2xl bg-card p-5">
          <div className="mb-3 flex items-baseline justify-between gap-3">
            <h2 className="text-xl font-extrabold">Kinds of competitions</h2>
            <Link href="/insights" className="text-sm font-semibold">Stats</Link>
          </div>
          <CompetitionMixDonut rows={competition_mix} href="/insights#event-mix" />
        </div>
      </section>

      <section className="rounded-2xl bg-card">
        <div className="flex flex-wrap items-baseline justify-between gap-3 border-b px-5 py-4">
          <h2 className="text-xl font-extrabold">Largest clubs</h2>
          <Link href="/clubs" className="text-sm font-semibold">All clubs</Link>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full min-w-[520px] border-collapse text-[15px]">
            <thead>
              <tr className="text-left text-xs uppercase tracking-[0.08em] text-muted-foreground">
                <th scope="col" className="px-5 py-2.5 font-medium">Club</th>
                <th scope="col" className="px-2 py-2.5 text-right font-medium">Skaters</th>
                <th scope="col" className="px-2 py-2.5 text-right font-medium">Competitions</th>
                <th scope="col" className="px-5 py-2.5 text-right font-medium">Best total</th>
              </tr>
            </thead>
            <tbody>
              {topClubs.map((c) => (
                <tr key={c.club_name} className="border-t">
                  <td className="px-5 py-3"><Link href={`/clubs/${encodeURIComponent(c.club_name)}`} className="font-semibold">{c.club_name}</Link></td>
                  <td className="num px-2 py-3 text-right">{number(c.skaters)}</td>
                  <td className="num px-2 py-3 text-right">{number(c.events)}</td>
                  <td className="num px-5 py-3 text-right">{points(c.best_tss)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <p className="text-center text-sm text-muted-foreground">
        Results from {formatDate(overview.latest_event_date)} back · {number(overview.element_scores)} judged elements ·{" "}
        <Link href="/about">How the data is collected</Link>
      </p>
    </div>
  );
}
