import type { MetadataRoute } from "next";

import { listClubs, listEvents, listNationalities, searchSkaters } from "@/lib/api";
import { SITE_URL } from "@/lib/site";

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
  const basePages: MetadataRoute.Sitemap = [
    { url: `${SITE_URL}/`, changeFrequency: "daily", priority: 1 },
    { url: `${SITE_URL}/about`, changeFrequency: "weekly", priority: 0.7 },
    { url: `${SITE_URL}/insights`, changeFrequency: "daily", priority: 0.8 },
    { url: `${SITE_URL}/skaters`, changeFrequency: "daily", priority: 0.9 },
    { url: `${SITE_URL}/clubs`, changeFrequency: "weekly", priority: 0.8 },
    { url: `${SITE_URL}/events`, changeFrequency: "daily", priority: 0.9 },
    { url: `${SITE_URL}/nationalities`, changeFrequency: "weekly", priority: 0.8 },
  ];

  const [skaters, clubs, events, nationalities] = await Promise.all([
    searchSkaters(undefined, 500).then((r) => r.items).catch(() => []),
    listClubs().then((r) => r.items).catch(() => []),
    listEvents({ limit: 1000 }).then((r) => r.items).catch(() => []),
    listNationalities().then((r) => r.items).catch(() => []),
  ]);

  return [
    ...basePages,
    ...skaters.map((skater) => ({
      url: `${SITE_URL}/skaters/${skater.skater_key}`,
      lastModified: skater.latest_seen_date ?? undefined,
      changeFrequency: "weekly" as const,
      priority: 0.8,
    })),
    ...clubs.map((club) => ({
      url: `${SITE_URL}/clubs/${encodeURIComponent(club.club_name)}`,
      lastModified: club.latest_seen_date ?? undefined,
      changeFrequency: "weekly" as const,
      priority: 0.75,
    })),
    ...events.map((event) => ({
      url: `${SITE_URL}/events/${event.event_key}`,
      lastModified: event.event_start_date ?? undefined,
      changeFrequency: "monthly" as const,
      priority: 0.75,
    })),
    ...nationalities.map((nation) => ({
      url: `${SITE_URL}/nationalities/${encodeURIComponent(nation.nation_code)}`,
      lastModified: nation.latest_seen_date ?? undefined,
      changeFrequency: "weekly" as const,
      priority: 0.7,
    })),
  ];
}
