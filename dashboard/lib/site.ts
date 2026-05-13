import type { Metadata } from "next";

export const SITE_NAME = "Skating Data";
export const SITE_DESCRIPTION = "Public figure-skating profiles, competitions, clubs, countries, and source-backed score analytics.";
export const SITE_URL = process.env.NEXT_PUBLIC_SITE_URL || "http://127.0.0.1:3000";

export function absoluteUrl(path = "/") {
  return new URL(path, SITE_URL);
}

export function buildMetadata({
  title,
  description = SITE_DESCRIPTION,
  path = "/",
}: {
  title: string;
  description?: string;
  path?: string;
}): Metadata {
  return {
    title,
    description,
    alternates: { canonical: path },
    openGraph: {
      title,
      description,
      url: absoluteUrl(path),
      siteName: SITE_NAME,
      type: "website",
    },
    twitter: {
      card: "summary_large_image",
      title,
      description,
    },
  };
}
