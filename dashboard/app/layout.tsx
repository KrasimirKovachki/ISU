import type { Metadata, Viewport } from "next";
import { Archivo, IBM_Plex_Mono } from "next/font/google";
import { Suspense } from "react";

import { Providers } from "./providers";
import { Topbar } from "@/components/topbar";
import { SITE_DESCRIPTION, SITE_NAME, SITE_THEME_COLOR, SITE_URL } from "@/lib/site";
import "./globals.css";

// Edited 2026-10-12 by Claude (AI assistant): redesign fonts — Archivo for text, IBM Plex Mono for every score
const archivo = Archivo({ subsets: ["latin", "latin-ext"], variable: "--font-archivo", display: "swap" });
const plexMono = IBM_Plex_Mono({ subsets: ["latin", "latin-ext"], weight: ["400", "500", "600"], variable: "--font-plex-mono", display: "swap" });

export const metadata: Metadata = {
  metadataBase: new URL(SITE_URL),
  applicationName: SITE_NAME,
  title: {
    default: SITE_NAME,
    template: `%s | ${SITE_NAME}`,
  },
  description: SITE_DESCRIPTION,
  manifest: "/manifest.webmanifest",
  icons: {
    icon: "/icon",
    apple: "/apple-icon",
  },
  openGraph: {
    title: SITE_NAME,
    description: SITE_DESCRIPTION,
    url: SITE_URL,
    siteName: SITE_NAME,
    type: "website",
  },
  twitter: {
    card: "summary_large_image",
    title: SITE_NAME,
    description: SITE_DESCRIPTION,
  },
};

export const viewport: Viewport = {
  themeColor: SITE_THEME_COLOR,
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en" className={`${archivo.variable} ${plexMono.variable} h-full antialiased`} suppressHydrationWarning>
      <body className="min-h-full bg-background">
        <Providers>
          <div className="min-h-screen">
            <Suspense fallback={<div className="sticky top-0 z-10 h-[72px] bg-navy" />}>
              <Topbar />
            </Suspense>
            {/* navy band behind the top of every page; page headers overlap it */}
            <div className="relative">
              <div className="hero-band absolute inset-x-0 top-0 h-36" aria-hidden="true" />
              <main className="relative mx-auto w-full max-w-[1280px] px-4 pb-10 pt-3 md:px-6">{children}</main>
            </div>
          </div>
        </Providers>
      </body>
    </html>
  );
}
