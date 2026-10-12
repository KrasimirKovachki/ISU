import type { Metadata, Viewport } from "next";
import { Suspense } from "react";

import { Providers } from "./providers";
import { Topbar } from "@/components/topbar";
import { SITE_DESCRIPTION, SITE_NAME, SITE_THEME_COLOR, SITE_URL } from "@/lib/site";
import "./globals.css";

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
    <html lang="en" className="dark h-full antialiased" suppressHydrationWarning>
      <body className="min-h-full bg-background">
        <Providers>
          <div className="min-h-screen">
            <Suspense fallback={<div className="sticky top-0 z-10 h-[88px] border-b bg-background/80 backdrop-blur" />}>
              <Topbar />
            </Suspense>
            <main className="mx-auto flex-1 w-full max-w-[1600px] px-4 py-6 md:px-6 lg:px-8">{children}</main>
          </div>
        </Providers>
      </body>
    </html>
  );
}
