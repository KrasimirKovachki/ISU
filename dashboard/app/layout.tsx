import type { Metadata, Viewport } from "next";
import { Suspense } from "react";

import { Providers } from "./providers";
import { Sidebar } from "@/components/sidebar";
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
    <html lang="en" className="h-full antialiased" suppressHydrationWarning>
      <body className="min-h-full bg-background">
        <Providers>
          <div className="flex min-h-screen">
            <Sidebar />
            <div className="flex-1 flex flex-col min-w-0">
              <Suspense fallback={<div className="sticky top-0 z-10 h-[61px] border-b bg-background/80 backdrop-blur" />}>
                <Topbar />
              </Suspense>
              <main className="flex-1 px-4 md:px-6 py-6">{children}</main>
            </div>
          </div>
        </Providers>
      </body>
    </html>
  );
}
