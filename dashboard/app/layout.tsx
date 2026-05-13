import type { Metadata } from "next";
import { Suspense } from "react";

import { Providers } from "./providers";
import { Sidebar } from "@/components/sidebar";
import { Topbar } from "@/components/topbar";
import "./globals.css";

export const metadata: Metadata = {
  title: "Skating Data",
  description: "ISU & national figure-skating results, scores, and analytics.",
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
