"use client";
/** Sticky top bar: global search box + a visible submit button so Enter
 * isn't the only way to fire (some browsers don't reliably submit
 * single-input forms via Enter inside complex layouts). */
import { BookOpen, Building2, Calendar, Globe2, Home, LineChart, Search, Users } from "lucide-react";
import Link from "next/link";
import { useRouter, useSearchParams, usePathname } from "next/navigation";
import { useState, type FormEvent } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { SITE_NAME } from "@/lib/site";
import { cn } from "@/lib/utils";

const NAV = [
  { href: "/", label: "Home", Icon: Home },
  { href: "/events", label: "Events", Icon: Calendar },
  { href: "/skaters", label: "Skaters", Icon: Users },
  { href: "/clubs", label: "Clubs", Icon: Building2 },
  { href: "/nationalities", label: "Countries", Icon: Globe2 },
  { href: "/insights", label: "Stats", Icon: LineChart },
  { href: "/about", label: "About", Icon: BookOpen },
];

export function Topbar() {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const q = searchParams.get("q") ?? "";

  return <TopbarInner key={`${pathname}?${searchParams.toString()}`} initialQuery={q} />;
}

function TopbarInner({ initialQuery }: { initialQuery: string }) {
  const router = useRouter();
  const pathname = usePathname();
  const [q, setQ] = useState(initialQuery);

  function submit(event?: FormEvent<HTMLFormElement>) {
    event?.preventDefault();
    const trimmed = q.trim();
    if (!trimmed) {
      router.push("/skaters");
      return;
    }
    // If already on a list page (skaters/clubs/events/nationalities), search
    // within that list. Otherwise default to skaters.
    const target =
      pathname.startsWith("/clubs") ||
      pathname.startsWith("/events") ||
      pathname.startsWith("/nationalities")
        ? pathname
        : "/skaters";
    router.push(`${target}?q=${encodeURIComponent(trimmed)}`);
  }

  // Edited 2026-10-12 by Claude (AI assistant): redesign — navy bar, logo, flat nav, search on the right
  return (
    <header className="sticky top-0 z-20 bg-navy text-white">
      <div className="mx-auto flex w-full max-w-[1280px] flex-wrap items-center gap-x-6 gap-y-2 px-4 py-3 md:px-6">
        <Link href="/" className="flex items-center gap-2.5 text-lg font-extrabold tracking-wide text-white hover:no-underline">
          <svg width="28" height="28" viewBox="0 0 28 28" aria-hidden="true">
            <path d="M4 20 L14 6 L24 20" fill="none" stroke="#7FC4FF" strokeWidth="2.5" strokeLinejoin="round" />
            <path d="M3 23 H25" stroke="#FFFFFF" strokeWidth="2.5" strokeLinecap="round" />
          </svg>
          {SITE_NAME}
        </Link>
        <nav aria-label="Main" className="order-3 flex w-full gap-1 overflow-x-auto text-[15px] lg:order-none lg:w-auto">
          {NAV.map(({ href, label }) => {
            const active = href === "/" ? pathname === "/" : pathname.startsWith(href);
            return (
              <Link
                key={href}
                href={href}
                aria-current={active ? "page" : undefined}
                className={cn(
                  "shrink-0 rounded-lg px-3.5 py-2.5 hover:no-underline",
                  active ? "bg-navy-2 text-white" : "text-[#C9D6E3] hover:bg-navy-2 hover:text-white",
                )}
              >
                {label}
              </Link>
            );
          })}
        </nav>
        <form onSubmit={submit} role="search" className="order-4 flex w-full items-center gap-2 lg:order-none lg:ml-auto lg:w-auto lg:min-w-[260px] lg:max-w-[360px] lg:flex-1">
          <label htmlFor="site-search" className="sr-only">Search skaters, clubs, events, countries</label>
          <div className="relative flex-1">
            <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-[#9FB3C8]" />
            <Input
              id="site-search"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Search skaters, clubs, events"
              className="h-11 rounded-[10px] border-[#2A425C] bg-[#12263A] pl-9 text-[15px] text-white placeholder:text-[#9FB3C8]"
            />
          </div>
          <Button type="submit" className="h-11 rounded-[10px] bg-brand px-4 text-white hover:bg-brand-strong">Go</Button>
        </form>
      </div>
    </header>
  );
}
