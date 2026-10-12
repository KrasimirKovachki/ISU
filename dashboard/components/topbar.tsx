"use client";
/** Sticky top bar: global search box + a visible submit button so Enter
 * isn't the only way to fire (some browsers don't reliably submit
 * single-input forms via Enter inside complex layouts). */
import { BookOpen, Building2, Calendar, Globe2, Home, LineChart, Search, Users } from "lucide-react";
import Link from "next/link";
import { useRouter, useSearchParams, usePathname } from "next/navigation";
import { useState, type FormEvent } from "react";

import { SkatingBladeIcon } from "@/components/skating-icons";
import { ThemeToggle } from "@/components/theme-toggle";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

const NAV = [
  { href: "/", label: "Home", Icon: Home },
  { href: "/skaters", label: "Skaters", Icon: Users },
  { href: "/clubs", label: "Clubs", Icon: Building2 },
  { href: "/events", label: "Events", Icon: Calendar },
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

  return (
    <header className="sticky top-0 z-20 border-b border-slate-800 bg-slate-950/94 text-slate-200 backdrop-blur">
      <div className="mx-auto flex w-full max-w-[1600px] flex-col gap-3 px-4 py-3 md:px-6 lg:px-8">
        <div className="flex items-center justify-between gap-4">
          <Link href="/" className="flex min-w-0 items-center gap-3">
            <div className="ice-emblem flex h-10 w-10 items-center justify-center rounded-xl text-slate-950">
              <SkatingBladeIcon className="h-5 w-5" />
            </div>
            <div className="min-w-0">
              <span className="block truncate text-base font-semibold tracking-tight text-slate-100">Ice Skating Club</span>
              <span className="block truncate text-[11px] text-slate-400">club website, results, and skating stats</span>
            </div>
          </Link>
          <div className="hidden items-center gap-2 md:flex">
            <ThemeToggle />
            <a
              href="/api/health"
              target="_blank"
              rel="noreferrer"
              className="text-xs text-slate-400 hover:text-slate-100"
            >
              API
            </a>
          </div>
        </div>
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <nav className="flex flex-wrap gap-1">
            {NAV.map(({ href, label, Icon }) => {
              const active = href === "/" ? pathname === "/" : pathname.startsWith(href);
              return (
                <Link
                  key={href}
                  href={href}
                  className={cn(
                    "inline-flex items-center gap-2 rounded-full px-3 py-2 text-sm",
                    active
                      ? "bg-slate-800 text-slate-100"
                      : "text-slate-400 hover:bg-slate-900 hover:text-slate-100",
                  )}
                >
                  <Icon className="h-4 w-4" />
                  {label}
                </Link>
              );
            })}
          </nav>
          <form onSubmit={submit} className="flex w-full max-w-xl items-center gap-2">
            <div className="relative flex-1">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-slate-500" />
              <Input
                value={q}
                onChange={(e) => setQ(e.target.value)}
                placeholder="Search skaters, clubs, events, nations…"
                className="border-white/10 bg-slate-900/80 pl-8 text-slate-100 placeholder:text-slate-500"
              />
            </div>
            <Button type="submit" size="sm" className="bg-sky-700 text-white hover:bg-sky-600">Go</Button>
          </form>
        </div>
      </div>
    </header>
  );
}
