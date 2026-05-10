"use client";
/** Sticky top bar: global search box + a visible submit button so Enter
 * isn't the only way to fire (some browsers don't reliably submit
 * single-input forms via Enter inside complex layouts). */
import { Search } from "lucide-react";
import { useRouter, useSearchParams, usePathname } from "next/navigation";
import { useEffect, useState, type FormEvent } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

export function Topbar() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  // Seed the input with the current ?q= so refreshing on a search keeps the term visible.
  const [q, setQ] = useState(searchParams.get("q") ?? "");

  // If the URL ?q changes (e.g. user used the back button), sync the input.
  useEffect(() => {
    setQ(searchParams.get("q") ?? "");
  }, [searchParams]);

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
    <header className="sticky top-0 z-10 flex items-center gap-3 border-b bg-background/80 backdrop-blur px-4 md:px-6 py-3">
      <form onSubmit={submit} className="flex-1 max-w-xl flex items-center gap-2">
        <div className="relative flex-1">
          <Search className="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Search skaters, clubs, events, nations…"
            className="pl-8"
          />
        </div>
        <Button type="submit" size="sm">Go</Button>
      </form>
      <a
        href="/api/health"
        target="_blank"
        rel="noreferrer"
        className="text-xs text-muted-foreground hover:text-foreground"
      >
        API
      </a>
    </header>
  );
}
