"use client";
/** Sticky top bar: page-level breadcrumb-ish title slot + global search box. */
import { Search } from "lucide-react";
import { useRouter } from "next/navigation";
import { useState, type FormEvent } from "react";

import { Input } from "@/components/ui/input";

export function Topbar() {
  const router = useRouter();
  const [q, setQ] = useState("");
  function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmed = q.trim();
    if (!trimmed) return;
    router.push(`/skaters?q=${encodeURIComponent(trimmed)}`);
  }
  return (
    <header className="sticky top-0 z-10 flex items-center gap-3 border-b bg-background/80 backdrop-blur px-4 md:px-6 py-3">
      <form onSubmit={onSubmit} className="flex-1 max-w-md">
        <div className="relative">
          <Search className="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Search skaters, clubs, events…"
            className="pl-8"
          />
        </div>
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
