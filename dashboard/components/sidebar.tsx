"use client";
/** App sidebar: branded header + primary navigation with active highlight. */
import Link from "next/link";
import { usePathname } from "next/navigation";
import { BookOpen, Building2, Calendar, Globe2, Home, LineChart, Users } from "lucide-react";

import { SkatingBladeIcon } from "@/components/skating-icons";
import { cn } from "@/lib/utils";

const NAV = [
  { href: "/", label: "Home", Icon: Home },
  { href: "/insights", label: "Insights", Icon: LineChart },
  { href: "/skaters", label: "Skaters", Icon: Users },
  { href: "/clubs", label: "Clubs", Icon: Building2 },
  { href: "/events", label: "Events", Icon: Calendar },
  { href: "/nationalities", label: "Countries", Icon: Globe2 },
  { href: "/about", label: "About", Icon: BookOpen },
];

export function Sidebar() {
  const pathname = usePathname();
  return (
    <aside className="hidden md:flex w-60 shrink-0 flex-col border-r border-sidebar-border bg-sidebar text-sidebar-foreground">
      <div className="flex items-center gap-2 border-b border-sidebar-border px-5 py-4">
        <div className="ice-emblem flex h-9 w-9 items-center justify-center rounded-xl text-foreground">
          <SkatingBladeIcon className="h-5 w-5" />
        </div>
        <div>
          <span className="block font-semibold tracking-tight">Skating Data</span>
          <span className="block text-[11px] text-sidebar-foreground/55">Public information site</span>
        </div>
      </div>
      <nav className="flex flex-col gap-0.5 p-2">
        {NAV.map(({ href, label, Icon }) => {
          const active = href === "/" ? pathname === "/" : pathname.startsWith(href);
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                "flex items-center gap-2 rounded-lg px-3 py-2 text-sm transition-colors",
                active
                  ? "bg-sidebar-accent text-sidebar-accent-foreground font-medium shadow-[inset_0_1px_0_rgba(255,255,255,0.05)]"
                  : "text-sidebar-foreground/68 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
              )}
            >
              <Icon className="h-4 w-4" />
              {label}
            </Link>
          );
        })}
      </nav>
      <div className="mt-auto border-t border-sidebar-border p-3 text-[11px] leading-5 text-sidebar-foreground/50">
        Official results, public profiles, and analytics from the same shared data layer.
      </div>
    </aside>
  );
}
