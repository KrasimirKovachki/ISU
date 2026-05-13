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
    <aside className="hidden md:flex w-60 shrink-0 flex-col border-r bg-sidebar">
      <div className="flex items-center gap-2 px-5 py-4 border-b">
        <div className="ice-emblem h-9 w-9 rounded-xl text-sky-800 flex items-center justify-center">
          <SkatingBladeIcon className="h-5 w-5" />
        </div>
        <div>
          <span className="block font-semibold tracking-tight">Skating Data</span>
          <span className="block text-[11px] text-muted-foreground">Public information site</span>
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
                "flex items-center gap-2 rounded-md px-3 py-2 text-sm transition-colors",
                active
                  ? "bg-sidebar-accent text-sidebar-accent-foreground font-medium"
                  : "text-sidebar-foreground/70 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
              )}
            >
              <Icon className="h-4 w-4" />
              {label}
            </Link>
          );
        })}
      </nav>
      <div className="mt-auto p-3 text-[11px] text-muted-foreground border-t leading-5">
        Official results, public profiles, and analytics from the same shared data layer.
      </div>
    </aside>
  );
}
