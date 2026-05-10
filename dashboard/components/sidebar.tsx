"use client";
/** App sidebar: branded header + primary navigation with active highlight. */
import Link from "next/link";
import { usePathname } from "next/navigation";
import { Award, Building2, Calendar, Globe2, LayoutDashboard, Users } from "lucide-react";

import { cn } from "@/lib/utils";

const NAV = [
  { href: "/",              label: "Dashboard",     Icon: LayoutDashboard },
  { href: "/skaters",       label: "Skaters",       Icon: Users },
  { href: "/clubs",         label: "Clubs",         Icon: Building2 },
  { href: "/events",        label: "Events",        Icon: Calendar },
  { href: "/nationalities", label: "Nationalities", Icon: Globe2 },
];

export function Sidebar() {
  const pathname = usePathname();
  return (
    <aside className="hidden md:flex w-60 shrink-0 flex-col border-r bg-sidebar">
      <div className="flex items-center gap-2 px-5 py-4 border-b">
        <Award className="h-5 w-5 text-primary" />
        <span className="font-semibold tracking-tight">Skating Data</span>
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
      <div className="mt-auto p-3 text-[11px] text-muted-foreground border-t">
        ISU + national data · v0.2
      </div>
    </aside>
  );
}
