"use client";
/** Tri-state theme toggle: light / dark / system. Cycles on click. */
import { Monitor, Moon, Sun } from "lucide-react";
import { useTheme } from "next-themes";
import { useSyncExternalStore } from "react";

import { Button } from "@/components/ui/button";

function subscribe() {
  return () => {};
}

export function ThemeToggle() {
  const mounted = useSyncExternalStore(subscribe, () => true, () => false);
  const { theme, setTheme } = useTheme();

  // next-themes is client-only; render a placeholder until mounted to avoid
  // hydration mismatch between server (system theme unknown) and client.
  if (!mounted) return <Button variant="ghost" size="icon" disabled className="h-8 w-8" />;

  const next = theme === "light" ? "dark" : theme === "dark" ? "system" : "light";
  const Icon = theme === "dark" ? Moon : theme === "light" ? Sun : Monitor;
  return (
    <Button
      variant="ghost"
      size="icon"
      className="h-8 w-8"
      title={`Theme: ${theme} (click for ${next})`}
      onClick={() => setTheme(next)}
    >
      <Icon className="h-4 w-4" />
    </Button>
  );
}
