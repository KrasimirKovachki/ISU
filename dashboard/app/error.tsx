"use client";
/** Route-segment error boundary. Next.js renders this when a server
 * component throws (e.g. API timeout, malformed response). */
import { useEffect } from "react";

import { Button } from "@/components/ui/button";

export default function GlobalError({ error, reset }: { error: Error & { digest?: string }; reset: () => void }) {
  useEffect(() => {
    console.error("Route error:", error);
  }, [error]);

  return (
    <div className="flex flex-col items-center justify-center py-24 text-center">
      <p className="text-sm font-medium uppercase tracking-wide text-destructive">error</p>
      <h1 className="mt-2 text-3xl font-semibold tracking-tight">Something went wrong</h1>
      <p className="mt-2 max-w-md text-sm text-muted-foreground">
        The API call for this page failed. The most common cause is the FastAPI backend not
        being reachable; the dashboard rewrites <code>/api/*</code> to <code>SKATING_API_URL</code>.
      </p>
      {error.digest ? (
        <p className="mt-3 text-xs text-muted-foreground font-mono">trace: {error.digest}</p>
      ) : null}
      <div className="mt-6 flex gap-2">
        <Button onClick={() => reset()}>Try again</Button>
        <Button variant="outline" onClick={() => (window.location.href = "/")}>Home</Button>
      </div>
    </div>
  );
}
