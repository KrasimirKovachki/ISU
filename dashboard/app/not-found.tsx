import Link from "next/link";

import { Button } from "@/components/ui/button";

export default function NotFound() {
  return (
    <div className="flex flex-col items-center justify-center py-24 text-center">
      <p className="text-sm font-medium uppercase tracking-wide text-muted-foreground">404</p>
      <h1 className="mt-2 text-3xl font-semibold tracking-tight">Not found</h1>
      <p className="mt-2 max-w-md text-sm text-muted-foreground">
        That skater, club, event, or score doesn&apos;t exist in our index.
        Skater URLs use a stable hash key — they don&apos;t shift between releases — so this
        either was never imported, or it&apos;s an old link from before identity merging.
      </p>
      <div className="mt-6 flex gap-2">
        <Button asChild>
          <Link href="/">Go to dashboard</Link>
        </Button>
        <Button asChild variant="outline">
          <Link href="/skaters">Browse skaters</Link>
        </Button>
      </div>
    </div>
  );
}
