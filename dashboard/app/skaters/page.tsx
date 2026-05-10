import { PageHeader } from "@/components/page-header";
import { SkatersTable } from "@/components/tables/skaters-table";
import { Card, CardContent } from "@/components/ui/card";
import { searchSkaters } from "@/lib/api";

export const revalidate = 30;

export default async function Page({ searchParams }: { searchParams: Promise<{ q?: string }> }) {
  const sp = await searchParams;
  const q = sp.q?.trim();
  const { items } = await searchSkaters(q, 500);
  return (
    <div className="space-y-6">
      <PageHeader
        title="Skaters"
        subtitle={
          q
            ? `${items.length} match${items.length === 1 ? "" : "es"} for "${q}"`
            : `${items.length} skaters indexed (most-recent first). Use the topbar Search to filter.`
        }
      />
      <Card><CardContent className="p-4"><SkatersTable rows={items} /></CardContent></Card>
    </div>
  );
}
