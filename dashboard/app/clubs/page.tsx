import { PageHeader } from "@/components/page-header";
import { ClubsTable } from "@/components/tables/clubs-table";
import { Card, CardContent } from "@/components/ui/card";
import { listClubs } from "@/lib/api";

export const revalidate = 30;

export default async function Page({ searchParams }: { searchParams: Promise<{ q?: string }> }) {
  const sp = await searchParams;
  const { items } = await listClubs(sp.q);
  return (
    <div className="space-y-6">
      <PageHeader title="Clubs" subtitle={`${items.length} club${items.length === 1 ? "" : "s"} indexed${sp.q ? ` matching "${sp.q}"` : ""}`} />
      <Card><CardContent className="p-4"><ClubsTable rows={items} /></CardContent></Card>
    </div>
  );
}
