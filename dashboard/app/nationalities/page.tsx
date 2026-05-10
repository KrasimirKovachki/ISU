import { PageHeader } from "@/components/page-header";
import { NationalitiesTable } from "@/components/tables/nationalities-table";
import { Card, CardContent } from "@/components/ui/card";
import { listNationalities } from "@/lib/api";

export const revalidate = 30;

export default async function Page({ searchParams }: { searchParams: Promise<{ q?: string }> }) {
  const sp = await searchParams;
  const { items } = await listNationalities(sp.q);
  return (
    <div className="space-y-6">
      <PageHeader title="Nationalities" subtitle={`${items.length} nation${items.length === 1 ? "" : "s"}${sp.q ? ` matching "${sp.q}"` : ""}`} />
      <Card><CardContent className="p-4"><NationalitiesTable rows={items} /></CardContent></Card>
    </div>
  );
}
