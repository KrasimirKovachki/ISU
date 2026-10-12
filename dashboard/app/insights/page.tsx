export const dynamic = "force-dynamic";
import { InsightsOverview } from "@/components/insights-overview";
import { buildMetadata } from "@/lib/site";

export const revalidate = 30;
export const metadata = buildMetadata({
  title: "Club Stats",
  description: "Analytical views across skaters, clubs, countries, and event types in the Ice Skating Club website.",
  path: "/insights",
});

export default async function Page({
  searchParams,
}: {
  searchParams: Promise<{ from_year?: string; to_year?: string }>;
}) {
  return (
    <InsightsOverview
      searchParams={searchParams}
      title="Club Stats"
      subtitle="Analytical views across skaters, clubs, countries, and event types."
    />
  );
}
