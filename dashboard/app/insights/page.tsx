import { InsightsOverview } from "@/components/insights-overview";

export const revalidate = 30;

export default async function Page({
  searchParams,
}: {
  searchParams: Promise<{ from_year?: string; to_year?: string }>;
}) {
  return (
    <InsightsOverview
      searchParams={searchParams}
      title="Insights"
      subtitle="Analytical views across skaters, clubs, countries, and event types."
    />
  );
}
