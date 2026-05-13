import { InsightsOverview } from "@/components/insights-overview";
import { buildMetadata } from "@/lib/site";

export const revalidate = 30;
export const metadata = buildMetadata({
  title: "Insights",
  description: "Analytical views across skaters, clubs, countries, and event types in the Skating Data public site.",
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
      title="Insights"
      subtitle="Analytical views across skaters, clubs, countries, and event types."
    />
  );
}
