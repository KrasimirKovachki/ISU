import { getEvent } from "@/lib/api";
import { renderOgImage, OG_IMAGE_CONTENT_TYPE, OG_IMAGE_SIZE } from "@/lib/og";

export const alt = "Competition page";
export const size = OG_IMAGE_SIZE;
export const contentType = OG_IMAGE_CONTENT_TYPE;

export default async function Image({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const event = await getEvent(Number(id));
  return renderOgImage({
    kicker: "Competition Page",
    title: event.event_name ?? "Competition",
    subtitle: `${event.location ?? "Location pending"}${event.event_start_date ? ` • ${event.event_start_date}` : ""}`,
    chips: [
      `${event.results.length} entries`,
      event.competition_stream ?? "stream pending",
      event.event_type ?? "type pending",
    ],
  });
}
