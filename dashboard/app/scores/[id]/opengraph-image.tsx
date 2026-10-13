import { getScore } from "@/lib/api";
import { renderOgImage, OG_IMAGE_CONTENT_TYPE, OG_IMAGE_SIZE } from "@/lib/og";
import { points } from "@/lib/format";

export const alt = "Score detail";
export const size = OG_IMAGE_SIZE;
export const contentType = OG_IMAGE_CONTENT_TYPE;

export default async function Image({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const score = await getScore(Number(id));
  return renderOgImage({
    kicker: "Score Detail",
    title: `${score.skater_name} · ${score.segment_name ?? "Segment"}`,
    subtitle: `${score.event_name ?? "Event"}${score.category_name ? ` • ${score.category_name}` : ""}`,
    chips: [
      `TSS ${points(score.tss)}`,
      `TES ${points(score.tes)}`,
      `PCS ${points(score.pcs)}`,
      `${score.judges.length} judges`,
    ],
  });
}
