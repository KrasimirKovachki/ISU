import { getSkater } from "@/lib/api";
import { renderOgImage, OG_IMAGE_CONTENT_TYPE, OG_IMAGE_SIZE } from "@/lib/og";
import { points } from "@/lib/format";

export const alt = "Skater profile";
export const size = OG_IMAGE_SIZE;
export const contentType = OG_IMAGE_CONTENT_TYPE;

export default async function Image({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const skater = await getSkater(Number(id));
  return renderOgImage({
    kicker: "Skater Profile",
    title: skater.skater_name,
    subtitle: `${skater.nation_code ?? "Nation pending"}${skater.club_name ? ` • ${skater.club_name}` : ""}`,
    chips: [
      `${skater.competitions} competitions`,
      `${skater.scored_competitions} scored`,
      `Best total ${points(skater.best_score_total)}`,
    ],
  });
}
