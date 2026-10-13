import { getClub } from "@/lib/api";
import { renderOgImage, OG_IMAGE_CONTENT_TYPE, OG_IMAGE_SIZE } from "@/lib/og";
import { points } from "@/lib/format";

export const alt = "Club profile";
export const size = OG_IMAGE_SIZE;
export const contentType = OG_IMAGE_CONTENT_TYPE;

export default async function Image({ params }: { params: Promise<{ name: string }> }) {
  const { name } = await params;
  const club = await getClub(decodeURIComponent(name));
  return renderOgImage({
    kicker: "Club Profile",
    title: club.club_name,
    subtitle: `${club.country_code ?? "Country pending"} • ${club.events} competitions • ${club.skaters} skaters`,
    chips: [
      `${club.segment_scores} segment scores`,
      `Best TSS ${points(club.best_tss)}`,
      `${club.international_events} international`,
    ],
  });
}
