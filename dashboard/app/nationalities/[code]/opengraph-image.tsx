import { getNationality } from "@/lib/api";
import { renderOgImage, OG_IMAGE_CONTENT_TYPE, OG_IMAGE_SIZE } from "@/lib/og";
import { points } from "@/lib/format";

export const alt = "Country page";
export const size = OG_IMAGE_SIZE;
export const contentType = OG_IMAGE_CONTENT_TYPE;

export default async function Image({ params }: { params: Promise<{ code: string }> }) {
  const { code } = await params;
  const nation = await getNationality(decodeURIComponent(code));
  return renderOgImage({
    kicker: "Country Page",
    title: nation.nation_code,
    subtitle: `${nation.skaters} skaters • ${nation.events} events • ${nation.segment_scores} segment scores`,
    chips: [
      `Best TSS ${points(nation.best_tss)}`,
      `${nation.development_skaters} development`,
      `${nation.converted_development_skaters} converted`,
    ],
  });
}
