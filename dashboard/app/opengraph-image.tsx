import { renderOgImage, OG_IMAGE_CONTENT_TYPE, OG_IMAGE_SIZE } from "@/lib/og";

export const alt = "Ice Skating Club";
export const size = OG_IMAGE_SIZE;
export const contentType = OG_IMAGE_CONTENT_TYPE;

export default function Image() {
  return renderOgImage({
    kicker: "Ice Skating Club",
    title: "Club profiles, results, and skating stats",
    subtitle: "Skaters, competitions, countries, and source-backed performance coverage.",
    chips: ["Club pages", "Skater profiles", "Competition results", "Club stats"],
  });
}
