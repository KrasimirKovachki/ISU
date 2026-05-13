import { renderOgImage, OG_IMAGE_CONTENT_TYPE, OG_IMAGE_SIZE } from "@/lib/og";

export const alt = "Skating Data";
export const size = OG_IMAGE_SIZE;
export const contentType = OG_IMAGE_CONTENT_TYPE;

export default function Image() {
  return renderOgImage({
    kicker: "Skating Data",
    title: "Public Figure-Skating Data",
    subtitle: "Skaters, clubs, competitions, countries, and source-backed score analytics.",
    chips: ["Skater profiles", "Competition pages", "Country coverage", "Score evidence"],
  });
}
