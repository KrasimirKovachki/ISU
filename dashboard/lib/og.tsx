import { ImageResponse } from "next/og";

import { SITE_NAME } from "@/lib/site";

export const OG_IMAGE_SIZE = {
  width: 1200,
  height: 630,
} as const;

export const OG_IMAGE_CONTENT_TYPE = "image/png";

export function renderOgImage({
  kicker,
  title,
  subtitle,
  chips = [],
}: {
  kicker: string;
  title: string;
  subtitle?: string;
  chips?: string[];
}) {
  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          position: "relative",
          overflow: "hidden",
          background:
            "linear-gradient(135deg, #e0f2fe 0%, #f8fbff 45%, #dbeafe 100%)",
          color: "#0f172a",
          fontFamily: "Avenir Next, Segoe UI, Arial, sans-serif",
        }}
      >
        <div
          style={{
            position: "absolute",
            inset: 0,
            backgroundImage:
              "linear-gradient(rgba(255,255,255,0.35) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.35) 1px, transparent 1px)",
            backgroundSize: "42px 42px",
            opacity: 0.65,
          }}
        />
        <div
          style={{
            position: "absolute",
            top: -80,
            right: -60,
            width: 360,
            height: 360,
            borderRadius: "9999px",
            background: "radial-gradient(circle, rgba(56,189,248,0.22), rgba(56,189,248,0))",
          }}
        />
        <div
          style={{
            position: "absolute",
            bottom: -120,
            left: -40,
            width: 440,
            height: 440,
            borderRadius: "9999px",
            background: "radial-gradient(circle, rgba(14,165,233,0.18), rgba(14,165,233,0))",
          }}
        />

        <div
          style={{
            display: "flex",
            flexDirection: "column",
            justifyContent: "space-between",
            padding: "64px",
            width: "100%",
            height: "100%",
          }}
        >
          <div style={{ display: "flex", flexDirection: "column", gap: 22, maxWidth: 920 }}>
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 14,
                fontSize: 26,
                color: "#075985",
                fontWeight: 700,
                letterSpacing: "0.18em",
                textTransform: "uppercase",
              }}
            >
              <div
                style={{
                  width: 56,
                  height: 56,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  borderRadius: 18,
                  background: "linear-gradient(180deg, rgba(255,255,255,0.96), rgba(186,230,253,0.9))",
                  boxShadow: "inset 0 1px 0 rgba(255,255,255,0.8)",
                  fontSize: 28,
                }}
              >
                ⛸
              </div>
              <span>{kicker}</span>
            </div>

            <div
              style={{
                fontSize: title.length > 36 ? 62 : 74,
                lineHeight: 1.05,
                fontWeight: 800,
                letterSpacing: "-0.04em",
                color: "#0f172a",
              }}
            >
              {title}
            </div>

            {subtitle ? (
              <div
                style={{
                  fontSize: 28,
                  lineHeight: 1.35,
                  color: "#334155",
                  maxWidth: 920,
                }}
              >
                {subtitle}
              </div>
            ) : null}
          </div>

          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", gap: 24 }}>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 12, maxWidth: 840 }}>
              {chips.slice(0, 4).map((chip) => (
                <div
                  key={chip}
                  style={{
                    display: "flex",
                    alignItems: "center",
                    padding: "10px 18px",
                    borderRadius: 9999,
                    background: "rgba(255,255,255,0.82)",
                    border: "1px solid rgba(125,211,252,0.55)",
                    color: "#334155",
                    fontSize: 22,
                    fontWeight: 600,
                  }}
                >
                  {chip}
                </div>
              ))}
            </div>

            <div
              style={{
                display: "flex",
                flexDirection: "column",
                alignItems: "flex-end",
                gap: 8,
              }}
            >
              <div style={{ fontSize: 22, color: "#0f172a", fontWeight: 700 }}>{SITE_NAME}</div>
              <div style={{ fontSize: 18, color: "#475569" }}>public figure-skating data</div>
            </div>
          </div>
        </div>
      </div>
    ),
    OG_IMAGE_SIZE,
  );
}
