import { ImageResponse } from "next/og";

export const size = {
  width: 512,
  height: 512,
};

export const contentType = "image/png";

export default function Icon() {
  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          position: "relative",
          overflow: "hidden",
          background: "linear-gradient(155deg, #f8fbff 0%, #dbeafe 52%, #7dd3fc 100%)",
          fontFamily: "Avenir Next, Segoe UI, Arial, sans-serif",
        }}
      >
        <div
          style={{
            position: "absolute",
            inset: 0,
            backgroundImage:
              "linear-gradient(rgba(255,255,255,0.35) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.35) 1px, transparent 1px)",
            backgroundSize: "28px 28px",
            opacity: 0.55,
          }}
        />
        <div
          style={{
            width: 352,
            height: 352,
            borderRadius: 88,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            background: "linear-gradient(180deg, rgba(255,255,255,0.98), rgba(224,242,254,0.9))",
            boxShadow: "0 30px 90px rgba(14,165,233,0.18), inset 0 1px 0 rgba(255,255,255,0.85)",
            color: "#075985",
            fontSize: 190,
            fontWeight: 800,
          }}
        >
          ⛸
        </div>
      </div>
    ),
    size,
  );
}
