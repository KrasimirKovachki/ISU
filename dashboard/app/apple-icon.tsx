import { ImageResponse } from "next/og";

export const size = {
  width: 180,
  height: 180,
};

export const contentType = "image/png";

export default function AppleIcon() {
  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          background: "linear-gradient(155deg, #f8fbff 0%, #dbeafe 55%, #7dd3fc 100%)",
          color: "#075985",
          fontSize: 96,
          fontWeight: 800,
          fontFamily: "Avenir Next, Segoe UI, Arial, sans-serif",
        }}
      >
        ⛸
      </div>
    ),
    size,
  );
}
