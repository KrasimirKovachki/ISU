import type { SVGProps } from "react";

type IconProps = SVGProps<SVGSVGElement>;

export function SkatingBladeIcon(props: IconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" {...props}>
      <path d="M6 6.5h7.2c1.9 0 3.5 1.6 3.5 3.5v2.2c0 .9.5 1.8 1.3 2.2l.7.4" />
      <path d="M6 6.5V12c0 2.2 1.8 4 4 4h7.8" />
      <path d="M5 17.5h13.5c.8 0 1.5.7 1.5 1.5 0 1.7-1.3 3-3 3H8.4c-1.8 0-3.3-1-4-2.6l-.4-1" />
      <path d="M9.5 10.5h2.5" />
    </svg>
  );
}

export function IceRinkIcon(props: IconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" {...props}>
      <rect x="3" y="5" width="18" height="14" rx="5" />
      <path d="M8.5 5v14" />
      <path d="M15.5 5v14" />
      <path d="M3 12h18" />
      <path d="M10.4 12a1.6 1.6 0 1 1 3.2 0a1.6 1.6 0 1 1-3.2 0Z" />
    </svg>
  );
}

export function ScoreSheetIcon(props: IconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" {...props}>
      <path d="M7 3.5h7l4 4V20a1.5 1.5 0 0 1-1.5 1.5h-9A1.5 1.5 0 0 1 6 20V5A1.5 1.5 0 0 1 7.5 3.5Z" />
      <path d="M14 3.5V8h4" />
      <path d="M9 12h6" />
      <path d="M9 15.5h6" />
      <path d="M9 19h3.5" />
    </svg>
  );
}

export function MedalRibbonIcon(props: IconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" {...props}>
      <path d="M8.2 3h2.8l1 4.5H9.2L8.2 3Z" />
      <path d="M13 3h2.8l-1 4.5H12L13 3Z" />
      <circle cx="12" cy="14" r="4.5" />
      <path d="m10.6 14 1 1 1.8-2" />
    </svg>
  );
}

export function SkateSparkIcon(props: IconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" {...props}>
      <path d="M8 7.5h6.6c1.7 0 3 1.3 3 3v1.4c0 .8.4 1.4 1.1 1.8l.9.4" />
      <path d="M8 7.5V11c0 2.5 2 4.5 4.5 4.5h6" />
      <path d="M5.3 17.5H18c1.1 0 2 .9 2 2 0 1.4-1.1 2.5-2.5 2.5h-7.8c-1.7 0-3.2-.9-4-2.4l-.5-.9" />
      <path d="m5 4 1 1" />
      <path d="m18.5 4.5.7-1.5" />
      <path d="m20 9 1.5.5" />
    </svg>
  );
}
