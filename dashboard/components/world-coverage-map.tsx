import Link from "next/link";

import { nationFlag } from "@/lib/format";
import type { NationalityRow } from "@/lib/types";

const GEO: Record<string, { x: number; y: number }> = {
  USA: { x: 20, y: 34 },
  CAN: { x: 22, y: 22 },
  MEX: { x: 19, y: 46 },
  BRA: { x: 32, y: 67 },
  ARG: { x: 30, y: 84 },
  GBR: { x: 47, y: 24 },
  IRL: { x: 45, y: 24 },
  ISL: { x: 41, y: 15 },
  POR: { x: 46, y: 38 },
  ESP: { x: 49, y: 39 },
  FRA: { x: 52, y: 32 },
  BEL: { x: 53, y: 29 },
  NED: { x: 54, y: 27 },
  GER: { x: 56, y: 28 },
  DEN: { x: 57, y: 22 },
  NOR: { x: 56, y: 11 },
  SWE: { x: 60, y: 14 },
  FIN: { x: 64, y: 12 },
  EST: { x: 64, y: 20 },
  LAT: { x: 63, y: 23 },
  LTU: { x: 62, y: 26 },
  POL: { x: 60, y: 28 },
  CZE: { x: 58, y: 31 },
  SVK: { x: 61, y: 32 },
  AUT: { x: 59, y: 34 },
  SUI: { x: 55, y: 34 },
  ITA: { x: 57, y: 39 },
  SLO: { x: 60, y: 36 },
  CRO: { x: 61, y: 38 },
  BIH: { x: 62, y: 40 },
  SRB: { x: 64, y: 39 },
  MNE: { x: 63, y: 42 },
  MKD: { x: 65, y: 43 },
  GRE: { x: 66, y: 47 },
  BUL: { x: 67, y: 40 },
  ROU: { x: 67, y: 35 },
  MDA: { x: 69, y: 34 },
  UKR: { x: 71, y: 31 },
  BLR: { x: 69, y: 26 },
  TUR: { x: 72, y: 42 },
  GEO: { x: 77, y: 38 },
  ARM: { x: 78, y: 40 },
  AZE: { x: 80, y: 39 },
  KAZ: { x: 84, y: 27 },
  UZB: { x: 86, y: 34 },
  ISR: { x: 71, y: 49 },
  ARE: { x: 78, y: 54 },
  CHN: { x: 96, y: 33 },
  HKG: { x: 101, y: 41 },
  KOR: { x: 104, y: 31 },
  JPN: { x: 108, y: 32 },
  AUS: { x: 106, y: 76 },
  NZL: { x: 116, y: 84 },
};

function continents() {
  return [
    "M10 16 C18 10, 31 12, 36 22 C33 29, 26 31, 22 39 C17 45, 18 55, 24 60 C27 64, 26 72, 20 82 C15 88, 11 83, 12 75 C14 66, 11 57, 8 49 C5 40, 3 27, 10 16Z",
    "M44 18 C54 13, 67 14, 78 20 C83 16, 92 15, 102 20 C111 25, 121 29, 127 37 C123 42, 116 42, 112 47 C107 52, 100 54, 96 60 C91 66, 83 67, 77 61 C69 54, 60 50, 56 42 C51 35, 44 29, 44 18Z",
    "M86 66 C92 63, 100 64, 107 70 C109 76, 106 82, 102 87 C97 90, 91 90, 87 84 C84 79, 84 72, 86 66Z",
    "M61 73 C66 71, 71 72, 75 76 C76 82, 72 87, 67 90 C63 89, 60 85, 59 80 C58 77, 59 74, 61 73Z",
  ];
}

export function WorldCoverageMap({ rows, take = 24 }: { rows: NationalityRow[]; take?: number }) {
  const points = rows
    .filter((row) => GEO[row.nation_code])
    .sort((a, b) => b.segment_scores - a.segment_scores)
    .slice(0, take);
  const labeled = points.slice(0, 6);

  return (
    <div className="space-y-4">
      <div className="rounded-[1.5rem] border border-slate-800 bg-[radial-gradient(circle_at_30%_20%,rgba(56,189,248,0.18),transparent_26%),radial-gradient(circle_at_72%_28%,rgba(14,165,233,0.12),transparent_24%),linear-gradient(180deg,rgba(2,6,23,0.98),rgba(15,23,42,1))] p-4 md:p-5">
        <div className="mb-3 flex items-center justify-between gap-3">
          <div>
            <p className="text-[11px] uppercase tracking-[0.22em] text-sky-300/80">Coverage Map</p>
            <p className="mt-1 text-sm text-slate-300">Visible country footprint across the indexed skating results.</p>
          </div>
          <div className="rounded-full border border-slate-700/80 bg-slate-950/75 px-3 py-1 text-xs text-slate-300">
            {points.length} plotted
          </div>
        </div>
        <svg viewBox="0 0 128 92" className="h-auto min-h-[280px] w-full" role="img" aria-label="World coverage map">
          <rect x="0" y="0" width="128" height="92" rx="14" fill="rgba(15,23,42,0.3)" />
          {[16, 32, 48, 64, 80].map((y) => (
            <line key={`h-${y}`} x1="6" x2="122" y1={y} y2={y} stroke="rgba(148,163,184,0.08)" strokeWidth="0.5" />
          ))}
          {[20, 40, 60, 80, 100].map((x) => (
            <line key={`v-${x}`} y1="8" y2="84" x1={x} x2={x} stroke="rgba(148,163,184,0.08)" strokeWidth="0.5" />
          ))}
          {continents().map((path, index) => (
            <path
              key={index}
              d={path}
              fill="rgba(51,65,85,0.96)"
              stroke="rgba(203,213,225,0.14)"
              strokeWidth="0.7"
            />
          ))}
          {points.map((row) => {
            const pos = GEO[row.nation_code];
            const size = Math.max(2.2, Math.min(5.4, Math.log10(row.segment_scores + 1) * 1.5));
            return (
              <g key={row.nation_code}>
                <circle cx={pos.x} cy={pos.y} r={size + 1.6} fill="rgba(56,189,248,0.18)" />
                <circle cx={pos.x} cy={pos.y} r={size} fill="rgba(125,211,252,0.95)" />
              </g>
            );
          })}
          {labeled.map((row, index) => {
            const pos = GEO[row.nation_code];
            const dx = pos.x > 92 ? -12 : 3;
            const dy = index % 2 === 0 ? -2.5 : 3.5;
            return (
              <g key={`label-${row.nation_code}`}>
                <text
                  x={pos.x + dx}
                  y={pos.y + dy}
                  fontSize="3.8"
                  fill="rgba(226,232,240,0.92)"
                  textAnchor={dx < 0 ? "end" : "start"}
                >
                  {row.nation_code}
                </text>
              </g>
            );
          })}
        </svg>
      </div>

      <div className="grid gap-2 sm:grid-cols-2">
        {points.slice(0, 8).map((row) => (
          <Link
            key={row.nation_code}
            href={`/nationalities/${encodeURIComponent(row.nation_code)}`}
            className="surface-inset flex items-center justify-between rounded-2xl border p-3"
          >
            <span className="flex items-center gap-2 text-sm text-slate-100">
              <span className="text-base">{nationFlag(row.nation_code)}</span>
              <span>{row.nation_code}</span>
            </span>
            <span className="text-xs text-slate-400">{row.segment_scores.toLocaleString()} scores</span>
          </Link>
        ))}
      </div>
    </div>
  );
}
