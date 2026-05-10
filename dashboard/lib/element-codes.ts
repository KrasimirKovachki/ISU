/**
 * ISU element-code → human label dictionary. Mirrors the vanilla dashboard's
 * `describeElement` so labels are consistent across the two UIs.
 *
 * Markers (<<, <, !, e, q, *, F, x, b, REP) are surfaced as separate badges
 * on the score-detail page; this helper just decodes the structural code
 * (rotations + jump letter, spin prefix + level, step prefix + level).
 */

const JUMP_LETTERS: Record<string, string> = {
  A: "Axel", F: "Flip", Lz: "Lutz", Lo: "Loop", S: "Salchow", T: "Toe Loop", Eu: "Euler",
};
const ROTATION_NAMES = ["", "Single", "Double", "Triple", "Quad"];

const SPIN_PREFIXES: Array<[string, string]> = [
  ["FCCoSp", "Flying Change Combination Spin"],
  ["CCoSp", "Change Combination Spin"],
  ["CoSp", "Combination Spin"],
  ["FCSSp", "Flying Change Sit Spin"],
  ["FCLSp", "Flying Change Layback Spin"],
  ["FCCSp", "Flying Change Camel Spin"],
  ["FSSp", "Flying Sit Spin"],
  ["FCSp", "Flying Camel Spin"],
  ["FLSp", "Flying Layback Spin"],
  ["CSSp", "Change Sit Spin"],
  ["CLSp", "Change Layback Spin"],
  ["CCSp", "Change Camel Spin"],
  ["SSp", "Sit Spin"],
  ["LSp", "Layback Spin"],
  ["CSp", "Camel Spin"],
  ["USp", "Upright Spin"],
];
const STEP_PREFIXES: Array<[string, string]> = [
  ["StSq", "Step Sequence"],
  ["ChSq", "Choreographic Sequence"],
  ["ChSt", "Choreographic Step Sequence"],
  ["ChSp", "Choreographic Spin"],
  ["SpSq", "Spiral Sequence"],
  ["PiF",  "Pivot Figure"],
];

const MARKER_TAIL = /(?:<<|<|!|e|q|\*|F|x|b)+$/;

function describeToken(token: string): string {
  const raw = token.trim();
  if (!raw) return "";
  if (/^SEQ$/i.test(raw)) return "Jump Sequence";
  if (/^REP$/i.test(raw)) return "Repetition";
  // Jump: leading digit + 1-2 letter jump code (longest letter prefix wins).
  const jump = raw.match(/^(\d)(Lz|Lo|Eu|A|F|S|T)/);
  if (jump) {
    const rotation = ROTATION_NAMES[Number(jump[1])] ?? `${jump[1]}-rotation`;
    return `${rotation} ${JUMP_LETTERS[jump[2]] ?? jump[2]}`;
  }
  for (const [prefix, label] of SPIN_PREFIXES) {
    if (raw.startsWith(prefix)) {
      const tail = raw.slice(prefix.length).replace(MARKER_TAIL, "");
      const level = tail.replace(/V$/i, "");
      const variation = /V$/i.test(tail) ? " (feature variation)" : "";
      return `${label}${level ? ` lvl ${level}` : ""}${variation}`;
    }
  }
  for (const [prefix, label] of STEP_PREFIXES) {
    if (raw.startsWith(prefix)) {
      const tail = raw.slice(prefix.length).replace(MARKER_TAIL, "");
      return `${label}${tail ? ` lvl ${tail}` : ""}`;
    }
  }
  return raw;
}

export function describeElement(code: string | null | undefined): string {
  const raw = String(code ?? "").trim();
  if (!raw) return "";
  return raw
    .split("+")
    .filter(Boolean)
    .map(describeToken)
    .filter(Boolean)
    .join(" + ");
}

/** Distinct base codes appearing in `elements`, decoded for a legend strip. */
export function elementLegendEntries(
  elements: { element_code?: string | null; base_element_code?: string | null }[],
): Array<{ code: string; description: string }> {
  const seen = new Map<string, string>();
  for (const e of elements) {
    const code = e.base_element_code || e.element_code || "";
    if (!code || seen.has(code)) continue;
    const description = describeElement(code);
    if (description && description !== code) seen.set(code, description);
  }
  return [...seen.entries()].map(([code, description]) => ({ code, description }));
}
