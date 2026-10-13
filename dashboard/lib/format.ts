/** Tiny formatting helpers used across pages. */
export function points(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  return value.toFixed(2);
}

export function number(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  return value.toLocaleString();
}

export function fmt<T>(value: T | null | undefined): T | "—" {
  if (value === null || value === undefined || (typeof value === "string" && value === "")) return "—";
  return value;
}

export function formatDate(value: string | null | undefined): string {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en", { year: "numeric", month: "short", day: "numeric" }).format(date);
}

/** ISO3 → emoji flag (best-effort; falls back to bare code). */
export function nationFlag(nationCode: string | null | undefined): string {
  if (!nationCode) return "";
  const ISO3_TO_ISO2: Record<string, string> = {
    AND: "AD", ARE: "AE", ARG: "AR", ARM: "AM", AUS: "AU", AUT: "AT", AZE: "AZ", BEL: "BE", BGR: "BG", BIH: "BA",
    BLR: "BY", BRA: "BR", BUL: "BG", CAN: "CA", CHE: "CH", CHN: "CN", CRO: "HR", CYP: "CY", CZE: "CZ", DEN: "DK",
    ESP: "ES", EST: "EE", FIN: "FI", FRA: "FR", GBR: "GB", GEO: "GE", GER: "DE", GRE: "GR", HKG: "HK", HUN: "HU",
    IRL: "IE", ISL: "IS", ISR: "IL", ITA: "IT", JPN: "JP", KAZ: "KZ", KOR: "KR", LAT: "LV", LTU: "LT", LUX: "LU",
    MDA: "MD", MEX: "MX", MKD: "MK", MNE: "ME", NED: "NL", NOR: "NO", NZL: "NZ", POL: "PL", POR: "PT", ROU: "RO",
    RUS: "RU", SRB: "RS", SLO: "SI", SVK: "SK", SUI: "CH", SWE: "SE", TUR: "TR", UKR: "UA", USA: "US", UZB: "UZ",
  };
  const iso2 = ISO3_TO_ISO2[nationCode.toUpperCase()];
  if (!iso2 || iso2.length !== 2) return "";
  const A = "A".charCodeAt(0);
  return String.fromCodePoint(...iso2.split("").map((c) => 0x1f1e6 + (c.charCodeAt(0) - A)));
}

/** "BUL" -> "🇧🇬 BUL" */
export function nationDisplay(nationCode: string | null | undefined): string {
  if (!nationCode) return "—";
  const flag = nationFlag(nationCode);
  return flag ? `${flag} ${nationCode}` : nationCode;
}
