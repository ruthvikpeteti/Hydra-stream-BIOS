export const na = "n/a";
export const num = (v: number | null | undefined) => (v === null || v === undefined ? na : String(v));
export function bytes(v: number | null | undefined): string {
  if (v === null || v === undefined) return na;
  if (v < 1024) return `${v} B`;
  if (v < 1048576) return `${(v / 1024).toFixed(1)} KiB`;
  return `${(v / 1048576).toFixed(1)} MiB`;
}
export const secs = (v: number | null | undefined) => (v === null || v === undefined ? na : `${v.toFixed(1)} s`);
export const ms = (v: number | null | undefined) => (v === null || v === undefined ? na : `${v.toFixed(1)} ms`);
export const pct = (v: number | null | undefined) => (v === null || v === undefined ? na : `${(v * 100).toFixed(0)} %`);
export const bps = (v: number | null | undefined) => (v === null || v === undefined ? na : `${bytes(v)}/s`);
export const short = (r: string | null | undefined) => (r ? r.replace("region-", "").toUpperCase() : na);
export const when = (iso: string | null | undefined) => (iso ? iso.replace("T", " ").replace("+00:00", " UTC") : na);
