import type { ReactNode } from "react";

export function Badge({ tone, children, title }: { tone: "ok" | "warn" | "bad" | "info" | "muted"; children: ReactNode; title?: string }) {
  return (
    <span className={`badge badge-${tone}`} title={title}>
      {children}
    </span>
  );
}

export const healthTone = (h: string): "ok" | "warn" | "bad" | "muted" => (h === "UP" ? "ok" : h === "DEGRADED" ? "warn" : h === "DOWN" ? "bad" : "muted");

export function Section({ id, title, stale, ageS, children }: { id: string; title: string; stale: boolean; ageS: number | null; children: ReactNode }) {
  return (
    <section id={id} aria-label={title} data-stale={stale ? "true" : "false"} className={stale ? "section stale" : "section"}>
      <header className="section-head">
        <h2>{title}</h2>
        <span className="muted" data-testid={`${id}-age`}>
          {stale ? "STALE · " : ""}
          {ageS === null ? "no data yet" : `updated ${ageS}s ago`}
        </span>
      </header>
      {children}
    </section>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return (
    <p className="empty" role="note">
      {children}
    </p>
  );
}

export function Table({ head, children, label }: { head: string[]; children: ReactNode; label: string }) {
  return (
    <div className="table-wrap">
      <table aria-label={label}>
        <thead>
          <tr>
            {head.map((h) => (
              <th key={h}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>{children}</tbody>
      </table>
    </div>
  );
}

export function KV({ k, children, testid }: { k: string; children: ReactNode; testid?: string }) {
  return (
    <div className="kv">
      <dt>{k}</dt>
      <dd data-testid={testid}>{children}</dd>
    </div>
  );
}
