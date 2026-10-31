import { useState } from "react";
import type { Snapshot } from "../types";
import { Badge, Empty, Section, Table } from "../ui";
import { when } from "../format";

const CATS = ["all", "policy", "replication", "route", "failover", "failback", "admin"] as const;

export function Audit({ s, stale, ageS }: { s: Snapshot; stale: boolean; ageS: number | null }) {
  const [cat, setCat] = useState<(typeof CATS)[number]>("all");
  const rows = s.audit.filter((a) => cat === "all" || a.category === cat);
  return (
    <Section id="audit" title="Audit logs" stale={stale} ageS={ageS}>
      <label>
        Category{" "}
        <select aria-label="Audit category" value={cat} onChange={(e) => setCat(e.target.value as (typeof CATS)[number])}>
          {CATS.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>
      </label>
      {rows.length === 0 ? (
        <Empty>No audit entries in this category.</Empty>
      ) : (
        <Table label="Audit entries" head={["When", "Category", "Operation", "Decision", "Topic", "Region", "By", "Detail"]}>
          {rows.map((a) => (
            <tr key={String(a.id)}>
              <td>{when(a.ts)}</td>
              <td>{a.category}</td>
              <td>{a.operation}</td>
              <td>
                <Badge tone={a.decision === "ALLOWED" ? "ok" : "bad"}>{a.decision}</Badge>
              </td>
              <td>{a.topic ?? "n/a"}</td>
              <td>{a.region ?? "n/a"}</td>
              <td>{a.requester ?? "n/a"}</td>
              <td>{a.detail ?? (a.reasons.join(", ") || "n/a")}</td>
            </tr>
          ))}
        </Table>
      )}
    </Section>
  );
}
