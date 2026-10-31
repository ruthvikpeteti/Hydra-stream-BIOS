import type { Snapshot } from "../types";
import { Badge, Empty, Section, Table } from "../ui";
import { short, when } from "../format";

export function Policies({ s, stale, ageS }: { s: Snapshot; stale: boolean; ageS: number | null }) {
  const { policies, recent_blocked } = s.policies;
  return (
    <Section id="policies" title="Residency policies" stale={stale} ageS={ageS}>
      {policies.length === 0 ? (
        <Empty>No topic has a residency policy yet.</Empty>
      ) : (
        <Table
          label="Topic policies"
          head={["Topic", "Classification", "Legal zone", "Allowed regions", "Prohibited regions", "Replica placement", "Policy version"]}
        >
          {policies.map((p) => (
            <tr key={p.topic}>
              <td>{p.topic}</td>
              <td>
                <Badge tone={p.classification === "RESTRICTED" ? "bad" : p.classification === "CONFIDENTIAL" ? "warn" : "info"}>{p.classification}</Badge>
              </td>
              <td>{p.legal_zone}</td>
              <td>{p.allowed_storage_regions.map(short).join(", ")}</td>
              <td>{p.prohibited_regions.map(short).join(", ") || "none"}</td>
              <td>{p.replicas.length === 0 ? "none" : p.replicas.map((r) => `${short(r.region)}: ${r.status}`).join(", ")}</td>
              <td>v{p.policy_version}</td>
            </tr>
          ))}
        </Table>
      )}
      <h3>Recent blocked operations</h3>
      {recent_blocked.length === 0 ? (
        <Empty>No operation has been blocked recently.</Empty>
      ) : (
        <Table label="Recent blocked operations" head={["When", "Operation", "Decision", "Topic", "Reasons"]}>
          {recent_blocked.map((a) => (
            <tr key={String(a.id)}>
              <td>{when(a.ts)}</td>
              <td>{a.operation}</td>
              <td>
                <Badge tone="bad">{a.decision}</Badge>
              </td>
              <td>{a.topic ?? "n/a"}</td>
              <td>{a.reasons.join(", ") || "n/a"}</td>
            </tr>
          ))}
        </Table>
      )}
    </Section>
  );
}
