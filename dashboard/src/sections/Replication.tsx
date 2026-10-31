import type { Snapshot } from "../types";
import { Badge, Empty, Section, Table } from "../ui";
import { bps, bytes, num, secs, short, when } from "../format";

export function Replication({ s, stale, ageS }: { s: Snapshot; stale: boolean; ageS: number | null }) {
  return (
    <Section id="replication" title="Replication" stale={stale} ageS={ageS}>
      {s.replication.length === 0 ? (
        <Empty>No replication flow exists yet.</Empty>
      ) : (
        <Table
          label="Replication flows"
          head={[
            "Topic",
            "Source cluster",
            "Target cluster",
            "Status",
            "Last source offset",
            "Last destination offset",
            "Lag (records)",
            "Lag (est. drain)",
            "Backlog",
            "Throughput",
            "Last acknowledgement",
          ]}
        >
          {s.replication.map((r) => (
            <tr key={r.flow_id} data-testid={`flow-${r.flow_id}`}>
              <td>{r.topic}</td>
              <td>
                {r.source_cluster} ({short(r.source_region)})
              </td>
              <td>
                {r.target_cluster} ({short(r.target_region)})
              </td>
              <td>
                {r.status}{" "}
                {r.pipeline_states.length > 0 && (
                  <Badge tone={r.pipeline_states.every((x) => x === "RUNNING") ? "ok" : "warn"}>{r.pipeline_states.join("/")}</Badge>
                )}
              </td>
              <td>{num(r.last_source_offset)}</td>
              <td>{num(r.last_destination_offset)}</td>
              <td data-testid={`lag-records-${r.flow_id}`}>{num(r.lag_records)}</td>
              <td>{r.lag_seconds === null ? "unknown" : secs(r.lag_seconds)}</td>
              <td data-testid={`backlog-${r.flow_id}`}>{bytes(r.backlog_bytes)}</td>
              <td>{bps(r.throughput_bps)}</td>
              <td>{when(r.last_ack_at)}</td>
            </tr>
          ))}
        </Table>
      )}
      <p className="muted">
        Lag is the replicator's own report (records) and its estimate of the time to drain the backlog; "unknown" means it cannot be estimated. Offsets are the
        highest over the topic's partitions.
      </p>
    </Section>
  );
}
