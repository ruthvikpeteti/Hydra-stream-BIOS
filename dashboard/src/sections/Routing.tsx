import type { Snapshot } from "../types";
import { Badge, Empty, Section, Table } from "../ui";
import { bps, ms, num, pct, short, when } from "../format";

export function Routing({ s, stale, ageS }: { s: Snapshot; stale: boolean; ageS: number | null }) {
  return (
    <Section id="routing" title="Adaptive network routing" stale={stale} ageS={ageS}>
      {s.routing.length === 0 ? (
        <Empty>No route is being measured: there are no running replication flows.</Empty>
      ) : (
        s.routing.map((r) => (
          <div key={`${r.source_region}-${r.target_region}`} data-testid={`route-${r.source_region}-${r.target_region}`}>
            <h3>
              {short(r.source_region)} → {short(r.target_region)}
            </h3>
            <p>
              Selected path <strong data-testid={`selected-${r.source_region}-${r.target_region}`}>{r.selected_path ?? "none"}</strong> · last route change{" "}
              {when(r.last_route_change)} · {r.route_changes} change(s) · reason {r.reason ?? "n/a"}
            </p>
            <Table
              label={`Paths ${short(r.source_region)} to ${short(r.target_region)}`}
              head={["Path", "Kind", "Selected", "RTT", "Loss", "Bandwidth", "Score", "Circuit breaker", "Excluded"]}
            >
              {r.paths.map((p) => (
                <tr key={p.path_id} className={p.selected ? "selected" : ""}>
                  <td>{p.path_id}</td>
                  <td>{p.kind ?? "n/a"}</td>
                  <td>{p.selected ? "yes" : "no"}</td>
                  <td>{ms(p.rtt_ms)}</td>
                  <td>{pct(p.loss)}</td>
                  <td>{bps(p.bandwidth_bps)}</td>
                  <td>{p.score === null ? "n/a" : p.score.toFixed(3)}</td>
                  <td>
                    <Badge tone={p.breaker === "CLOSED" ? "ok" : p.breaker === "HALF_OPEN" ? "warn" : "bad"}>{p.breaker}</Badge>
                  </td>
                  <td>{p.excluded ?? "no"}</td>
                </tr>
              ))}
            </Table>
            <p className="muted">
              Scores: lower is better. Loss is the share of failed probes and sends, measured at the application level (not ICMP). {num(r.paths.length)}{" "}
              path(s).
            </p>
          </div>
        ))
      )}
    </Section>
  );
}
