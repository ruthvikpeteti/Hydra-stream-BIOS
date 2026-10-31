import type { Snapshot } from "../types";
import { Badge, Empty, Section, Table, healthTone } from "../ui";
import { num, short } from "../format";

export function Topology({ s, stale, ageS }: { s: Snapshot; stale: boolean; ageS: number | null }) {
  const t = s.topology;
  return (
    <Section id="topology" title="Global topology" stale={stale} ageS={ageS}>
      <p>
        Active region <strong data-testid="active-region">{t.active_region ? short(t.active_region) : "none (no region is authorised to write)"}</strong> ·
        leadership epoch <strong data-testid="epoch">{num(t.leadership_epoch)}</strong> · state <strong>{t.phase ?? "unknown"}</strong>
      </p>
      <div className="cards">
        {t.regions.map((r) => (
          <article key={r.region} className="card" data-testid={`region-${r.region}`} aria-label={`Region ${short(r.region)}`}>
            <h3>
              Region {short(r.region)} <Badge tone={r.role === "active" ? "ok" : r.role === "fenced" ? "bad" : "info"}>{r.role}</Badge>
            </h3>
            <dl>
              <div className="kv">
                <dt>Health</dt>
                <dd>
                  <Badge tone={healthTone(r.declared_health)}>{r.declared_health}</Badge>
                </dd>
              </div>
              <div className="kv">
                <dt>Brokers</dt>
                <dd data-testid={`brokers-${r.region}`}>{r.brokers_up === null ? "unknown" : `${r.brokers_up} of ${r.brokers_expected} up`}</dd>
              </div>
              <div className="kv">
                <dt>Controller quorum</dt>
                <dd>{r.controller_quorum === null ? "unknown" : r.controller_quorum ? "operational" : "not operational"}</dd>
              </div>
              <div className="kv">
                <dt>Gateway</dt>
                <dd>{r.gateway_healthy === null ? "unknown" : r.gateway_healthy ? "healthy" : "unhealthy"}</dd>
              </div>
              <div className="kv">
                <dt>Replicator</dt>
                <dd>{r.replicator_up ? "up" : "not answering"}</dd>
              </div>
              {r.fenced_at_epoch !== null && (
                <div className="kv">
                  <dt>Fenced at epoch</dt>
                  <dd>{r.fenced_at_epoch}</dd>
                </div>
              )}
            </dl>
          </article>
        ))}
      </div>
      <h3>Replication connections</h3>
      {t.connections.length === 0 ? (
        <Empty>No replication flows exist yet.</Empty>
      ) : (
        <Table label="Replication connections" head={["From", "To", "Flows", "States", "Active route"]}>
          {t.connections.map((c) => (
            <tr key={`${c.source_region}-${c.target_region}`}>
              <td>{short(c.source_region)}</td>
              <td>{short(c.target_region)}</td>
              <td>{c.flows}</td>
              <td>
                {Object.entries(c.states)
                  .map(([k, v]) => `${k}: ${v}`)
                  .join(", ")}
              </td>
              <td>{c.route ?? "none"}</td>
            </tr>
          ))}
        </Table>
      )}
    </Section>
  );
}
