import { useState } from "react";
import type { Api } from "../api";
import { ApiError } from "../api";
import type { DR, Me, Snapshot } from "../types";
import { Badge, Empty, KV, Section, Table } from "../ui";
import { num, secs, short, when } from "../format";

type Op = "failover" | "failback/start" | "failback/approve" | "failback/abort";

/** Administrative controls. They exist only for the operator role; the API enforces the same rule. The panel never claims success: it
 *  shows the DR controller's own answer, and the state sections change only when the coordinator reports a new state. */
export function AdminPanel({ me, api, dr }: { me: Me | null; api: Api; dr: DR }) {
  const [pending, setPending] = useState<{ op: Op; body: object; label: string } | null>(null);
  const [answer, setAnswer] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [degraded, setDegraded] = useState(false);
  const [region, setRegion] = useState("");
  const fenced = Object.keys(dr.fenced);

  if (!me?.can_administer) {
    return (
      <p className="muted" data-testid="admin-readonly">
        Administrative controls need the operator role. You are signed in as {me?.role ?? "unknown"}: this view is read-only.
      </p>
    );
  }
  async function send() {
    if (!pending) return;
    setBusy(true);
    try {
      const r = await api.admin(pending.op, pending.body);
      const a = r.controller_answer as Record<string, unknown>;
      setAnswer(
        `The DR controller answered: ${String(a.result ?? "no result")}${a.reason ? ` (${String(a.reason)})` : ""}${a.detail ? ` — ${String(a.detail)}` : ""}. This page changes only when the coordinator reports a new state.`,
      );
    } catch (e) {
      setAnswer(e instanceof ApiError ? `Refused by the dashboard API (${e.status}): ${e.message}` : "The request failed");
    } finally {
      setBusy(false);
      setPending(null);
    }
  }
  return (
    <div className="admin" aria-label="Administrative operations">
      <h3>Administrative operations</h3>
      <div className="row">
        <button
          onClick={() =>
            setPending({
              op: "failover",
              body: { accept_degraded_rpo: degraded },
              label: degraded ? "request a failover accepting degraded RPO" : "request a failover",
            })
          }
        >
          Request failover
        </button>
        <label>
          <input type="checkbox" checked={degraded} disabled={!dr.controller.allow_degraded_rpo} onChange={(e) => setDegraded(e.target.checked)} /> accept
          degraded RPO{!dr.controller.allow_degraded_rpo && " (not permitted by this deployment)"}
        </label>
      </div>
      <div className="row">
        <select aria-label="Region to fail back to" value={region} onChange={(e) => setRegion(e.target.value)} disabled={fenced.length === 0}>
          <option value="">{fenced.length === 0 ? "no fenced region" : "choose a fenced region"}</option>
          {fenced.map((r) => (
            <option key={r} value={r}>
              {short(r)}
            </option>
          ))}
        </select>
        <button
          disabled={!region || dr.failback !== null}
          onClick={() => setPending({ op: "failback/start", body: { region }, label: `start the failback to ${short(region)}` })}
        >
          Start failback
        </button>
        {dr.failback?.stage === "READY" && (
          <button onClick={() => setPending({ op: "failback/approve", body: {}, label: `approve the failback to ${short(dr.failback!.region)}` })}>
            Approve failback
          </button>
        )}
        {dr.failback && dr.failback.stage !== "SWITCHING" && (
          <button onClick={() => setPending({ op: "failback/abort", body: {}, label: "abort the failback" })}>Abort failback</button>
        )}
      </div>
      {pending && (
        <div role="alertdialog" aria-label="Confirm administrative operation" className="confirm">
          <p>
            You are about to {pending.label} as <strong>{me.user}</strong>. The request is audited.
          </p>
          <button onClick={send} disabled={busy}>
            Confirm
          </button>{" "}
          <button onClick={() => setPending(null)} disabled={busy}>
            Cancel
          </button>
        </div>
      )}
      {answer && (
        <p role="status" data-testid="admin-answer">
          {answer}
        </p>
      )}
    </div>
  );
}

export function DisasterRecovery({ s, stale, ageS, me, api }: { s: Snapshot; stale: boolean; ageS: number | null; me: Me | null; api: Api }) {
  const d = s.dr;
  const lf = d.last_failover;
  return (
    <Section id="dr" title="Disaster recovery" stale={stale} ageS={ageS}>
      {!d.coordination_reachable && (
        <p role="alert" className="warn">
          The coordinator is not reachable from the dashboard: the state below is the last one seen, not current.
        </p>
      )}
      <dl className="grid">
        <KV k="Active region" testid="dr-active">
          {d.active_region ? short(d.active_region) : "none (fencing in progress or unknown)"}
        </KV>
        <KV k="Standby regions">{d.standby_regions.length ? d.standby_regions.map(short).join(", ") : "none"}</KV>
        <KV k="Failover state" testid="dr-phase">
          <Badge tone={d.phase?.startsWith("HEALTHY") ? "ok" : d.phase ? "warn" : "muted"}>{d.phase ?? "unknown"}</Badge>
        </KV>
        <KV k="Leadership epoch" testid="dr-epoch">
          {num(d.leadership_epoch)}
        </KV>
        <KV k="Controller">
          {d.controller.instance ?? "n/a"} {d.controller.is_leader ? "(leader)" : ""}
        </KV>
        <KV k="Actual RPO" testid="dr-actual-rpo">
          {d.actual_rpo
            ? `${d.actual_rpo.records} acknowledged record(s) missing from the live history (kept in quarantine)`
            : "not known: it can only be measured after the old region returns and is reconciled"}
        </KV>
      </dl>
      {d.transition && (
        <p role="status">
          Transition in progress: {short(d.transition.from)} → {short(d.transition.to)}, step {d.transition.step}, epoch {d.transition.epoch}
          {d.transition.degraded_rpo ? " (degraded RPO, approved)" : ""}.
        </p>
      )}
      <h3>Last failover event</h3>
      {!lf ? (
        <Empty>No failover has happened since this system started.</Empty>
      ) : (
        <dl className="grid" data-testid="last-failover">
          <KV k="Movement">
            {short(lf.from)} → {short(lf.to)} at epoch {lf.epoch}
          </KV>
          <KV k="Activated">{when(lf.activated_at)}</KV>
          <KV k="Measured RTO" testid="dr-rto">
            {secs(lf.measured_rto_s)} (of which lease wait {secs(lf.fence_wait_s)})
          </KV>
          <KV k="Replication reconfigured after">{secs(lf.replication_reconfigured_s)}</KV>
          <KV k="Expected RPO bound at decision">{secs(lf.expected_rpo_bound_s)}</KV>
          <KV k="Degraded-RPO approval">{lf.degraded_rpo ? `approved by ${lf.approved_by}` : "none"}</KV>
        </dl>
      )}
      <h3>Candidate eligibility</h3>
      {Object.keys(d.candidate_eligibility).length === 0 ? (
        <Empty>Eligibility is evaluated only after a failure of the active region.</Empty>
      ) : (
        <Table label="Candidate eligibility" head={["Region", "Eligible", "Reasons", "Topics checked"]}>
          {Object.entries(d.candidate_eligibility).map(([r, e]) => (
            <tr key={r}>
              <td>{short(r)}</td>
              <td>
                <Badge tone={e.eligible ? "ok" : "bad"}>{e.eligible ? "eligible" : "blocked"}</Badge>
              </td>
              <td>{e.reasons.join(", ") || "none"}</td>
              <td>{num(e.topics_checked)}</td>
            </tr>
          ))}
        </Table>
      )}
      <h3>Fence status</h3>
      {Object.keys(d.fenced).length === 0 ? (
        <Empty>No region is fenced.</Empty>
      ) : (
        <Table label="Fenced regions" head={["Region", "Fenced at epoch", "Meaning"]}>
          {Object.entries(d.fenced).map(([r, e]) => (
            <tr key={r}>
              <td>{short(r)}</td>
              <td>{e}</td>
              <td>cannot accept writes until a controlled failback</td>
            </tr>
          ))}
        </Table>
      )}
      {d.failback && (
        <p role="status" data-testid="failback-status">
          Failback to {short(d.failback.region)}: stage {d.failback.stage}, requested by {d.failback.requested_by}
          {d.failback.approved_by ? `, approved by ${d.failback.approved_by}` : ", awaiting approval at READY"}.
        </p>
      )}
      <AdminPanel me={me} api={api} dr={d} />
    </Section>
  );
}
