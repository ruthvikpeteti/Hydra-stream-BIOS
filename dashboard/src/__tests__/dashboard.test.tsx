import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "../App";
import { backend, jsonResponse, snapshot } from "./fixture";

async function signedIn(f: typeof fetch) {
  sessionStorage.setItem("ss-dashboard-token", "tok");
  render(<App fetchImpl={f} />);
  await screen.findByText("SovereignStream operations");
}

beforeEach(() => {
  sessionStorage.clear();
});
afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe("states", () => {
  it("shows a loading state before the first snapshot", async () => {
    let release: (r: Response) => void = () => {};
    const f = (async (u: RequestInfo | URL) =>
      String(u).endsWith("/me")
        ? jsonResponse({ user: "t", role: "viewer", can_administer: false })
        : new Promise<Response>((r) => {
            release = r;
          })) as typeof fetch;
    await signedIn(f);
    expect(await screen.findByText(/Loading the current state/)).toBeInTheDocument();
    release(jsonResponse(snapshot()));
    expect(await screen.findByLabelText("Global topology")).toBeInTheDocument();
  });

  it("shows an error state, with retry, when the backend has no snapshot", async () => {
    const { f } = backend({ snap: () => jsonResponse({ error: "NOT_READY" }, 503) });
    await signedIn(f);
    expect(await screen.findByTestId("error-banner")).toHaveTextContent("still collecting");
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
  });

  it("shows empty states instead of blank tables", async () => {
    const empty = snapshot({ policies: { policies: [], recent_blocked: [] }, routing: [], replication: [], audit: [] });
    empty.topology.connections = [];
    empty.dr.candidate_eligibility = {};
    empty.dr.fenced = {};
    empty.dr.last_failover = null;
    const { f } = backend({ snap: () => empty });
    await signedIn(f);
    for (const text of [
      /No topic has a residency policy/,
      /No operation has been blocked/,
      /No route is being measured/,
      /No replication flow exists/,
      /No audit entries/,
      /No failover has happened/,
      /No region is fenced/,
      /No replication flows exist yet/,
    ]) {
      expect(await screen.findByText(text)).toBeInTheDocument();
    }
  });
});

describe("accuracy: the screen shows what the backend says", () => {
  it("broker health, active region and epoch", async () => {
    const { f } = backend({ snap: () => snapshot() });
    await signedIn(f);
    expect(await screen.findByTestId("brokers-region-a")).toHaveTextContent("2 of 3 up");
    expect(screen.getByTestId("brokers-region-b")).toHaveTextContent("1 of 1 up");
    expect(within(screen.getByTestId("region-region-c")).getByText("DOWN")).toBeInTheDocument();
    expect(screen.getByTestId("active-region")).toHaveTextContent("A");
    expect(screen.getByTestId("epoch")).toHaveTextContent("4");
    expect(screen.getByTestId("dr-active")).toHaveTextContent("A");
    expect(screen.getByTestId("dr-epoch")).toHaveTextContent("4");
  });

  it("replication lag and backlog equal the backend values; unknown stays unknown", async () => {
    const s = snapshot();
    s.replication.push({ ...s.replication[0], flow_id: 2, lag_records: 900, lag_seconds: null, backlog_bytes: 180000 });
    const { f } = backend({ snap: () => s });
    await signedIn(f);
    expect(await screen.findByTestId("lag-records-1")).toHaveTextContent("40");
    expect(screen.getByTestId("backlog-1")).toHaveTextContent("8.0 KiB");
    expect(within(screen.getByTestId("flow-1")).getByText("2.5 s")).toBeInTheDocument();
    expect(within(screen.getByTestId("flow-2")).getByText("unknown")).toBeInTheDocument();
  });

  it("a route change appears only after the backend reports it", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    let current = "a-b-direct";
    const { f } = backend({
      snap: () => {
        const s = snapshot();
        s.routing[0] = { ...s.routing[0], selected_path: current, route_changes: current === "a-b-direct" ? 3 : 4 };
        return s;
      },
    });
    sessionStorage.setItem("ss-dashboard-token", "tok");
    render(<App fetchImpl={f} />);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10);
    });
    expect(screen.getByTestId("selected-region-a-region-b")).toHaveTextContent("a-b-direct");
    current = "a-b-relay"; // the backend changes its mind; the UI has not been told yet
    expect(screen.getByTestId("selected-region-a-region-b")).toHaveTextContent("a-b-direct");
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2100);
    }); // next poll
    expect(screen.getByTestId("selected-region-a-region-b")).toHaveTextContent("a-b-relay");
  });

  it("shows RTT, loss, bandwidth, score and circuit-breaker state of every path", async () => {
    const { f } = backend({ snap: () => snapshot() });
    await signedIn(f);
    const table = await screen.findByRole("table", { name: /Paths A to B/ });
    const peering = within(table).getByText("a-b-peering").closest("tr")!;
    expect(peering).toHaveTextContent("120.0 ms");
    expect(peering).toHaveTextContent("40 %");
    expect(peering).toHaveTextContent("OPEN");
    expect(peering).toHaveTextContent("0.310");
  });
});

describe("staleness", () => {
  it("marks the dashboard stale when updates stop, and keeps the old data visibly marked", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    let up = true;
    const { f } = backend({ snap: () => (up ? snapshot() : jsonResponse({ detail: "down" }, 502)) });
    sessionStorage.setItem("ss-dashboard-token", "tok");
    render(<App fetchImpl={f} />);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10);
    });
    expect(screen.queryByTestId("stale-banner")).toBeNull();
    up = false;
    await act(async () => {
      await vi.advanceTimersByTimeAsync(14000);
    });
    expect(screen.getByTestId("stale-banner")).toHaveTextContent(/may be outdated/);
    expect(screen.getByLabelText("Global topology")).toHaveAttribute("data-stale", "true");
    expect(screen.getByTestId("topology-age")).toHaveTextContent(/STALE/);
    expect(screen.getByTestId("error-banner")).toBeInTheDocument();
    up = true;
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2100);
    });
    expect(screen.queryByTestId("stale-banner")).toBeNull();
    expect(screen.getByLabelText("Global topology")).toHaveAttribute("data-stale", "false");
  });

  it("marks sections stale when the backend says a source is stale", async () => {
    const s = snapshot();
    s.sources.engine = { ok: false, age_s: 120, error: "ConnectError", stale: true };
    const { f } = backend({ snap: () => s });
    await signedIn(f);
    expect(await screen.findByTestId("stale-banner")).toHaveTextContent("engine is stale");
    expect(screen.getByLabelText("Replication")).toHaveAttribute("data-stale", "true");
    expect(screen.getByLabelText("Global topology")).toHaveAttribute("data-stale", "false");
  });
});

describe("permissions and administrative operations", () => {
  it("a viewer sees no administrative controls", async () => {
    const { f } = backend({ role: "viewer", snap: () => snapshot() });
    await signedIn(f);
    expect(await screen.findByTestId("admin-readonly")).toHaveTextContent(/operator role/);
    expect(screen.queryByRole("button", { name: /Request failover/ })).toBeNull();
    expect(screen.queryByRole("button", { name: /Start failback/ })).toBeNull();
  });

  it("an operator must confirm, and the answer shown is the controller's: a refusal is not shown as success", async () => {
    const calls: string[] = [];
    const { f } = backend({
      role: "operator",
      snap: () => snapshot(),
      calls,
      admin: () =>
        jsonResponse({ requested_by: "tester", controller_answer: { result: "REFUSED", reason: "ACTIVE_REGION_NOT_FAILED", detail: "region-a is UP" } }),
    });
    await signedIn(f);
    await userEvent.click(await screen.findByRole("button", { name: "Request failover" }));
    expect(calls.some((c) => c.includes("/admin/"))).toBe(false); // nothing is sent before confirmation
    await userEvent.click(within(screen.getByRole("alertdialog")).getByRole("button", { name: "Confirm" }));
    const ans = await screen.findByTestId("admin-answer");
    expect(ans).toHaveTextContent("REFUSED");
    expect(ans).toHaveTextContent("ACTIVE_REGION_NOT_FAILED");
    expect(ans).not.toHaveTextContent(/succe|promoted to/i);
    expect(screen.getByTestId("dr-active")).toHaveTextContent("A"); // the active region did not change: only the backend can change it
    expect(screen.getByTestId("dr-phase")).toHaveTextContent("HEALTHY_A");
  });

  it("a controller answer of PROMOTED still changes nothing on screen until the coordinator reports it", async () => {
    const { f } = backend({
      role: "operator",
      snap: () => snapshot(),
      admin: () => jsonResponse({ requested_by: "tester", controller_answer: { result: "PROMOTED", active_region: "region-b" } }),
    });
    await signedIn(f);
    await userEvent.click(await screen.findByRole("button", { name: "Request failover" }));
    await userEvent.click(within(screen.getByRole("alertdialog")).getByRole("button", { name: "Confirm" }));
    await screen.findByTestId("admin-answer");
    expect(screen.getByTestId("dr-active")).toHaveTextContent("A");
    expect(screen.getByTestId("active-region")).toHaveTextContent("A");
  });

  it("an unauthorised admin call (403 from the API) is reported as refused", async () => {
    const { f } = backend({ role: "operator", snap: () => snapshot(), admin: () => jsonResponse({ detail: "this operation needs the operator role" }, 403) });
    await signedIn(f);
    await userEvent.click(await screen.findByRole("button", { name: "Request failover" }));
    await userEvent.click(within(screen.getByRole("alertdialog")).getByRole("button", { name: "Confirm" }));
    expect(await screen.findByTestId("admin-answer")).toHaveTextContent("Refused by the dashboard API (403)");
  });

  it("degraded RPO can only be requested where the deployment permits it", async () => {
    const { f } = backend({ role: "operator", snap: () => snapshot() });
    await signedIn(f);
    expect(await screen.findByRole("checkbox", { name: /accept degraded RPO/ })).toBeDisabled();
  });

  it("failback approval is offered only when the backend reports READY", async () => {
    const s = snapshot();
    s.dr.fenced = { "region-a": 2 };
    s.dr.failback = { region: "region-a", stage: "FRESHNESS", requested_by: "x", approved_by: null, from: "region-b" };
    const { f } = backend({ role: "operator", snap: () => s });
    await signedIn(f);
    await screen.findByTestId("failback-status");
    expect(screen.queryByRole("button", { name: "Approve failback" })).toBeNull();
    cleanup();
    sessionStorage.clear();
    const r = snapshot();
    r.dr.fenced = { "region-a": 2 };
    r.dr.failback = { region: "region-a", stage: "READY", requested_by: "x", approved_by: null, from: "region-b" };
    await signedIn(backend({ role: "operator", snap: () => r }).f);
    expect(await screen.findByRole("button", { name: "Approve failback" })).toBeInTheDocument();
  });
});

describe("sensitive data", () => {
  it("never renders payloads or credentials even if the backend sent them", async () => {
    const s = snapshot() as unknown as Record<string, any>;
    s.audit[0].payload = "TOP-SECRET-PAYLOAD";
    s.replication[0].value = "TOP-SECRET-VALUE";
    s.policies.policies[0].api_key = "TOP-SECRET-KEY";
    s.dr.token = "TOP-SECRET-TOKEN";
    const { f } = backend({ snap: () => s as any });
    await signedIn(f);
    await screen.findByLabelText("Audit logs");
    expect(document.body.textContent).not.toMatch(/TOP-SECRET/);
    expect(document.body.innerHTML).not.toMatch(/TOP-SECRET/);
  });
});

describe("sign in", () => {
  it("rejects an unknown token and stores nothing", async () => {
    const f = (async () => jsonResponse({ detail: "unknown token" }, 401)) as typeof fetch;
    render(<App fetchImpl={f} />);
    await userEvent.type(screen.getByLabelText("Access token"), "bad");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByText(/not valid/)).toBeInTheDocument();
    await waitFor(() => expect(sessionStorage.getItem("ss-dashboard-token")).toBeNull());
  });
});
