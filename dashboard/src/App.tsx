import { useEffect, useMemo, useState } from "react";
import { Api, ApiError } from "./api";
import { useSnapshot } from "./useSnapshot";
import type { Me } from "./types";
import { Topology } from "./sections/Topology";
import { Policies } from "./sections/Policies";
import { Routing } from "./sections/Routing";
import { Replication } from "./sections/Replication";
import { DisasterRecovery } from "./sections/DisasterRecovery";
import { Audit } from "./sections/Audit";
import { Badge } from "./ui";

const KEY = "ss-dashboard-token";

export function Dashboard({
  api,
  me,
  onSignOut,
  intervalMs = 2000,
  staleAfterS = 10,
}: {
  api: Api;
  me: Me;
  onSignOut: () => void;
  intervalMs?: number;
  staleAfterS?: number;
}) {
  const feed = useSnapshot(api, intervalMs, staleAfterS);
  const { snapshot: s, stale, ageS } = feed;
  return (
    <div className="app">
      <header className="top">
        <h1>SovereignStream operations</h1>
        <div className="who">
          <Badge tone={me.can_administer ? "warn" : "info"}>{me.role}</Badge> {me.user} <button onClick={onSignOut}>Sign out</button>
        </div>
      </header>
      {(stale || feed.sourcesStale) && (
        <div role="alert" className="stale-banner" data-testid="stale-banner">
          Data may be outdated: the last successful update was {ageS === null ? "never" : `${ageS}s ago`}
          {s &&
            Object.entries(s.sources)
              .filter(([, v]) => v.stale)
              .map(([k]) => ` · ${k} is stale`)}
          . Nothing here should be trusted as current until this banner clears.
        </div>
      )}
      {feed.error && (
        <div role="alert" className="error-banner" data-testid="error-banner">
          {feed.error} <button onClick={feed.refresh}>Retry</button>
        </div>
      )}
      {feed.status === "loading" && (
        <p role="status" className="loading">
          Loading the current state…
        </p>
      )}
      {s && (
        <>
          <nav aria-label="Sections">
            {["topology", "policies", "routing", "replication", "dr", "audit"].map((id) => (
              <a key={id} href={`#${id}`}>
                {id}
              </a>
            ))}
          </nav>
          <details className="sources" open={false}>
            <summary>Data sources</summary>
            <ul>
              {Object.entries(s.sources).map(([k, v]) => (
                <li key={k} data-testid={`source-${k}`}>
                  {k}: {v.ok ? "ok" : `error (${v.error ?? "unknown"})`}, {v.age_s === null ? "never fetched" : `${v.age_s}s old`}
                  {v.stale ? " — STALE" : ""}
                </li>
              ))}
            </ul>
          </details>
          <Topology s={s} stale={stale || s.sources.coordinator.stale || s.sources.dr.stale} ageS={ageS} />
          <Policies s={s} stale={stale || s.sources.engine.stale} ageS={ageS} />
          <Routing s={s} stale={stale || s.sources.replicators.stale} ageS={ageS} />
          <Replication s={s} stale={stale || s.sources.engine.stale} ageS={ageS} />
          <DisasterRecovery s={s} stale={stale || s.sources.coordinator.stale} ageS={ageS} me={me} api={api} />
          <Audit s={s} stale={stale || s.sources.engine.stale} ageS={ageS} />
        </>
      )}
    </div>
  );
}

export function App({ fetchImpl }: { fetchImpl?: typeof fetch }) {
  const [token, setToken] = useState<string>(() => sessionStorage.getItem(KEY) ?? "");
  const [draft, setDraft] = useState("");
  const [me, setMe] = useState<Me | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const api = useMemo(() => (token ? new Api(token, "", fetchImpl) : null), [token, fetchImpl]);

  useEffect(() => {
    if (!api) {
      setMe(null);
      return;
    }
    api
      .me()
      .then((m) => {
        setMe(m);
        setErr(null);
      })
      .catch((e) => {
        setMe(null);
        setErr(e instanceof ApiError && e.status === 401 ? "That token is not valid." : "The dashboard API cannot be reached.");
        if (e instanceof ApiError && e.status === 401) {
          sessionStorage.removeItem(KEY);
          setToken("");
        }
      });
  }, [api]);

  if (!api || !me) {
    return (
      <main className="login">
        <h1>SovereignStream operations</h1>
        <form
          onSubmit={(e) => {
            e.preventDefault();
            sessionStorage.setItem(KEY, draft);
            setToken(draft);
          }}
        >
          <label>
            Access token <input type="password" aria-label="Access token" value={draft} onChange={(e) => setDraft(e.target.value)} autoComplete="off" />
          </label>
          <button type="submit" disabled={!draft}>
            Sign in
          </button>
        </form>
        {err && (
          <p role="alert" className="error-banner">
            {err}
          </p>
        )}
        {api && !me && !err && <p role="status">Checking the token…</p>}
      </main>
    );
  }
  return (
    <Dashboard
      api={api}
      me={me}
      onSignOut={() => {
        sessionStorage.removeItem(KEY);
        setToken("");
        setMe(null);
      }}
    />
  );
}
