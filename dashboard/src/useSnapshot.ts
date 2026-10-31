import { useCallback, useEffect, useRef, useState } from "react";
import type { Api } from "./api";
import { ApiError } from "./api";
import type { Snapshot } from "./types";

export type Feed = {
  status: "loading" | "ready" | "error";
  snapshot: Snapshot | null;
  error: string | null;
  /** seconds since the last SUCCESSFUL fetch (null before the first one) */
  ageS: number | null;
  /** the last successful fetch is older than the threshold (the whole screen is outdated) */
  stale: boolean;
  /** the backend itself reports that some of its sources are outdated */
  sourcesStale: boolean;
  refresh: () => void;
};

/** Polls the backend. `stale` is true when the last successful fetch is older than `staleAfterS`, or when the backend says a source is stale.
 *  After a failed fetch the previous snapshot stays on screen, clearly marked stale: it is never silently kept as if current. */
export function useSnapshot(api: Api | null, intervalMs = 2000, staleAfterS = 10, now: () => number = Date.now): Feed {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [lastOk, setLastOk] = useState<number | null>(null);
  const [tick, setTick] = useState(0);
  const alive = useRef(true);

  const load = useCallback(async () => {
    if (!api) return;
    try {
      const s = await api.snapshot();
      if (!alive.current) return;
      setSnapshot(s);
      setError(null);
      setLastOk(now());
    } catch (e) {
      if (!alive.current) return;
      setError(e instanceof ApiError ? (e.status === 503 ? "The backend is still collecting its first snapshot" : e.message) : "Unexpected error");
    }
  }, [api, now]);

  useEffect(() => {
    alive.current = true;
    load();
    const id = setInterval(load, intervalMs);
    const clock = setInterval(() => setTick((t) => t + 1), 1000);
    return () => {
      alive.current = false;
      clearInterval(id);
      clearInterval(clock);
    };
  }, [load, intervalMs]);

  void tick;
  const ageS = lastOk === null ? null : Math.max(0, Math.round((now() - lastOk) / 1000));
  const backendStale = snapshot ? Object.values(snapshot.sources).some((s) => s.stale) : false;
  const stale = ageS !== null && ageS > staleAfterS;
  const status: Feed["status"] = snapshot ? "ready" : error ? "error" : "loading";
  return { status, snapshot, error, ageS, stale, sourcesStale: backendStale, refresh: load };
}
