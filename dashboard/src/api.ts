import type { AdminAnswer, Me, Snapshot } from "./types";

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

export class Api {
  constructor(
    private token: string,
    private base = "",
    private f: typeof fetch = (...a) => fetch(...a),
  ) {}

  private async call<T>(path: string, init: RequestInit = {}): Promise<T> {
    let r: Response;
    try {
      r = await this.f(`${this.base}${path}`, {
        ...init,
        headers: { ...(init.headers ?? {}), Authorization: `Bearer ${this.token}`, "Content-Type": "application/json" },
      });
    } catch (e) {
      throw new ApiError(0, "The dashboard API cannot be reached");
    }
    if (!r.ok) {
      let detail = r.statusText;
      try {
        const j = await r.json();
        detail = j.detail ?? j.error ?? detail;
      } catch {
        /* keep status text */
      }
      throw new ApiError(r.status, typeof detail === "string" ? detail : JSON.stringify(detail));
    }
    return r.json() as Promise<T>;
  }

  me() {
    return this.call<Me>("/api/v1/me");
  }
  snapshot() {
    return this.call<Snapshot>("/api/v1/snapshot");
  }
  /** Administrative operations are forwarded by the API to the DR controller. The returned object is the controller's own answer. */
  admin(op: "failover" | "failback/start" | "failback/approve" | "failback/abort", body: object = {}) {
    return this.call<AdminAnswer>(`/api/v1/admin/${op}`, { method: "POST", body: JSON.stringify(body) });
  }
}
