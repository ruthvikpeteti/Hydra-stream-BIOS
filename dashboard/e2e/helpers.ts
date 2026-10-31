import { APIRequestContext, Page, expect } from "@playwright/test";
import { ENGINE, ENV, GATEWAY } from "./env";

export async function signIn(page: Page, token: string) {
  await page.goto("/");
  await page.getByLabel("Access token").fill(token);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { name: "Global topology" })).toBeVisible();
}

/** Makes sure at least one replicated topic with running flows exists, so that routing and replication have something to show. */
export async function ensureReplicatedTopic(request: APIRequestContext): Promise<string> {
  const name = `e2e-${Date.now().toString(36)}`;
  const gw = { "X-API-Key": ENV.SS_GATEWAY_API_KEY };
  const r = await request.post(`${GATEWAY}/topics`, {
    headers: gw,
    data: {
      name,
      partitions: 3,
      legal_zone: "GLOBAL",
      classification: "PUBLIC",
      allowed_storage_regions: ["region-a", "region-b", "region-c"],
      allowed_processing_regions: ["region-a", "region-b", "region-c"],
      allowed_transit_zones: ["EU", "ATLANTIC", "US"],
      failover_priority: ["region-a", "region-b", "region-c"],
      regional_replica_count: 2,
      durability_mode: "ASYNC_REGIONAL",
      local_replication_factor: 1,
      local_min_insync_replicas: 1,
    },
  });
  expect(r.status()).toBe(201);
  const s = await request.post(`${ENGINE}/replication/topics/${name}/start`, {
    headers: { "X-Service-Key": ENV.SS_POLICY_WRITER_KEY },
    data: { partitions: 3 },
  });
  expect(s.status()).toBe(200);
  for (let i = 0; i < 20; i++) {
    const e = await request.post(`${GATEWAY}/events`, {
      headers: gw,
      data: { topic: name, event_id: crypto.randomUUID(), partition_key: `k${i}`, payload: { n: i } },
    });
    expect(e.status()).toBe(200);
  }
  return name;
}
