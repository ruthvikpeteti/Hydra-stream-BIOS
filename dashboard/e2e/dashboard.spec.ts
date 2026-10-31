import { expect, test } from "@playwright/test";
import { DR, ENV, GATEWAY, OPERATOR, VIEWER } from "./env";
import { ensureReplicatedTopic, signIn } from "./helpers";

let topic = "";
test.beforeAll(async ({ request }) => {
  topic = await ensureReplicatedTopic(request);
});

test("an unknown token is rejected and nothing is shown", async ({ page }) => {
  await page.goto("/");
  await page.getByLabel("Access token").fill("not-a-token");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByText("That token is not valid.")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Global topology" })).toHaveCount(0);
});

test("viewer: all six sections show backend state, and the active region equals the coordinator's", async ({ page, request }) => {
  await signIn(page, VIEWER);
  for (const title of ["Global topology", "Residency policies", "Adaptive network routing", "Replication", "Disaster recovery", "Audit logs"]) {
    await expect(page.getByRole("region", { name: title })).toBeVisible();
  }
  const dr = await (await request.get(`${DR}/status`)).json();
  const active = (dr.state.active_region as string).replace("region-", "").toUpperCase();
  await expect(page.getByTestId("active-region")).toHaveText(active);
  await expect(page.getByTestId("dr-active")).toHaveText(active);
  await expect(page.getByTestId("epoch")).toHaveText(String(dr.state.leadership_epoch));
  await expect(page.getByTestId("dr-phase")).toContainText(dr.state.phase);
  await expect(page.getByTestId("brokers-region-a")).toContainText("of 3 up");
  // routing and replication show the replication we just created
  await expect(page.getByRole("table", { name: /Paths A to B/ })).toBeVisible();
  await expect(page.getByTestId("selected-region-a-region-b")).not.toHaveText("none");
  await expect(page.getByRole("table", { name: "Replication flows" }).getByText(topic).first()).toBeVisible();
  await expect(page.getByRole("table", { name: "Topic policies" }).getByText(topic)).toBeVisible();
});

test("viewer: no administrative controls, and the API refuses a viewer who tries anyway", async ({ page, request }) => {
  await signIn(page, VIEWER);
  await expect(page.getByTestId("admin-readonly")).toBeVisible();
  await expect(page.getByRole("button", { name: "Request failover" })).toHaveCount(0);
  const before = await (await request.get(`${DR}/status`)).json();
  const r = await request.post("/api/v1/admin/failover", { headers: { Authorization: `Bearer ${VIEWER}` }, data: {} });
  expect(r.status()).toBe(403);
  const after = await (await request.get(`${DR}/status`)).json();
  expect(after.state.seq).toBe(before.state.seq);
});

test("operator: a failover request needs confirmation, and a healthy region's refusal is shown as a refusal, never as success", async ({ page }) => {
  await signIn(page, OPERATOR);
  await page.getByRole("button", { name: "Request failover" }).click();
  await expect(page.getByRole("alertdialog")).toContainText("operator");
  await page.getByRole("alertdialog").getByRole("button", { name: "Confirm" }).click();
  const answer = page.getByTestId("admin-answer");
  await expect(answer).toContainText("REFUSED");
  await expect(answer).toContainText("ACTIVE_REGION_NOT_FAILED");
  await expect(answer).not.toContainText(/succe|promoted to/i);
  await expect(page.getByTestId("dr-active")).toHaveText("A");
  await expect(page.getByTestId("dr-epoch")).toHaveText(/^\d+$/);
  // the request is in the audit view with who asked
  await page.getByLabel("Audit category").selectOption("admin");
  await expect(page.getByRole("table", { name: "Audit entries" }).getByText("DASH_FAILOVER").first()).toBeVisible({ timeout: 30_000 });
});

test("stale data is flagged when updates stop, and the flag clears when they resume", async ({ page }) => {
  await signIn(page, VIEWER);
  await expect(page.getByTestId("stale-banner")).toHaveCount(0);
  await page.route("**/api/v1/snapshot", (route) => route.abort());
  await expect(page.getByTestId("stale-banner")).toBeVisible({ timeout: 40_000 });
  await expect(page.getByRole("region", { name: "Global topology" })).toHaveAttribute("data-stale", "true");
  await expect(page.getByTestId("error-banner")).toBeVisible();
  await page.unroute("**/api/v1/snapshot");
  await expect(page.getByTestId("stale-banner")).toHaveCount(0, { timeout: 30_000 });
  await expect(page.getByRole("region", { name: "Global topology" })).toHaveAttribute("data-stale", "false");
});

test("message payloads and credentials are never rendered", async ({ page, request }) => {
  const marker = `PAYLOAD-MARKER-${Date.now()}`;
  const e = await request.post(`${GATEWAY}/events`, {
    headers: { "X-API-Key": ENV.SS_GATEWAY_API_KEY },
    data: { topic, event_id: crypto.randomUUID(), partition_key: "customer-4711-secret", payload: { ssn: marker } },
  });
  expect(e.status()).toBe(200);
  await signIn(page, OPERATOR);
  await page.waitForTimeout(6000); // at least two refresh cycles after the write
  const html = await page.content();
  const text = await page.locator("body").innerText();
  expect(html).not.toContain(marker);
  expect(text).not.toContain(marker);
  expect(html).not.toContain("customer-4711");
  for (const [k, v] of Object.entries(ENV))
    if (/(KEY|PASSWORD|TOKEN)/.test(k) && v.length >= 8) {
      expect(html).not.toContain(v);
    }
  expect(await page.evaluate(() => JSON.stringify({ ...sessionStorage }))).not.toContain(ENV.SS_POLICY_ADMIN_KEY); // the controller's admin key never reaches the browser
});
