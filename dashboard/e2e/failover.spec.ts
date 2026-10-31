// DESTRUCTIVE (kills region A, then restores it). Run with:  SS_E2E_DESTRUCTIVE=1 npx playwright test e2e/failover.spec.ts
// Starts from a healthy DR stack with every region up.
import { execFileSync } from "node:child_process";
import { expect, test } from "@playwright/test";
import { DR, OPERATOR } from "./env";
import { signIn } from "./helpers";

const A = [
  "ss-a-gateway",
  "ss-a-agent",
  "ss-a-replicator",
  "ss-a-exporter",
  "ss-a-broker-1",
  "ss-a-broker-2",
  "ss-a-broker-3",
  "ss-a-controller-1",
  "ss-a-controller-2",
  "ss-a-controller-3",
  "ss-etcd-a",
];
const docker = (...a: string[]) => {
  try {
    execFileSync("docker", a, { stdio: "ignore" });
  } catch {
    /* already stopped / started */
  }
};

test.skip(!process.env.SS_E2E_DESTRUCTIVE, "destructive: set SS_E2E_DESTRUCTIVE=1");

test("operator workflow: region A fails, the dashboard shows only what the coordinator reports, and the restored region stays fenced", async ({
  page,
  request,
}) => {
  test.setTimeout(420_000);
  await signIn(page, OPERATOR);
  await expect(page.getByTestId("dr-phase")).toContainText("HEALTHY_A");
  A.forEach((c) => docker("stop", "-t", "1", c));
  try {
    await expect(page.getByTestId("region-region-a")).toContainText("DOWN", { timeout: 60_000 });
    // ask for the failover from the browser; whichever controller acts first, the screen must follow the coordinator, not the click
    await page.getByRole("button", { name: "Request failover" }).click();
    await page.getByRole("alertdialog").getByRole("button", { name: "Confirm" }).click();
    await expect(page.getByTestId("admin-answer")).toContainText("The DR controller answered");
    await expect(page.getByTestId("dr-phase")).toContainText("HEALTHY_B", { timeout: 180_000 });
    await expect(page.getByTestId("dr-active")).toHaveText("B");
    await expect(page.getByTestId("dr-epoch")).toHaveText("2");
    const truth = await (await request.get(`${DR}/status`)).json();
    expect(truth.state.active_region).toBe("region-b");
    await expect(page.getByTestId("last-failover")).toContainText("Measured RTO");
    await expect(page.getByRole("table", { name: "Audit entries" }).getByText("PROMOTED").first()).toBeVisible();
  } finally {
    A.slice()
      .reverse()
      .forEach((c) => docker("start", c));
  }
  await expect(page.getByTestId("region-region-a")).toContainText("fenced", { timeout: 240_000 });
  await expect(page.getByRole("table", { name: "Fenced regions" })).toContainText("cannot accept writes");
  await expect(page.getByTestId("dr-active")).toHaveText("B"); // restoring A did not make it active
});
