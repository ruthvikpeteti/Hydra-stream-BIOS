import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));

const text = readFileSync(resolve(here, "../../infrastructure/.env"), "utf8");
export const ENV: Record<string, string> = Object.fromEntries(
  text
    .split("\n")
    .filter((l) => l.includes("="))
    .map((l) => [l.slice(0, l.indexOf("=")), l.slice(l.indexOf("=") + 1)]),
);
export const VIEWER = ENV.SS_DASH_VIEWER_TOKEN;
export const OPERATOR = ENV.SS_DASH_OPERATOR_TOKEN;
export const ENGINE = "http://localhost:8090/api/v1";
export const GATEWAY = "http://localhost:8080/api/v1";
export const DR = "http://localhost:8101/dr/v1";
