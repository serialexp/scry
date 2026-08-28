//! Drive the real TypeScript live-tail client against a running `scry web`.
//!
//! This is the browser half of `scripts/smoke-webui-tail.sh`. It exists because
//! the UI's tail path is not just an HTTP call: it pipelines a Hello+Subscribe
//! encoded by the binschema **ingest** bindings, then incrementally deframes a
//! chunked response through `FrameStream`. `curl` can prove the relay copies
//! bytes; only this can prove the client speaks the protocol.
//!
//! It imports `HttpTransport` and `runTail` **unmodified** — the shipped code,
//! not a copy. The two things a browser supplies for free (a document base URL
//! for `/api/...` and an automatic session cookie) are supplied here by wrapping
//! `globalThis.fetch`, so the harness carries the shim and production carries
//! none of it.
//!
//! Usage:
//!   bun scripts/tail-probe.ts --base http://127.0.0.1:8080 --password secret \
//!       [--target ID] [--matcher 'service="api"'] [--seconds 5] [--targets]
//!
//! Records print to stdout as one JSON object per line. Diagnostics go to
//! stderr. Exit codes: 0 ok · 2 usage/login failure · 3 the server refused the
//! subscription (a `TailError` or a 409 `LiveUnavailableError`) — the refusal is
//! printed to stdout as `{"refused":…}` so the shell can assert on it.

import { HttpTransport } from "../src/protocol/transport-http";
import { LiveUnavailableError } from "../src/protocol/transport";
import { TailError, runTail } from "../src/protocol/tail";

interface Args {
  base: string;
  password: string;
  target: string;
  matchers: string[];
  seconds: number;
  targetsOnly: boolean;
}

function parseArgs(argv: string[]): Args {
  const out: Args = {
    base: "",
    password: "",
    target: "",
    matchers: [],
    seconds: 5,
    targetsOnly: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = () => {
      const v = argv[++i];
      if (v === undefined) die(`${a} needs a value`);
      return v!;
    };
    switch (a) {
      case "--base": out.base = next(); break;
      case "--password": out.password = next(); break;
      case "--target": out.target = next(); break;
      case "--matcher": out.matchers.push(next()); break;
      case "--seconds": out.seconds = Number(next()); break;
      case "--targets": out.targetsOnly = true; break;
      default: die(`unknown argument: ${a}`);
    }
  }
  if (out.base === "") die("--base is required");
  if (out.password === "") die("--password is required");
  return out;
}

function die(msg: string): never {
  console.error(`tail-probe: ${msg}`);
  process.exit(2);
}

/**
 * Make the shipped transport work outside a browser tab.
 *
 * `HttpTransport` fetches `/api/tail` — a document-relative URL — and relies on
 * `credentials: "same-origin"` for the session cookie. Outside a document
 * neither resolves, so we resolve them here and leave the transport alone.
 */
function installBrowserShim(base: string, cookie: string): void {
  const real = globalThis.fetch;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" && input.startsWith("/") ? base + input : input;
    const headers = new Headers(init?.headers);
    if (cookie !== "") headers.set("cookie", cookie);
    return real(url as RequestInfo | URL, { ...init, headers });
  }) as typeof fetch;
}

/** Log in and return the raw `scry_session=…` cookie pair. */
async function login(base: string, password: string): Promise<string> {
  const res = await fetch(`${base}/api/login`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ password }),
  });
  if (res.status !== 204) die(`login failed: HTTP ${res.status}`);
  const setCookie = res.headers.get("set-cookie") ?? "";
  const pair = setCookie.split(";")[0]?.trim() ?? "";
  if (!pair.startsWith("scry_session=")) die("login did not set a session cookie");
  return pair;
}

const args = parseArgs(process.argv.slice(2));
const cookie = await login(args.base, args.password);
installBrowserShim(args.base, cookie);

// `--targets` mode: dump the capability list and stop. The `live` flag is what
// tells the UI whether to offer the toggle at all, so the smoke asserts on it.
if (args.targetsOnly) {
  const res = await fetch("/api/targets");
  if (!res.ok) die(`/api/targets failed: HTTP ${res.status}`);
  console.log(JSON.stringify(await res.json()));
  process.exit(0);
}

const controller = new AbortController();
const stop = setTimeout(() => controller.abort(), args.seconds * 1000);
let count = 0;

try {
  await runTail(
    new HttpTransport(),
    args.target,
    { matchers: args.matchers },
    "tail-probe",
    {
      onSubscribed: () => console.error("tail-probe: subscribed"),
      onRecord: (rec) => {
        count++;
        console.log(
          JSON.stringify({
            ts: rec.tsUnixNano.toString(),
            sev: rec.severity,
            body: rec.body,
            labels: Object.fromEntries(rec.labels),
            attrs: Object.fromEntries(rec.attrs),
          }),
        );
      },
    },
    controller.signal,
  );
} catch (e) {
  clearTimeout(stop);
  if (e instanceof TailError) {
    console.log(JSON.stringify({ refused: "TailError", code: e.code, message: e.message }));
    console.error(`tail-probe: server refused: code=${e.code} ${e.message}`);
    process.exit(3);
  }
  if (e instanceof LiveUnavailableError) {
    console.log(JSON.stringify({ refused: "LiveUnavailable", message: e.message }));
    console.error(`tail-probe: no live endpoint: ${e.message}`);
    process.exit(3);
  }
  console.error(`tail-probe: ${e instanceof Error ? e.message : String(e)}`);
  process.exit(1);
}

clearTimeout(stop);
console.error(`tail-probe: ${count} record(s)`);
