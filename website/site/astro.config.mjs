// The paros site (#810 (website redo 1/4)): one Astro app, one style.
// Every page uses src/layouts/Base.astro and the shared design system in
// website/shared/ (copied to public/shared/ by scripts/sync-shared.mjs).
import { defineConfig } from "astro/config";

// Old URLs of the Zola site and of the mdbook before it. Each one points at
// the page that now holds its subject.
const single = ["choose-one-value", "safety"];
const multi = ["replicated-log", "stable-leader", "restart-safety", "truncation-and-snapshots", "linearizable-reads"];
const tricky = {
  "matchmaker-paxos": "matchmaker",
  "matchmaker-generations": "matchmaker",
  "matchmaking-before-phase-1": "matchmaker",
  "reconfiguration-is-a-round-change": "matchmaker",
  "quorum-systems": "quorums",
  "quorum-reads": "quorums",
  "persist-before-send": "multi-paxos",
  "phase-1-over-a-log": "multi-paxos",
  "recovery-and-gap-fill": "multi-paxos",
  "accepted-chosen-applied": "multi-paxos",
  "ballot-is-not-leadership": "multi-paxos",
  "trim-point-jump": "multi-paxos",
  "gc-and-watermarks": "matchmaker",
  "allocator-frontier": "multi-paxos",
  "faulty-records": "multi-paxos",
};
// The pages of the old parosd docs: the service before the rewrite is at the tag
// v0.2.0-alpha.1, and /parosd/ says how to run it. A page whose subject has a
// page of its own in the new group goes there.
const parosd = {
  "admin-views": "", bench: "", "journal-api": "", metrics: "", tls: "",
  cells: "universe/", roles: "machines/", tokens: "names-and-tokens/",
};

const redirects = {
  "/play": "/paxos/",
  "/play.html": "/paxos/",
  "/sandbox": "/parosd/",
  "/sandbox/app": "/parosd/",
  "/playground": "/parosd/",
  "/docs": "/parosd/",
  "/docs/quickstart": "/parosd/",
  "/docs/reference": "/parosd/",
  "/docs/playground": "/parosd/",
  "/engineering/same-code": "/engineering/",
  "/engineering/swarm": "/engineering/",
  "/engineering/durable-writes": "/engineering/",
  "/engineering/fsync": "/engineering/",
  "/engineering/dogfooding": "/engineering/",
  "/engineering/sans-io": "/consensus/driver/",
  "/paxos/play": "/paxos/",
  "/paxos/tricky": "/paxos/",
  "/paxos/beyond-multi-paxos": "/paxos/",
  "/beyond-multi-paxos.html": "/paxos/",
};
for (const p of single) {
  redirects[`/paxos/${p}`] = "/paxos/single-decree/";
  redirects[`/${p}.html`] = "/paxos/single-decree/";
}
for (const p of multi) {
  redirects[`/paxos/${p}`] = "/paxos/multi-paxos/";
  redirects[`/${p}.html`] = "/paxos/multi-paxos/";
}
for (const [p, to] of Object.entries(tricky)) redirects[`/paxos/tricky/${p}`] = `/paxos/${to}/`;
for (const [p, to] of Object.entries(parosd)) redirects[`/parosd/${p}`] = `/parosd/${to}`;

// Astro does not put the base in front of a redirect's target.
const BASE = "/paros";
for (const from of Object.keys(redirects)) redirects[from] = BASE + redirects[from];

export default defineConfig({
  site: "https://pierrez.github.io",
  base: BASE,
  trailingSlash: "always",
  redirects,
  markdown: {
    shikiConfig: { theme: "css-variables" },
  },
});
