// What the matchmakers do, as the page knows it from the events the machines
// log (Matchmaker Paxos, #120 (matchmaking), #123 (GC), #125 (handover)).
// A read only: the page folds the events into what each matchmaker stores
// for each journal, and draws it. Nothing here acts on the universe.
//
// Pure: no DOM, so `node --test website/sandbox/*.test.mjs` checks it.

// The events a matchmaker or a leader logs while it works with the set.
export const MATCH_EVENTS = new Set([
  "matchmaking_started", "matchmaking_completed",
  "match_request_sent", "match_replied", "match_refused", "match_registered",
  "gc_request_sent", "gc_replied", "gc_watermark_raised",
  "reconfigure_request_sent", "reconfigure_replied", "matchmaker_activated",
]);

// The message kinds of the matchmaker wire, as the stage draws them.
export const MATCH_KINDS = new Set(["match", "match_reply", "gc", "gc_ack", "handover", "handover_reply"]);

// The steps of a matchmaker-set handover (#125), in order: the old set is
// stopped, the successor gets the registries (bootstrap), a one-slot decree
// chooses it (prepare, accept, chosen), and its matchmakers activate.
export const HANDOVER_STEPS = ["stop", "bootstrap", "decree_prepare", "decree_accept", "chosen"];

// The most configurations the page keeps for one journal, and the most lines
// of its log.
const MAX_CONFIGS = 6;
const MAX_LOG = 6;

function field(e, k) {
  for (const [key, v] of e.fields) if (key === k) return v;
  return undefined;
}

// A ballot as the events give it: its round and its node (decimal).
function ballotOf(round, node) {
  return { round: Number(round ?? 0), node: node ?? "0" };
}

// Ballots in order: by round, then by node. Node ids are u64 in decimal, so
// they compare as BigInt.
export function compareBallots(a, b) {
  if (a.round !== b.round) return a.round < b.round ? -1 : 1;
  const x = BigInt(a.node), y = BigInt(b.node);
  return x === y ? 0 : x < y ? -1 : 1;
}

export function ballotKey(b) {
  return `${b.round}.${b.node}`;
}

// The page's copy of one journal's registry.
function journalState() {
  return {
    // The configurations a matchmaker registered, by ballot.
    configs: new Map(),
    // Each matchmaker's floor (its GC watermark), by node id.
    floors: new Map(),
    // The newest matchmaking phase a leader opened.
    phase: null,
    // What happened last, newest first.
    log: [],
  };
}

export class Matchmakers {
  constructor() {
    // Each journal's state, by `<tenant>:<journal>` (decimal ids).
    this.journals = new Map();
    // Each tenant's set generation, by tenant id: the newest a matchmaker
    // activated.
    this.generations = new Map();
    // Each tenant's newest set handover, by tenant id.
    this.handovers = new Map();
    // The last time each matchmaker worked for each tenant, by
    // `<tenant>@<node>`: the stage lights its badge.
    this.busy = new Map();
  }

  journal(key) {
    let j = this.journals.get(key);
    if (!j) { j = journalState(); this.journals.set(key, j); }
    return j;
  }

  note(j, t, text) {
    j.log.unshift({ t, text });
    if (j.log.length > MAX_LOG) j.log.length = MAX_LOG;
  }

  // Fold one event. `node` is the node id of the machine that logged it,
  // when the page knows it. It answers the message to draw, when the event
  // is a send: `{ from, to, kind, key }` with node ids, or null.
  add(e, node) {
    const tenant = field(e, "tenant");
    const journalId = field(e, "journal");
    const key = `${tenant}:${journalId}`;
    const mm = field(e, "matchmaker");
    if (mm !== undefined && tenant !== undefined) this.busy.set(`${tenant}@${mm}`, e.time_ms);
    switch (e.name) {
      case "matchmaking_started": {
        const j = this.journal(key);
        const ballot = ballotOf(field(e, "round"), field(e, "bnode"));
        const reconfiguration = field(e, "reconfiguration") === "true";
        j.phase = { ballot, leader: field(e, "node"), reconfiguration, t: e.time_ms,
          registeredBy: 0, prior: null, done: false, asked: new Set(), floor: null };
        this.note(j, e.time_ms, { kind: "started", ballot, leader: field(e, "node"), reconfiguration });
        return null;
      }
      case "match_request_sent": {
        const j = this.journal(key);
        const ballot = ballotOf(field(e, "round"), field(e, "bnode"));
        if (j.phase && compareBallots(j.phase.ballot, ballot) === 0) j.phase.asked.add(mm);
        return { from: field(e, "node"), to: mm, kind: "match", key };
      }
      case "match_registered": {
        const j = this.journal(key);
        const ballot = ballotOf(field(e, "round"), field(e, "bnode"));
        const id = ballotKey(ballot);
        let c = j.configs.get(id);
        if (!c) {
          c = { ballot, acceptors: (field(e, "acceptors") ?? "").split(" ").filter(Boolean),
            reconfiguration: field(e, "reconfiguration") === "true", holders: new Set(), t: e.time_ms };
          j.configs.set(id, c);
          // The oldest go first once the page holds too many.
          if (j.configs.size > MAX_CONFIGS) {
            const oldest = [...j.configs.values()].sort((a, b) => compareBallots(a.ballot, b.ballot))[0];
            j.configs.delete(ballotKey(oldest.ballot));
          }
        }
        c.holders.add(mm);
        return null;
      }
      case "match_replied":
      case "match_refused":
        return { from: mm, to: field(e, "to"), kind: "match_reply", key };
      case "matchmaking_completed": {
        const j = this.journal(key);
        const ballot = ballotOf(field(e, "round"), field(e, "bnode"));
        if (j.phase && compareBallots(j.phase.ballot, ballot) === 0) {
          j.phase.done = true;
          j.phase.prior = Number(field(e, "prior") ?? 0);
          j.phase.registeredBy = Number(field(e, "registered_by") ?? 0);
        }
        this.note(j, e.time_ms, { kind: "completed", ballot, prior: Number(field(e, "prior") ?? 0),
          registeredBy: Number(field(e, "registered_by") ?? 0) });
        return null;
      }
      case "gc_request_sent":
        return { from: field(e, "node"), to: mm, kind: "gc", key };
      case "gc_watermark_raised": {
        const j = this.journal(key);
        const floor = ballotOf(field(e, "round"), field(e, "bnode"));
        const before = j.floors.get(mm);
        if (!before || compareBallots(before, floor) < 0) j.floors.set(mm, floor);
        if (j.phase && compareBallots(j.phase.ballot, floor) === 0) j.phase.floor = floor;
        const dropped = [...j.configs.values()].filter((c) => compareBallots(c.ballot, floor) < 0 && c.holders.has(mm)).length;
        this.note(j, e.time_ms, { kind: "gc", matchmaker: mm, floor, dropped });
        return null;
      }
      case "gc_replied":
        return { from: mm, to: field(e, "to"), kind: "gc_ack", key };
      case "reconfigure_request_sent": {
        const kind = field(e, "kind");
        let h = this.handovers.get(tenant);
        // A stop opens a handover, unless one is open and not yet active.
        if (kind === "stop" && (!h || h.done)) {
          h = { t: e.time_ms, driver: field(e, "node"), from: this.generations.get(tenant) ?? 0,
            steps: new Map(), activated: new Set(), done: false };
          this.handovers.set(tenant, h);
        }
        if (h && !h.done && HANDOVER_STEPS.includes(kind)) {
          if (!h.steps.has(kind)) h.steps.set(kind, { t: e.time_ms, asked: new Set(), answered: new Set() });
          h.steps.get(kind).asked.add(mm);
        }
        return { from: field(e, "node"), to: mm, kind: "handover", key: `${tenant}:` };
      }
      case "reconfigure_replied": {
        // An answer counts for the newest handover, even once its set is
        // active: the last answers of a step may come after the activation.
        const h = this.handovers.get(tenant);
        const step = h ? h.steps.get(field(e, "kind")) : undefined;
        if (step) step.answered.add(mm);
        return { from: mm, to: field(e, "to"), kind: "handover_reply", key: `${tenant}:` };
      }
      case "matchmaker_activated": {
        const g = Number(field(e, "generation") ?? 0);
        if ((this.generations.get(tenant) ?? -1) < g) this.generations.set(tenant, g);
        const h = this.handovers.get(tenant);
        if (h && g > h.from) {
          h.activated.add(mm);
          h.to = g;
          h.done = true;
          h.end = e.time_ms;
        }
        return null;
      }
      default:
        return null;
    }
  }

  // What the page shows for one journal: its configurations, newest first,
  // each with where it is still stored, and the matchmakers' floors.
  view(key, set) {
    const j = this.journals.get(key);
    if (!j) return null;
    const configs = [...j.configs.values()].sort((a, b) => compareBallots(b.ballot, a.ballot));
    const rows = configs.map((c, i) => {
      // A matchmaker keeps a configuration until its floor passes the
      // configuration's ballot: GC drops it there.
      const kept = [], dropped = [];
      for (const m of set) {
        if (!c.holders.has(m)) continue;
        const floor = j.floors.get(m);
        if (floor && compareBallots(c.ballot, floor) < 0) dropped.push(m); else kept.push(m);
      }
      const state = i === 0 ? "newest" : kept.length ? "kept" : "dropped";
      return { ...c, kept, dropped, state };
    });
    return { rows, floors: j.floors, phase: j.phase, log: j.log };
  }

  // The newest handover of `tenant`'s set, as steps in order: each with
  // how many matchmakers it asked and how many answered. `null` before the
  // first handover.
  handover(tenant) {
    const h = this.handovers.get(tenant);
    if (!h) return null;
    const steps = HANDOVER_STEPS.map((kind) => {
      const step = h.steps.get(kind);
      return { kind, asked: step?.asked.size ?? 0, answered: step?.answered.size ?? 0, t: step?.t ?? null };
    });
    return { t: h.t, end: h.end ?? null, driver: h.driver, from: h.from, to: h.to ?? null,
      done: h.done, activated: h.activated.size, steps };
  }

  // Whether matchmaker `node` worked for `tenant` in the last `ms` before `t`.
  active(tenant, node, t, ms) {
    const last = this.busy.get(`${tenant}@${node}`);
    return last !== undefined && last <= t && t - last < ms;
  }
}
