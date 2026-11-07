// The page's fold of the matchmaker events: `node --test website/sandbox/*.test.mjs`.
import { test } from "node:test";
import assert from "node:assert/strict";

import { Matchmakers, compareBallots } from "./matchmakers.js";

const T = "11", J = "22", KEY = `${T}:${J}`;
const A = "100", B = "200", C = "300", D = "400";

function ev(name, time_ms, fields) {
  return { name, time_ms, machine: 0, fields: Object.entries({ tenant: T, journal: J, ...fields }) };
}

function registered(t, mm, round, acceptors, reconfiguration = false) {
  return ev("match_registered", t, { matchmaker: mm, round: String(round), bnode: A,
    acceptors: acceptors.join(" "), reconfiguration: String(reconfiguration) });
}

test("ballots order by round, then by node as a u64", () => {
  assert.equal(compareBallots({ round: 2, node: "9" }, { round: 3, node: "1" }), -1);
  assert.equal(compareBallots({ round: 3, node: "18446744073709551615" }, { round: 3, node: "18446744073709551614" }), 1);
  assert.equal(compareBallots({ round: 3, node: "5" }, { round: 3, node: "5" }), 0);
});

test("a send is a message to draw, from the leader to the matchmaker and back", () => {
  const mm = new Matchmakers();
  assert.deepEqual(mm.add(ev("match_request_sent", 1, { node: A, matchmaker: B, round: "2", bnode: A })),
    { from: A, to: B, kind: "match", key: KEY });
  assert.deepEqual(mm.add(ev("match_replied", 2, { matchmaker: B, to: A })),
    { from: B, to: A, kind: "match_reply", key: KEY });
  assert.deepEqual(mm.add(ev("gc_request_sent", 3, { node: A, matchmaker: B })),
    { from: A, to: B, kind: "gc", key: KEY });
  assert.deepEqual(mm.add(ev("gc_replied", 4, { matchmaker: B, to: A })),
    { from: B, to: A, kind: "gc_ack", key: KEY });
  assert.equal(mm.add(registered(5, B, 2, [A, B, C])), null);
  assert.ok(mm.active(T, B, 5, 100));
  assert.ok(!mm.active(T, B, 500, 100));
  assert.ok(!mm.active(T, C, 5, 100));
});

test("a reconfiguration registers a new configuration, and GC drops the old one", () => {
  const mm = new Matchmakers();
  const set = [A, B, C];
  // The first leader registers {A, B, C} at ballot 2 on two matchmakers.
  mm.add(ev("matchmaking_started", 10, { node: A, round: "2", bnode: A, reconfiguration: "false" }));
  mm.add(registered(11, A, 2, [A, B, C]));
  mm.add(registered(12, B, 2, [A, B, C]));
  mm.add(ev("matchmaking_completed", 12, { node: A, round: "2", bnode: A, prior: "0", registered_by: "2" }));
  let view = mm.view(KEY, set);
  assert.equal(view.rows.length, 1);
  assert.equal(view.rows[0].state, "newest");
  assert.deepEqual(view.rows[0].kept, [A, B]);
  assert.ok(view.phase.done);

  // C is down: the leader reconfigures to {A, B, D} at ballot 3.
  mm.add(ev("matchmaking_started", 20, { node: A, round: "3", bnode: A, reconfiguration: "true" }));
  mm.add(registered(21, A, 3, [A, B, D], true));
  mm.add(registered(22, C, 3, [A, B, D], true));
  mm.add(ev("matchmaking_completed", 22, { node: A, round: "3", bnode: A, prior: "1", registered_by: "2" }));
  view = mm.view(KEY, set);
  assert.deepEqual(view.rows.map((r) => r.ballot.round), [3, 2]);
  assert.equal(view.rows[0].reconfiguration, true);
  assert.equal(view.rows[1].state, "kept");
  assert.equal(view.phase.prior, 1);
  assert.equal(view.phase.floor, null);

  // GC: A and B raise their floor to ballot 3, so both drop ballot 2.
  mm.add(ev("gc_watermark_raised", 30, { matchmaker: A, round: "3", bnode: A }));
  mm.add(ev("gc_watermark_raised", 31, { matchmaker: B, round: "3", bnode: A }));
  view = mm.view(KEY, set);
  assert.equal(view.rows[1].state, "dropped");
  assert.deepEqual(view.rows[1].dropped, [A, B]);
  assert.deepEqual(view.rows[0].kept, [A, C]);
  assert.equal(view.phase.floor.round, 3);
  assert.equal(view.log[0].text.kind, "gc");
  assert.equal(view.log[0].text.dropped, 1);
});

test("a floor never goes down, and the set generation only goes up", () => {
  const mm = new Matchmakers();
  mm.add(ev("gc_watermark_raised", 1, { matchmaker: A, round: "4", bnode: A }));
  mm.add(ev("gc_watermark_raised", 2, { matchmaker: A, round: "2", bnode: A }));
  assert.equal(mm.journals.get(KEY).floors.get(A).round, 4);
  mm.add(ev("matchmaker_activated", 3, { matchmaker: A, generation: "2" }));
  mm.add(ev("matchmaker_activated", 4, { matchmaker: B, generation: "1" }));
  assert.equal(mm.generations.get(T), 2);
});

test("the page keeps a bounded number of configurations, the newest", () => {
  const mm = new Matchmakers();
  for (let r = 1; r <= 10; r++) mm.add(registered(r, A, r, [A]));
  const rounds = mm.view(KEY, [A]).rows.map((row) => row.ballot.round);
  assert.deepEqual(rounds, [10, 9, 8, 7, 6, 5]);
});
