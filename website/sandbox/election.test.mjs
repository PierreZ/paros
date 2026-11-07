// The election stepper's milestones: `node --test website/sandbox/*.test.mjs`.
import { test } from "node:test";
import assert from "node:assert/strict";

import { milestones, nextCampaign } from "./election.js";

const KEY = "1:2";
const msg = (t, from, to, kind, round, slot = null) => ({ t, from, to, kind, key: KEY, round, slot });

test("a campaign is a matchmaking start, or a Prepare of a higher round", () => {
  const msgs = [msg(5, 0, 1, "prepare", "2"), msg(50, 0, 1, "prepare", "2"), msg(80, 1, 0, "prepare", "3")];
  assert.equal(nextCampaign(msgs, [], KEY, 0), 5);
  // A re-sent Prepare of the same round is no new campaign.
  assert.equal(nextCampaign(msgs, [], KEY, 10), 80);
  const campaigns = [{ t: 40, key: KEY, kind: "started" }];
  assert.equal(nextCampaign(msgs, campaigns, KEY, 10), 40);
  assert.equal(nextCampaign(msgs, [], "9:9", 0), null);
});

test("the milestones go matchmaking, Phase 1, elected, Phase 2, chosen", () => {
  const campaigns = [
    { t: 10, key: KEY, kind: "started", leader: 0, round: "3", reconfiguration: true },
    { t: 20, key: KEY, kind: "completed", round: "3", prior: 1, registeredBy: 2 },
    { t: 40, key: KEY, kind: "elected", leader: 0, round: "3" },
  ];
  const msgs = [
    msg(21, 0, 1, "prepare", "3"), msg(21, 0, 2, "prepare", "3"),
    msg(30, 1, 0, "promise", "3"), msg(32, 2, 0, "promise", "3"),
    msg(41, 0, 1, "accept", "3", 7), msg(50, 0, 1, "commit", "3", 7),
  ];
  const steps = milestones(msgs, campaigns, KEY, 10, 2);
  assert.deepEqual(steps.map((s) => s.title), [
    "Matchmaking", "Matchmakers answered", "Phase 1: Prepare", "Phase 1: promise quorum",
    "Elected", "Phase 2: Accept", "Chosen",
  ]);
  assert.deepEqual(steps.map((s) => s.t), [10, 20, 21, 30, 40, 41, 50]);
  assert.match(steps[0].text, /reconfiguration/);
  assert.match(steps[1].text, /1 older configuration\./);
});

test("without matchmakers the election starts at its Prepare, and a quorum counts the candidate", () => {
  const msgs = [msg(5, 0, 1, "prepare", "2"), msg(9, 1, 0, "promise", "2"), msg(12, 0, 1, "accept", "2", 0)];
  const steps = milestones(msgs, [], KEY, 5, 3);
  assert.deepEqual(steps.map((s) => s.title), ["Phase 1: Prepare", "Phase 2: Accept"]);
  const enough = milestones([...msgs, msg(10, 2, 0, "promise", "2")], [], KEY, 5, 3);
  assert.deepEqual(enough.map((s) => s.title), ["Phase 1: Prepare", "Phase 1: promise quorum", "Phase 2: Accept"]);
});
