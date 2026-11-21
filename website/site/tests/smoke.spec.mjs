// The browser smoke test of the site (#1362 (browser smoke test and link
// check)). It drives the live parts that run paros-consensus in wasm
// (public/steps.js, story.js, lab.js, engineering.js, course.js) on the
// built site, as a reader does, and fails on:
//   - a console error or warning, an uncaught exception, a failed request
//     or an answer of 400 or more, on any page;
//   - a page wider than the screen at 390 px;
//   - a step-through that shows the recorded JSON in place of the live
//     wasm run (a renamed wasm export, a bundle that does not load);
//   - a story that does not reach its last screen, a step-through that
//     does not reach its last step;
//   - an exercise that the scripted clicks on Deliver and Drop do not
//     solve, and the Ready and quorum widgets that do not answer.
// No test is skipped, and a red run is not run again (AGENTS.md).
import { expect, test } from "@playwright/test";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const DIST = fileURLToPath(new URL("../dist/", import.meta.url));

// Every page of the built site: each index.html that is not a redirect.
function pages() {
  const out = [];
  (function walk(dir) {
    for (const name of readdirSync(dir)) {
      const path = join(dir, name);
      if (statSync(path).isDirectory()) walk(path);
      else if (name === "index.html" && !/http-equiv="refresh"/.test(readFileSync(path, "utf8"))) {
        out.push(relative(DIST, dir).split("\\").join("/").replace(/(.)$/, "$1/"));
      }
    }
  })(DIST);
  return out.sort();
}
const PAGES = pages();
// The pages with a step-through: the trace is inlined as <script id="trace">.
const TRACED = PAGES.filter((p) => readFileSync(join(DIST, p, "index.html"), "utf8").includes('id="trace"'));

// Collect every problem the page reports. `check()` fails the test on any.
function watch(page) {
  const problems = [];
  page.on("console", (m) => {
    if (m.type() === "error" || m.type() === "warning") problems.push(`console ${m.type()}: ${m.text()}`);
  });
  page.on("pageerror", (e) => problems.push(`uncaught: ${e.message}`));
  page.on("requestfailed", (r) => problems.push(`request failed: ${r.url()} (${r.failure()?.errorText})`));
  page.on("response", (r) => {
    if (r.status() >= 400) problems.push(`HTTP ${r.status()}: ${r.url()}`);
  });
  return { check: () => expect(problems, "the page reports no problem").toEqual([]) };
}

test("the site has pages, and some of them have a step-through", () => {
  expect(PAGES.length).toBeGreaterThan(15);
  expect(TRACED.length).toBeGreaterThan(3);
});

for (const path of PAGES) {
  test(`/${path} loads with no error, and does not scroll sideways at 390 px`, async ({ page }) => {
    const w = watch(page);
    for (const width of [1280, 390]) {
      await page.setViewportSize({ width, height: 900 });
      await page.goto(path, { waitUntil: "networkidle" });
      if (width === 390) {
        const wide = await page.evaluate(() => {
          const doc = document.documentElement;
          return { scroll: doc.scrollWidth, client: doc.clientWidth };
        });
        expect(wide.scroll, "no sideways scroll at 390 px").toBeLessThanOrEqual(wide.client);
      }
    }
    w.check();
  });
}

for (const path of TRACED) {
  test(`/${path}: the step-through runs live, every story and every run reach their end`, async ({ page }) => {
    const w = watch(page);
    await page.goto(path);
    const steps = page.locator(".panel.steps");
    // steps.js draws the recorded trace, then the live run once the wasm
    // answers `paxos_trace`: the chip says "live" only then.
    await expect(steps.locator(".panel-head .chip-status").first()).toHaveText("live");

    // Every story, to its last screen. With reduced motion a screen is
    // drawn at once; Continue on a question answers "Show me".
    const story = page.locator(".panel.story");
    if (await story.count()) {
      const ran = await page.evaluate(() => {
        const panel = document.querySelector(".panel.story");
        const next = [...panel.querySelectorAll(".story-controls button")].find((b) => b.textContent === "Continue" || b.textContent === "Skip");
        const counter = panel.querySelector(".panel-head .meta");
        const runs = [...panel.querySelectorAll(".story-others button[data-run]")];
        const out = [];
        const all = runs.length ? runs : [null];
        for (const pick of all) {
          if (pick) pick.click();
          let guard = 0;
          for (;;) {
            const m = /screen (\d+) \/ (\d+)/.exec(counter.textContent);
            const waiting = !panel.querySelector(".story-quiz").hidden && panel.querySelector(".story-answers:not(.done)");
            if (m && m[1] === m[2] && !waiting) { out.push({ run: pick ? pick.dataset.run : "first", screens: Number(m[2]) }); break; }
            if (++guard > 500) { out.push({ run: pick ? pick.dataset.run : "first", stuck: counter.textContent }); break; }
            next.click();
          }
        }
        return out;
      });
      for (const r of ran) expect(r.stuck, `story ${r.run} reaches its last screen`).toBeUndefined();
      expect(ran.length).toBeGreaterThan(0);
      // Open the step-through from the story, as a reader does.
      await story.getByText("Show each message").click();
    }

    // Every run of the step-through ("What if"), stepped to its last step.
    await expect(steps).toBeVisible();
    const runs = await page.evaluate(() => {
      const panel = document.querySelector(".panel.steps");
      const step = [...panel.querySelectorAll(".steps-controls button")].find((b) => b.textContent === "Step");
      const restart = [...panel.querySelectorAll(".steps-controls button")].find((b) => b.textContent === "Restart");
      const count = panel.querySelector(".panel-head .meta");
      const out = [];
      for (const pick of panel.querySelectorAll(".steps-whatif button")) {
        pick.click();
        if (!restart.disabled) restart.click();
        let guard = 0;
        while (!step.disabled && ++guard < 5000) step.click();
        const m = /step (\d+) \/ (\d+)/.exec(count.textContent);
        out.push({ run: pick.textContent, at: m && Number(m[1]), of: m && Number(m[2]) });
      }
      return out;
    });
    expect(runs.length).toBeGreaterThan(0);
    for (const r of runs) {
      expect(r.of, `run "${r.run}" has steps`).toBeGreaterThan(0);
      expect(r.at, `run "${r.run}" reaches its last step`).toBe(r.of);
    }
    w.check();
  });
}

// One exercise of each lesson that has exercises (src/course.ts), solved by
// clicks on the page's own buttons. Each move names the messages it acts on
// by the data-kind, data-from and data-to of their row (public/lab.js); the
// moves follow the `solution` of each exercise in
// crates/support/paros-wasm/src/paxos/lab.rs.
//   ["settle", f]   Deliver the first message that f matches, until none is left
//   ["deliver", f]  Deliver the first message that f matches
//   ["drop", f]     Drop every message that f matches
//   ["extra", text] press the extra move with that text
// A filter f: { kind: [..], from, to, notTo, or: [f, ..] }, with lanes named
// as the engine names them ("n3" is node or acceptor 3); {} matches all.
const EXERCISES = [
  {
    page: "paxos/single-decree/",
    id: "vote-not-chosen",
    moves: [
      ["settle", { kind: ["Prepare", "Promise"] }],
      ["deliver", { kind: ["Accept"], to: "n1" }],
      ["drop", { kind: ["Accept"] }],
      ["settle", {}],
    ],
  },
  { page: "paxos/quorums/", id: "majority", moves: [["settle", { notTo: "n3" }], ["drop", { to: "n3" }]] },
  {
    page: "paxos/multi-paxos/",
    id: "gap",
    moves: [
      ["extra", "The client writes bravo"],
      ["drop", { kind: ["Accept"] }],
      ["extra", "The client writes charlie"],
      ["settle", { or: [{ notTo: "n0" }, { kind: ["Accepted"] }] }],
      ["extra", "Node 0 crashes"],
      ["extra", "Node 1 starts an election"],
      ["settle", {}],
    ],
  },
  {
    page: "paxos/multi-paxos/",
    id: "read",
    moves: [["extra", "A client reads from node 2"], ["settle", { notTo: "n0" }], ["drop", { to: "n0" }]],
  },
  {
    page: "paxos/quorums/",
    id: "grid-column",
    moves: [["extra", "The client writes alpha"], ["settle", {}], ["extra", "The client writes bravo"], ["settle", {}]],
  },
  { page: "paxos/matchmaker/", id: "old-set", moves: [["extra", "Node 3 starts an election"], ["settle", {}]] },
];

for (const ex of EXERCISES) {
  test(`/${ex.page}: the exercise "${ex.id}" is solved by clicks on Deliver and Drop`, async ({ page }) => {
    const w = watch(page);
    await page.goto(`${ex.page}#lab=${ex.id}`);
    const lab = page.locator("#your-turn-lab");
    await expect(lab.locator(".lab-tab.now")).toBeVisible();
    const done = lab.locator(".lab-done");
    await expect(done).toBeHidden();
    const problem = await page.evaluate((moves) => {
      const panel = document.getElementById("your-turn-lab");
      const matches = (li, f) => {
        if (f.or) return f.or.some((g) => matches(li, g));
        if (f.kind && !f.kind.includes(li.dataset.kind)) return false;
        if (f.from != null && li.dataset.from !== f.from) return false;
        if (f.to != null && li.dataset.to !== f.to) return false;
        if (f.notTo != null && li.dataset.to === f.notTo) return false;
        return true;
      };
      const wire = () => [...panel.querySelectorAll(".lab-wire li.lab-msg")];
      const press = (li, text) => [...li.querySelectorAll("button")].find((b) => b.textContent === text).click();
      const refused = () => {
        const said = panel.querySelector(".lab-said");
        return said.querySelector(".chip-status.gap") ? said.textContent : null;
      };
      for (const [kind, arg] of moves) {
        if (kind === "extra") {
          const b = [...panel.querySelectorAll(".lab-extras button")].find((x) => x.textContent === arg);
          if (!b) return `no extra move "${arg}"`;
          b.click();
        } else if (kind === "deliver") {
          const li = wire().find((x) => matches(x, arg));
          if (!li) return `no message to deliver for ${JSON.stringify(arg)}`;
          press(li, "Deliver");
        } else if (kind === "settle") {
          let guard = 0;
          for (let li; (li = wire().find((x) => matches(x, arg))); ) {
            if (++guard > 500) return `settle ${JSON.stringify(arg)} does not end`;
            press(li, "Deliver");
            if (refused()) return `refused: ${refused()}`;
          }
        } else if (kind === "drop") {
          // Each Drop draws the wire again: find the next match each time.
          let guard = 0;
          for (let li; (li = wire().find((x) => matches(x, arg))); ) {
            if (++guard > 500) return `drop ${JSON.stringify(arg)} does not end`;
            press(li, "Drop");
          }
        }
        if (refused()) return `refused: ${refused()}`;
      }
      return null;
    }, ex.moves);
    expect(problem).toBeNull();
    await expect(done, "the page says the goal holds").toBeVisible();
    await expect(done).not.toBeEmpty();
    await expect(lab.locator(".lab-tab.now .lab-tab-n")).toHaveText("✓");
    w.check();
  });
}

test("/paxos/quorums/: the quorum picker answers from the wasm", async ({ page }) => {
  const w = watch(page);
  await page.goto("paxos/quorums/");
  const picker = page.locator(".panel.picker");
  await expect(picker.locator(".picker-line").first()).toBeVisible();
  const before = await picker.locator(".picker-result").textContent();
  await picker.locator(".picker-acceptor").first().click();
  await picker.locator(".picker-acceptor").nth(1).click();
  await expect(picker.locator(".picker-acceptor.down")).toHaveCount(2);
  await expect(picker.locator(".picker-result")).not.toHaveText(before);
  w.check();
});

test("/engineering/grid-quorums/: the quorum widget answers from the wasm", async ({ page }) => {
  const w = watch(page);
  await page.goto("engineering/grid-quorums/");
  const widget = page.locator('[data-widget="quorums"]');
  await expect(widget.locator(".strip")).toContainText("CAN FINISH");
  // Stop acceptors until no election can finish: one partition loses its majority.
  const cells = widget.locator(".qcell");
  await expect(cells.first()).toBeVisible();
  for (let i = 0; i < (await cells.count()) && !(await widget.locator(".strip").textContent()).includes("BLOCKED"); i++) {
    await cells.nth(i).click();
  }
  await expect(widget.locator(".strip")).toContainText("BLOCKED");
  w.check();
});

test("/consensus/driver/: the Ready widget elects a leader and commits a write", async ({ page }) => {
  const w = watch(page);
  await page.goto("consensus/driver/");
  const widget = page.locator('[data-widget="ready"]');
  const node0 = widget.locator(".lab-node").first();
  await expect(node0).toBeVisible();
  await node0.getByRole("button", { name: "timeout" }).click();
  await expect(widget).toContainText("ready() #1");
  await widget.getByRole("button", { name: "deliver all" }).click();
  await node0.getByRole("button", { name: "write" }).click();
  await widget.getByRole("button", { name: "deliver all" }).click();
  await expect(node0.locator(".lab-state")).toContainText("1 chosen");
  w.check();
});

// Every exercise of the journal fold lab (public/journal-lab.js), solved by
// clicks on its call buttons. A call is named by its data-call key (`A:write`,
// `seal`, from `Call::key` in crates/support/paros-wasm/src/journal/world.rs);
// the calls follow the `solution` of each exercise in
// crates/support/paros-wasm/src/journal/lab.rs.
const JOURNAL_EXERCISES = {
  "journal-duplicate": ["A:write-lost", "A:resend"],
  "journal-fenced": ["B:read", "B:set-leader", "A:write"],
  "journal-multi": ["A:set-multi", "B:write-open", "C:write-open"],
  "journal-below": ["A:write-lost", "seal", "A:resend", "A:resolve"],
  "journal-truncated": ["A:write-lost", "A:read", "A:truncate", "A:resend"],
};

for (const [id, keys] of Object.entries(JOURNAL_EXERCISES)) {
  test(`/journal/calls/: the journal exercise "${id}" is solved by its calls`, async ({ page }) => {
    const w = watch(page);
    await page.goto(`journal/calls/#lab=${id}`);
    const lab = page.locator("#your-turn-lab");
    await expect(lab.locator(".lab-tab.now")).toBeVisible();
    await expect(lab.locator(".chip-status.accent").first()).toHaveText("live");
    const done = lab.locator(".lab-done");
    await expect(done).toBeHidden();
    for (const key of keys) {
      await lab.locator(`.lab-extras button[data-call="${key}"]`).click();
      await expect(lab.locator(".jlab-answer .chip-status.gap", { hasText: "refused" })).toHaveCount(0);
    }
    await expect(done, "the page says the goal holds").toBeVisible();
    await expect(done).not.toBeEmpty();
    await expect(lab.locator(".lab-tab.now .lab-tab-n")).toHaveText("✓");
    // Undo takes the last call back, and the goal no longer holds.
    await lab.getByRole("button", { name: "Undo" }).click();
    await expect(done).toBeHidden();
    w.check();
  });
}

// Every exercise of the register lab (/journal/matchmakers/) and of the record
// lab (/journal/segments/), both in public/register-lab.js, solved by clicks
// on its moves. A move is named by its data-key (`P>m1:deliver`, `seal:1`),
// from `World::actions` in crates/support/paros-wasm/src/journal/register.rs
// and records.rs; the keys follow the `solution` of each exercise there.
const REGISTER_EXERCISES = {
  "journal/matchmakers/": {
    panel: "#your-turn-register",
    exercises: {
      "register-down": ["P:elect", "P>m1:deliver", "P>m2:deliver", "P>m1:deliver", "P>m2:deliver"],
      "register-race": ["P:elect", "Q:elect", "P>m1:deliver", "P>m2:deliver", "Q>m2:deliver", "Q>m3:deliver",
        "P>m1:deliver", "P>m2:deliver", "Q>m2:deliver", "Q>m3:deliver"],
      "register-minority": ["P:elect", "P>m1:deliver", "P>m2:deliver", "P>m1:lose", "P>m2:drop", "P>m3:drop",
        "Q:elect", "Q>m2:deliver", "Q>m3:deliver", "Q>m2:deliver", "Q>m3:deliver"],
      "register-refused": ["Q:elect", "Q>m2:deliver", "Q>m3:deliver", "Q>m2:deliver", "Q>m3:deliver",
        "P:reshape", "P>m1:deliver", "P>m3:deliver"],
    },
  },
  "journal/segments/": {
    panel: "#your-turn-records",
    exercises: {
      "records-retire": ["reshape", "seal:1", "boundary:1", "retire:1:2"],
      "records-trim": ["reshape", "seal:1", "boundary:1", "truncate", "trim:1"],
      "records-third": ["reshape", "seal:1", "boundary:1", "retire:1:2", "reshape"],
    },
  },
};

for (const [path, { panel, exercises }] of Object.entries(REGISTER_EXERCISES)) {
  for (const [id, keys] of Object.entries(exercises)) {
    test(`/${path}: the exercise "${id}" is solved by its moves`, async ({ page }) => {
      const w = watch(page);
      await page.goto(`${path}#lab=${id}`);
      const lab = page.locator(panel);
      await expect(lab.locator(".lab-tab.now")).toBeVisible();
      await expect(lab.locator(".chip-status.accent").first()).toHaveText("live");
      const done = lab.locator(".lab-done");
      await expect(done).toBeHidden();
      for (const key of keys) {
        await lab.locator(`.lab-extras button[data-key="${key}"]`).click();
        await expect(lab.locator(".rlab-refused")).toHaveCount(0);
      }
      await expect(done, "the page says the goal holds").toBeVisible();
      await expect(done).not.toBeEmpty();
      await expect(lab.locator(".lab-tab.now .lab-tab-n")).toHaveText("✓");
      await lab.getByRole("button", { name: "Undo" }).click();
      await expect(done).toBeHidden();
      w.check();
    });
  }
}

test("/journal/segments/: a refused move names the guard of the record", async ({ page }) => {
  const w = watch(page);
  await page.goto("journal/segments/#lab=records-retire");
  const lab = page.locator("#your-turn-records");
  await lab.locator('.lab-extras button[data-key="seal:1"]').click();
  await expect(lab.locator(".rlab-refused")).toContainText("NoSuccessor");
  w.check();
});
