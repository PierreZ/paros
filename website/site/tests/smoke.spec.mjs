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
//     does not reach its last step, a story player that does not play or
//     does not stop;
//   - a tour (public/tour.js) whose title, chapter menu, bridge or chapter
//     bar is missing, or a flavor page whose "Watch this flavor" does not
//     play its chapter;
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
// The tour page inlines the runs of its chapters, with no step-through.
const html = (p) => readFileSync(join(DIST, p, "index.html"), "utf8");
const TRACED = PAGES.filter((p) => html(p).includes('id="trace"') && !html(p).includes('data-tour="on"'));

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

// The player of the story (public/story.js): Play moves on by itself, Pause
// stops it, any other control stops it, and with reduced motion there is no
// Play (the other tests run with reduced motion).
test("/paxos/single-decree/: the story player plays, pauses and stops on a control", async ({ browser }) => {
  const context = await browser.newContext({ reducedMotion: "no-preference", baseURL: test.info().project.use.baseURL });
  const page = await context.newPage();
  const w = watch(page);
  await page.goto("paxos/single-decree/");
  const story = page.locator(".panel.story");
  const counter = story.locator(".panel-head .meta");
  const play = story.locator(".story-play");
  await expect(counter).toHaveText(/^screen 1 \//);
  await expect(play).toBeVisible();
  await expect(story.locator(".story-mark")).toHaveCount(await page.evaluate(() => {
    const t = JSON.parse(document.getElementById("trace").textContent);
    return t.scenarios.find((s) => s.beats && s.beats.length).beats.length;
  }));
  const playing = () => page.evaluate(() => window.paxosStory.playing());
  // Fast, so that the test does not wait long for the time to read. Screen 2
  // rests for 2.2 s / 4 at least, a wide window for the Pause below.
  await page.evaluate(() => window.paxosStory.speed(4));
  await play.click();
  await expect(play).toHaveText("Pause");
  await expect(counter).toHaveText(/^screen 2 \//, { timeout: 20_000 });
  await expect.poll(playing).toBe(true);
  await play.click();
  await expect(play).toHaveText("Play");
  expect(await playing()).toBe(false);
  const stopped = await counter.textContent();
  await page.waitForTimeout(1500);
  await expect(counter).toHaveText(stopped);
  // Played on, the run stops by itself at its first question.
  const ask = await page.evaluate(() => {
    const t = JSON.parse(document.getElementById("trace").textContent);
    return t.scenarios.find((s) => s.beats && s.beats.length).beats.findIndex((b) => b.question);
  });
  expect(ask).toBeGreaterThan(1);
  await page.evaluate(() => window.paxosStory.speed(20));
  await play.click();
  await expect(counter).toHaveText(new RegExp(`^screen ${ask + 1} \\/`), { timeout: 20_000 });
  await expect(play).toHaveText("Play");
  await story.locator("button.story-answer").first().click();
  // Play again, then a press on Back stops it.
  await play.click();
  await expect.poll(playing).toBe(true);
  await story.getByRole("button", { name: "Back" }).click();
  await expect(play).toHaveText("Play");
  // "Show each message" stops it too: the address stays the step-through's.
  await play.click();
  await expect.poll(playing).toBe(true);
  await story.locator(".story-foot a").click();
  expect(await playing()).toBe(false);
  await page.waitForTimeout(1500);
  expect(page.url()).toMatch(/&step=\d+$/);
  await page.locator(".panel.steps").getByText("Back to the story").click();
  await expect(story).toBeVisible();
  expect(await playing()).toBe(false);
  // A mark of the progress line opens its screen and keeps the focus, so
  // the arrow keys go on.
  await story.locator(".story-mark").nth(1).click();
  await expect(counter).toHaveText(/^screen 2 \//);
  await page.keyboard.press("ArrowRight");
  await expect(counter).toHaveText(/^screen 3 \//);
  w.check();
  await context.close();
});

test("/paxos/single-decree/: with reduced motion the story has no Play", async ({ page }) => {
  await page.goto("paxos/single-decree/");
  const story = page.locator(".panel.story");
  await expect(story).toBeVisible();
  await expect(story.getByRole("button", { name: "Back" })).toBeVisible();
  await expect(story.locator(".story-play")).toHaveCount(1);
  await expect(story.locator(".story-play")).toBeHidden();
  await expect(story.locator(".story-speed")).toBeHidden();
});

// The tour (public/tour.js, src/tour.ts): the title screen with Play and the
// chapter menu, a chapter started from the menu opens on its bridge, then its
// first run plays in watch mode (no question), and the address names the
// chapter only. Chapter 1 has no bridge.
test("/paxos/tour/: the title, the chapter menu, a bridge and a chapter", async ({ page }) => {
  const w = watch(page);
  await page.setViewportSize({ width: 390, height: 660 });
  await page.goto("paxos/tour/");
  const screen = page.locator(".tour-screen");
  await expect(screen).toBeVisible();
  await expect(screen.locator(".tour-play")).toBeVisible();
  const chapters = screen.locator(".tour-chapter");
  await expect(chapters).toHaveCount(7);
  await expect(page.locator(".panel.story")).toBeHidden();
  // The title screen fits a phone: Play is above the fold.
  const box = await screen.locator(".tour-play").boundingBox();
  expect(box.y + box.height).toBeLessThanOrEqual(660);
  await chapters.nth(2).click();
  await expect(screen.locator(".tour-bridge")).toBeVisible();
  expect(page.url()).toMatch(/#chapter=the-log$/);
  await screen.locator(".tour-next").click();
  const story = page.locator(".panel.story");
  await expect(story).toBeVisible();
  await expect(story.locator(".tour-bar")).toBeVisible();
  await expect(story.locator(".tour-bar")).toContainText("Chapter 3/7");
  await expect(story.locator(".story-mark.ask")).toHaveCount(0);
  // Every chapter fits a 390 x 660 phone under the site's header.
  const fits = await page.evaluate(() => {
    const header = document.querySelector("header");
    const out = [];
    for (let k = 0; k < 7; k++) {
      document.querySelector(".tour-menu-btn").click();
      document.querySelectorAll(".tour-chapter")[k].click();
      const next = document.querySelector(".tour-screen:not([hidden]) .tour-next");
      if (next) next.click();
      // Each screen of the chapter's runs, from its first to its end.
      const panel = document.querySelector(".panel.story");
      const cont = [...panel.querySelectorAll(".story-controls button")].find((b) => b.textContent === "Continue");
      let height = 0;
      let screens = 0;
      for (let guard = 0; !panel.hidden && guard < 200; guard++) {
        height = Math.max(height, panel.getBoundingClientRect().height + header.getBoundingClientRect().height);
        screens += 1;
        cont.click();
      }
      out.push({ chapter: k + 1, height, screens });
    }
    return out;
  });
  // The screens of each chapter: the beats of its runs (the tour data).
  const beats = await page.evaluate(() => {
    const trace = JSON.parse(document.getElementById("trace").textContent);
    const tour = JSON.parse(document.getElementById("tour").textContent);
    return tour.chapters.map((ch) => ch.runs.reduce((n, id) => n + trace.scenarios.find((s) => s.id === id).beats.length, 0));
  });
  expect(fits.length).toBe(7);
  for (const f of fits) {
    expect(f.screens, `chapter ${f.chapter} shows each screen`).toBe(beats[f.chapter - 1]);
    expect(f.height, `chapter ${f.chapter} is drawn`).toBeGreaterThan(200);
    expect(f.height, `chapter ${f.chapter} fits 660 px`).toBeLessThanOrEqual(660);
  }
  // The end of the last chapter: the tour is done, and the menu is there.
  await expect(screen.locator(".tour-done")).toHaveText("The tour is done.");
  // The end of the tour has no main button: the focus is still on a button
  // of the screen, so the keys go on.
  expect(await page.evaluate(() => !!document.activeElement && !!document.activeElement.closest(".tour-screen"))).toBe(true);
  // Chapter 2 has no bridge: it opens on its first run.
  await chapters.nth(1).click();
  await expect(screen).toBeHidden();
  await expect(story.locator(".tour-bar")).toContainText("Chapter 2/7");
  await expect(story.locator(".story-quiz")).toBeHidden();
  await expect(story.locator(".story-foot")).toBeHidden();
  // Back to the menu; chapter 1 opens on its first run, with no bridge.
  await story.locator(".tour-menu-btn").click();
  await chapters.nth(0).click();
  await expect(story).toBeVisible();
  await expect(story.locator(".tour-bar")).toContainText("Chapter 1/7");
  w.check();
});

// The tour with motion: Play goes from run to run and from a chapter to its
// end, the countdown shows and Stop stops it, the keys work, a hidden page
// stops the countdown, and a chapter opened from the address does not move.
test("/paxos/tour/: Play goes on from run to run, the countdown stops, and Space pauses", async ({ browser }) => {
  const context = await browser.newContext({ reducedMotion: "no-preference" });
  const page = await context.newPage();
  const w = watch(page);
  await page.goto("paxos/tour/#chapter=the-log");
  const screen = page.locator(".tour-screen");
  const story = page.locator(".panel.story");
  const playing = () => page.evaluate(() => window.paxosStory.playing());
  // The chapter opens on its bridge, the problem line of Multi-Paxos.
  await expect(screen.locator(".tour-bridge")).toHaveText("A journal needs many values in order, and two round trips per record is slow.");
  await page.evaluate(() => window.paxosStory.speed(20));
  await page.keyboard.press(" ");
  await expect(story).toBeVisible();
  await expect.poll(playing).toBe(true);
  // Space on the story pauses it, and Play goes on.
  await page.keyboard.press(" ");
  await expect.poll(playing).toBe(false);
  await story.locator(".story-play").click();
  // Run 1 ends: run 2 plays with no press.
  await expect(story.locator(".tour-bar")).toContainText("run 2/2", { timeout: 30_000 });
  await expect.poll(playing).toBe(true);
  // The chapter ends: the countdown shows, and Stop stops it.
  await expect(screen.locator(".tour-count")).toContainText("Next chapter in", { timeout: 30_000 });
  await screen.locator(".tour-stop").click();
  await expect(screen.locator(".tour-count")).toHaveText("");
  // Longer than the countdown: Stop stopped the timer, not only its text.
  await page.waitForTimeout(6000);
  await expect(screen.locator(".tour-done")).toBeVisible();
  // Next chapter: "Reads" has no bridge, so its first run plays.
  await screen.locator(".tour-next").click();
  await expect(story.locator(".tour-bar")).toContainText("Chapter 4/7");
  await expect.poll(playing).toBe(true);
  // To its end, then a hidden page stops the countdown and its text.
  await expect(screen.locator(".tour-count")).toContainText("Next chapter in", { timeout: 30_000 });
  await page.evaluate(() => {
    Object.defineProperty(document, "hidden", { configurable: true, get: () => true });
    document.dispatchEvent(new Event("visibilitychange"));
  });
  await expect(screen.locator(".tour-count")).toHaveText("");
  await page.waitForTimeout(6000);
  await expect(screen.locator(".tour-done")).toBeVisible();
  w.check();
  await context.close();
});

test("/paxos/tour/: a chapter opened from the address does not move before Play", async ({ browser }) => {
  const context = await browser.newContext({ reducedMotion: "no-preference" });
  const page = await context.newPage();
  const w = watch(page);
  await page.goto("paxos/tour/#chapter=one-value");
  const story = page.locator(".panel.story");
  await expect(story).toBeVisible();
  await expect(story.locator(".story-controls .btn.primary")).toHaveText("Continue");
  expect(await page.evaluate(() => window.paxosStory.playing())).toBe(false);
  w.check();
  await context.close();
});

test("/paxos/single-decree/: Watch this flavor plays the chapter in place, and Stop watching leaves it", async ({ page }) => {
  const w = watch(page);
  await page.goto("paxos/single-decree/");
  const story = page.locator(".panel.story");
  const watchBtn = story.locator(".tour-watch");
  await expect(watchBtn).toHaveText(/^▶ Watch this flavor \(\d+ min\)$/);
  const before = new URL(page.url()).hash;
  await watchBtn.click();
  await expect(watchBtn).toBeHidden();
  await expect(story.locator(".tour-bar")).toBeVisible();
  await expect(story.locator(".tour-bar")).toContainText("Chapter 1/7");
  await expect(story.locator(".story-foot")).toBeHidden();
  expect(await page.evaluate(() => window.paxosStory.watching())).toBe(true);
  await story.locator(".tour-menu-btn").click();
  expect(await page.evaluate(() => window.paxosStory.watching())).toBe(false);
  await expect(story.locator(".story-foot")).toBeVisible();
  await expect(watchBtn).toBeVisible();
  // The lesson comes back: its questions, and the address of the reader's
  // own place (the first screen, untouched: the address as it was).
  await expect(story.locator(".story-mark.ask")).not.toHaveCount(0);
  expect(new URL(page.url()).hash).toBe(before);
  // Go deeper on a flavor page opens the lesson in place.
  await watchBtn.click();
  await page.evaluate(() => {
    const p = document.querySelector(".panel.story");
    const cont = [...p.querySelectorAll(".story-controls button")].find((b) => b.textContent === "Continue");
    for (let i = 0; i < 100 && !p.hidden; i++) cont.click();
  });
  const end = page.locator(".tour-screen");
  await expect(end.locator(".tour-done")).toHaveText("This chapter is done.");
  // The place in the whole tour moves to the next chapter of the course,
  // which is on another page.
  expect(await page.evaluate(() => JSON.parse(localStorage.getItem("paros-course-v1")).tour.at.chapter)).toBe("majority");
  await end.locator(".tour-deeper").click();
  await expect(story).toBeVisible();
  expect(await page.evaluate(() => window.paxosStory.watching())).toBe(false);
  await expect(story.locator(".panel-head .meta")).toHaveText(/^screen 1 \//);
  w.check();
});

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
  { page: "paxos/quorums/", id: "majority", guided: true, moves: [["settle", { notTo: "n3" }], ["drop", { to: "n3" }]] },
  {
    page: "paxos/multi-paxos/",
    id: "gap",
    guided: true,
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
    guided: true,
    moves: [["extra", "The client writes alpha"], ["settle", {}], ["extra", "The client writes bravo"], ["settle", {}]],
  },
  {
    page: "paxos/compartmentalized/",
    id: "proxy-down",
    moves: [
      ["extra", "The client writes bravo"],
      ["settle", {}],
      ["extra", "Node 0 beats"],
      ["settle", {}],
      ["extra", "Node 0 beats"],
      ["settle", {}],
    ],
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
    // The stage of the story (public/stage.js) draws the parties from the
    // engine's typed state: a log world names its leader.
    await expect(lab.locator(".stage-node").first()).toBeVisible();
    // A guided exercise opens on its intro; Skip leaves it set up.
    if (ex.guided) {
      await expect(lab.locator(".lab-narrator")).toBeVisible();
      await lab.locator(".lab-narrator-skip").click();
      await expect(lab.locator(".lab-narrator")).toBeHidden();
    }
    if (ex.id === "gap") await expect(lab.locator(".stage-node.leader")).toHaveCount(1);
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
// A guided exercise (lab.js, `parts` in paros-wasm's lab.rs), on "gap":
// the intro plays step by step with Next (reduced motion: no Play), the goal
// is a checklist, the Hint has three levels and the third outlines the
// button to press, a dead end says so and Undo leaves it, and "Show me how"
// plays the guide to its end with every line of the checklist marked, but
// never counts as solved; "Now you" starts again.
test("/paxos/multi-paxos/: the guided exercise: intro, checklist, hints, dead end, Show me how", async ({ page }) => {
  const w = watch(page);
  await page.goto("paxos/multi-paxos/#lab=gap");
  const lab = page.locator("#your-turn-lab");
  const narr = lab.locator(".lab-narrator");
  await expect(narr).toBeVisible();
  await expect(lab.locator(".lab-narrator-play")).toBeHidden();
  await expect(lab.locator(".lab-narrator-at")).toHaveText("Intro 1/2");
  // The reader's moves wait while the intro plays.
  await expect(lab.locator(".lab-extras .btn").first()).toBeDisabled();
  await lab.locator(".lab-narrator-bar .btn", { hasText: /^Next$/ }).click();
  await expect(lab.locator(".lab-narrator-at")).toHaveText("Intro 2/2");
  await lab.locator(".lab-narrator-bar .btn", { hasText: /^Next$/ }).click();
  await expect(narr).toBeHidden();
  await expect(lab.locator(".lab-chosen")).toContainText("slot 0 = alpha");
  // The checklist: four lines, none holds yet.
  const parts = lab.locator(".lab-parts li");
  await expect(parts).toHaveCount(4);
  await expect(lab.locator(".lab-parts li.holds")).toHaveCount(0);
  // Three levels of hint; the third outlines the move to press.
  const hint = lab.locator(".lab-hint-btn");
  await hint.click();
  await expect(lab.locator(".lab-hint p")).toHaveCount(1);
  await hint.click();
  await expect(lab.locator(".lab-hint p")).toHaveCount(2);
  await hint.click();
  await expect(lab.locator(".btn.lab-next")).toHaveText("The client writes bravo");
  await lab.locator(".btn.lab-next").click();
  // The hint of the same line goes on outlining the next move: an Accept to drop.
  await expect(lab.locator(".btn.lab-next")).toHaveText("Drop");
  // A dead end: an Accept of slot 1 reaches node 1.
  await lab.locator('.lab-msg[data-kind="Accept"][data-to="n1"] .lab-deliver').click();
  const stuck = lab.locator(".lab-stuck");
  await expect(stuck).toBeVisible();
  await expect(lab.locator(".lab-hint-btn")).toBeHidden();
  await stuck.locator(".btn", { hasText: "Undo" }).click();
  await expect(stuck).toBeHidden();
  // Show me how: the guide from the start, step by step, to its end.
  await lab.locator(".lab-show-btn").click();
  await expect(narr).toBeVisible();
  await expect(lab.locator(".lab-narrator-at")).toHaveText(/^Step 1\/\d+$/);
  // While the reader reads why, the button of the first move (an extra
  // move) has the outline; then the next step's first move (a Drop).
  await expect(lab.locator(".lab-extras .btn.lab-next")).toHaveText("The client writes bravo");
  await lab.locator(".lab-narrator-bar .btn", { hasText: /^Next$/ }).click();
  await expect(lab.locator(".lab-narrator-at")).toHaveText(/^Step 2\/\d+$/);
  await expect(lab.locator(".lab-wire .btn.lab-next")).toHaveText("Drop");
  for (let i = 0; i < 20 && (await lab.locator(".lab-narrator-skip").textContent()) !== "Now you"; i++) {
    await lab.locator(".lab-narrator-bar .btn", { hasText: /^Next$/ }).click();
  }
  await expect(lab.locator(".lab-narrator-skip")).toHaveText("Now you");
  await expect(lab.locator(".lab-parts li.holds")).toHaveCount(4);
  await expect(lab.locator(".lab-done")).toBeHidden();
  expect(await page.evaluate(() => {
    const st = JSON.parse(localStorage.getItem("paros-course-v1") || "{}");
    return !!(st.solved && st.solved.gap);
  })).toBe(false);
  // Now you: the exercise starts again, with no intro.
  await lab.locator(".lab-narrator-skip").click();
  await expect(narr).toBeHidden();
  await expect(lab.locator(".lab-parts li.holds")).toHaveCount(0);
  await expect(lab.locator(".lab-extras .btn").first()).toBeEnabled();
  w.check();
});

// "Show me how" with motion: the press plays the guide by itself, each
// move's button is outlined before it plays, Pause stops it, and Play goes
// on to the end; Skip during a moving intro leaves the lab set up.
test("/paxos/multi-paxos/: Show me how plays by itself with motion, and Skip stops a moving intro", async ({ browser }) => {
  const context = await browser.newContext({ reducedMotion: "no-preference" });
  const page = await context.newPage();
  const w = watch(page);
  await page.goto("paxos/multi-paxos/#lab=gap");
  const lab = page.locator("#your-turn-lab");
  await expect(lab.locator(".lab-narrator")).toBeVisible();
  // Play the intro, then Skip while its messages move.
  await lab.locator(".lab-narrator-play").click();
  await expect(lab.locator(".lab-narrator-at")).toHaveText("Intro 1/2");
  await page.waitForTimeout(600);
  await lab.locator(".lab-narrator-skip").click();
  await expect(lab.locator(".lab-narrator")).toBeHidden();
  await expect(lab.locator(".lab-chosen")).toContainText("slot 0 = alpha");
  await expect(lab.locator(".lab-extras .btn").first()).toBeEnabled();
  await expect(lab.locator(".lab-msg")).toHaveCount(0);
  // Show me how plays by itself, at the fastest speed.
  await lab.locator(".lab-show-btn").click();
  await lab.locator(".lab-narrator-speed").click();
  await expect(lab.locator(".lab-narrator-play")).toHaveText("Pause");
  await expect(lab.locator(".lab-narrator-at")).toHaveText(/^Step 2\//, { timeout: 20000 });
  // Pause: a step that plays ends, and then nothing moves on.
  await lab.locator(".lab-narrator-play").click();
  await expect(lab.locator(".lab-narrator-play")).toHaveText("Play");
  await page.waitForTimeout(8000);
  const at = await lab.locator(".lab-narrator-at").textContent();
  await page.waitForTimeout(3000);
  await expect(lab.locator(".lab-narrator-at")).toHaveText(at);
  await lab.locator(".lab-narrator-play").click();
  await expect(lab.locator(".lab-narrator-skip")).toHaveText("Now you", { timeout: 90000 });
  await expect(lab.locator(".lab-parts li.holds")).toHaveCount(4);
  await expect(lab.locator(".lab-done")).toBeHidden();
  await lab.locator(".lab-narrator-skip").click();
  await expect(lab.locator(".lab-parts li.holds")).toHaveCount(0);
  w.check();
  await context.close();
});

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

// The live journal (/journal/live/, public/live-journal.js): the whole
// journal on six hosts in the page. Writes land, a crashed host does not stop
// them, a read gives them back, and a roll puts segment 2 in force.
test("/journal/live/: the live journal writes, survives a crash, reads and rolls", async ({ page }) => {
  test.setTimeout(120_000);
  const w = watch(page);
  await page.goto("journal/live/");
  const lab = page.locator("#live-journal");
  await expect(lab.locator(".chip-status.accent").first()).toHaveText("live");
  await lab.getByRole("button", { name: "fast" }).click();
  // The page starts paused with reduced motion on (playwright.config.mjs).
  const play = lab.locator('button[data-live="play"]');
  if ((await play.textContent()) === "Play") await play.click();
  await expect(play).toHaveText("Pause");
  const written = lab.locator(".llab-summary .strip > div", { hasText: "writes done" }).locator(".v");
  const append = lab.locator('button[data-live="append"]');
  const wait = { timeout: 30_000 };
  await expect(append).toBeEnabled(wait);
  await append.click();
  await expect(written).toHaveText("1", wait);
  await lab.locator('button[data-live="crash:3"]').click();
  await expect(lab.locator('[data-host="3"] .chip-status')).toHaveText("down");
  await expect(append).toBeEnabled(wait);
  await append.click();
  await expect(written).toHaveText("2", wait);
  await lab.locator('button[data-live="restart:3"]').click();
  await expect(lab.locator('[data-host="3"] .chip-status')).toHaveText("up");
  await expect(lab.locator('button[data-live="read"]')).toBeEnabled(wait);
  await lab.locator('button[data-live="read"]').click();
  await expect(lab.locator(".llab-records li")).toHaveText(["record 0", "record 1"], wait);
  await lab.locator('button[data-live="roll"]').click();
  await expect(lab.locator('.llab-segments [data-segment="2"] .chip-status')).toHaveText("in force", wait);
  await expect(lab.locator('.llab-segments [data-segment="1"] .chip-status')).not.toHaveText("in force");
  w.check();
});

test("/journal/segments/: a refused move names the guard of the record", async ({ page }) => {
  const w = watch(page);
  await page.goto("journal/segments/#lab=records-retire");
  const lab = page.locator("#your-turn-records");
  await lab.locator('.lab-extras button[data-key="seal:1"]').click();
  await expect(lab.locator(".rlab-refused")).toContainText("NoSuccessor");
  w.check();
});
