// Runs the live universe off the page's thread. The page sends commands:
//   {type: "start", config}       a fresh universe (Reset)
//   {type: "act", op}             one operator step, now
//   {type: "speed", value}        simulated ms per real ms
//   {type: "pause", value}        stop or resume the clock
// and receives {type: "frame", frame} about ten times a second, plus
// {type: "acted", index, op} or {type: "error", message}.
import init, { Universe } from "./wasm/paros_playground.js";

const ready = init();
const TICK_MS = 100;
// Never run more than this much simulated time in one tick: a slow tab
// slows the clock instead of freezing.
const MAX_STEP_MS = 2000;

let universe = null;
let speed = 0.2;
let paused = false;
let timer = null;

function tick() {
  if (!universe || paused) return;
  const now = universe.nowMs();
  const step = Math.min(MAX_STEP_MS, Math.max(1, TICK_MS * speed));
  const frame = JSON.parse(universe.advance(now + step));
  self.postMessage({ type: "frame", frame });
  if (frame.error) { clearInterval(timer); timer = null; }
}

self.onmessage = async ({ data }) => {
  await ready;
  try {
    if (data.type === "start") {
      if (universe) universe.free();
      universe = new Universe(JSON.stringify(data.config));
      if (!timer) timer = setInterval(tick, TICK_MS);
    } else if (data.type === "act") {
      const index = universe.act(JSON.stringify(data.op));
      self.postMessage({ type: "acted", index, op: data.op });
    } else if (data.type === "speed") {
      speed = data.value;
    } else if (data.type === "pause") {
      paused = data.value;
    }
  } catch (error) {
    self.postMessage({ type: "error", message: String(error) });
  }
};
