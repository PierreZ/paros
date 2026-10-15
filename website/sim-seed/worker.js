// Runs one seed of the paros simulation off the page's thread: one message
// in (the seed, as a decimal string), one message out (the run, as JSON text).
import init, { runSeed } from "./wasm/paros_sim_web.js";

const ready = init();

self.onmessage = async (message) => {
  await ready;
  const started = performance.now();
  const json = runSeed(String(message.data));
  self.postMessage({ json, wallMs: Math.round(performance.now() - started) });
};
