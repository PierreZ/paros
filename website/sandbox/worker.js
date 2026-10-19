// Runs the sandbox's wasm off the page's thread: one message in (a request
// `{config, actions}`), one message out (the run, as JSON text).
import init, { runSandbox } from "./wasm/paros_playground.js";

const ready = init();

self.onmessage = async (message) => {
  await ready;
  const started = performance.now();
  const json = runSandbox(JSON.stringify(message.data));
  self.postMessage({ json, wallMs: Math.round(performance.now() - started) });
};
