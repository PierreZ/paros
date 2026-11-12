// The engine of the Engineering pages' live widgets (#951 (live Engineering
// pages)): the playground engine (paros + moonpool-sim compiled to wasm,
// staged at /playground/app/wasm/ by scripts/build-wasm.sh) plays one whole
// action log per request, off the page's thread.
//   in:  {config, actions}
//   out: {json}, the run as JSON (a RunView, or {"error": …})
import init, { runJson } from "./playground/app/wasm/paros_playground.js";

const ready = init();

self.onmessage = async ({ data }) => {
  try {
    await ready;
    self.postMessage({ json: runJson(JSON.stringify(data)) });
  } catch (error) {
    self.postMessage({ json: JSON.stringify({ error: String(error) }) });
  }
};
