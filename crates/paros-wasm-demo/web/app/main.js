// main.js — the entry point: init wasm, parse the URL contract, boot the shell.
//
// Preserves every query-param contract from the original demo:
//   ?embed        hide page chrome + report height to the parent (book iframes)
//   ?mode=        single (default) | multi | crash — chosen client-side
//   ?seed=        the seed to run (crash defaults to 99)
//   ?still=<k>    render one frozen deterministic frame (screenshots)
//   ?dump         dump the raw RunResult JSON and stop
// The wasm boundary is unchanged: `runSeed(seed)` returns the RunResult JSON.

import init, { runSeed } from '../pkg/paros_wasm_demo.js';
import { MODES, modeFromParam } from './registry.js';
import { Shell } from './shell.js';

const params = new URLSearchParams(location.search);
const DUMP = params.has('dump');
const EMBED = params.has('embed');
const modeId = modeFromParam(params.get('mode'));

// default seed: crash mode uses a seed that crashes a node at the seam
let seed = params.has('seed') ? params.get('seed') : (modeId === 'crash' ? '99' : '0');

const statusEl = document.getElementById('status');
const rawEl = document.getElementById('raw');
const rootEl = document.getElementById('paros-root');

// Posts the content height to the parent once real content exists (no-op outside
// ?embed). Gated: the load event and early timers fire while the document is still
// the bare ~50px status line — posting that height squashes the iframe to the
// parent's 200px floor until the synchronous wasm run finally unblocks this frame.
let reportHeight = () => {};

if (EMBED) {
  document.body.classList.add('embed');
  // report our content height to the parent page so it can auto-size the iframe.
  // Do NOT rename the message: iframe-autosize.js listens for `paros-resize`.
  if (window.parent !== window) {
    let ready = false;
    const postHeight = () => {
      if (ready) window.parent.postMessage({ type: 'paros-resize', height: document.body.offsetHeight }, '*');
    };
    reportHeight = () => { ready = true; postHeight(); };
    if ('ResizeObserver' in window) new ResizeObserver(postHeight).observe(document.body);
    window.addEventListener('load', postHeight);
    // a couple of delayed posts to catch async layout (fonts, wasm-driven render)
    setTimeout(postHeight, 200);
    setTimeout(postHeight, 800);
  }
}

// Run one seed in wasm and return the parsed RunResult (or dump raw JSON).
function runOne(s) {
  const json = runSeed(BigInt(s || '0'));
  if (DUMP) {
    if (rawEl) { rawEl.style.display = 'block'; rawEl.textContent = json; }
    if (statusEl) statusEl.textContent = `dumped seed ${s}`;
    reportHeight();
    return null;
  }
  return JSON.parse(json);
}

let shell = null;

function boot(run) {
  shell = new Shell(rootEl, {
    registry: MODES,
    modeId,
    run,
    seed,
    params,
    // called on tab switch (id only) or when the seed field is submitted (id, newSeed)
    onModeChange: (id, newSeed) => {
      if (newSeed !== undefined && newSeed !== seed) {
        seed = newSeed;
        const nextRun = runOne(seed);
        if (nextRun) shell.loadRun(nextRun, seed);
      }
    },
  });
}

if (statusEl) statusEl.textContent = 'loading wasm…';
init()
  .then(() => {
    const run = runOne(seed);
    if (!run) return; // ?dump path
    if (statusEl) statusEl.style.display = 'none';
    boot(run);
    reportHeight();
  })
  .catch((e) => {
    if (statusEl) statusEl.textContent = 'failed to load wasm: ' + e;
    reportHeight();
    // eslint-disable-next-line no-console
    console.error(e);
  });
