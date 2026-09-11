// Briefings are markdown, authored in `crates/paros-play/src/level/`.
//
// The source is the game's own binary — not user input and not the network —
// so rendering it into `innerHTML` is rendering our own text. `marked` is
// configured synchronously because the whole app renders in one pass.

import { marked } from 'marked';

marked.setOptions({ async: false, gfm: true, breaks: false });

/** Render markdown into a block element. */
export function markdown(source: string, className = 'prose'): HTMLDivElement {
  const host = document.createElement('div');
  host.className = className;
  host.innerHTML = marked.parse(source) as string;
  return host;
}
