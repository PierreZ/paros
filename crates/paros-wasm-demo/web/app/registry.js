// registry.js — the MODES map.
//
// A mode is a renderer object implementing the interface documented in the README
// ("Adding a mode"). To add one (e.g. an animated crash, or a reconfiguration
// mode), write a renderer and add it here — the shell needs no edits. The three
// v1 modes are proof: single/multi/crash all plug in through this one map.

import { single } from './modes/single.js';
import { multi } from './modes/multi.js';
import { crash } from './modes/crash.js';

export const MODES = {
  single,
  multi,
  crash,
};

// Resolve the ?mode= param to a mode id (single is the default).
export function modeFromParam(mode) {
  if (mode === 'multi') return 'multi';
  if (mode === 'crash') return 'crash';
  return 'single';
}
