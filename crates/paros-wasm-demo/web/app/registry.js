// registry.js — the MODES map.
//
// A mode is a renderer object implementing the interface documented in the README
// ("Adding a mode"). To add one (e.g. a reconfiguration mode), write a renderer
// and add it here — the shell needs no edits. The eight modes are proof: the
// v1 trio (single/multi/crash) and the five stage views (reads, catch-up &
// snapshots, disk faults, corruption, ctrl recovery) all plug in through this
// one map.

import { single } from './modes/single.js';
import { multi } from './modes/multi.js';
import { read } from './modes/read.js';
import { log } from './modes/log.js';
import { crash } from './modes/crash.js';
import { disk } from './modes/disk.js';
import { corrupt } from './modes/corrupt.js';
import { ctrl } from './modes/ctrl.js';

export const MODES = {
  single,
  multi,
  read,
  log,
  crash,
  disk,
  corrupt,
  ctrl,
};

// Resolve the ?mode= param to a mode id (single is the default).
export function modeFromParam(mode) {
  return Object.prototype.hasOwnProperty.call(MODES, mode) && mode !== 'single' ? mode : 'single';
}

// The default seed for a mode: its first curated lesson (single keeps seed 0).
export function defaultSeed(modeId) {
  const m = MODES[modeId];
  return m && m.seeds && m.seeds.length ? String(m.seeds[0].value) : '0';
}
