// modes/timeline.js — shared scaffolding for the still storage-fault timelines
// (disk / corruption / CTRL). Mirrors the crash mode's lane layout so the three
// Stage 6–8 views read as one family: per-node lanes on a shared time axis, a
// grey volatile baseline broken by red down windows, and a header verdict line.
// Each mode draws its own lane marks on top via the geometry returned here.

import { C, PHASE } from '../tokens.js';
import { el } from '../svg.js';
import { downWindowsAll, clusterOf } from '../adapter.js';

// Lane geometry for `n` node lanes inside the mode's stage box.
export function timelineGeo(run, dims, opts = {}) {
  const { w, h } = dims;
  const n = clusterOf(run);
  const padL = opts.padL || 110, padR = 30, padT = opts.padT || 96, padB = opts.padB || 44;
  const plotW = w - padL - padR;
  const dur = Math.max(run.sim_duration_ms, 1);
  const xOf = (t) => padL + plotW * Math.max(0, Math.min(1, t / dur));
  const laneH = (h - padT - padB) / n;
  const laneY = (i) => padT + laneH * (i + 0.5);
  return { w, h, n, padL, padR, padT, padB, plotW, dur, xOf, laneH, laneY, axisY: h - padB };
}

// Header: title + a colour-coded verdict line (words, never glyphs alone).
// `verdict.color` overrides the default green/red, e.g. amber for a liveness
// pause that is not a safety loss.
export function drawHeader(g, title, verdict) {
  g.appendChild(el('text', { x: 12, y: 30, class: 'mono', 'font-size': 13, 'font-weight': 700, fill: C.text }, title));
  const fill = verdict.color || (verdict.ok ? PHASE.chosen : PHASE.nack);
  g.appendChild(el('text', { x: 12, y: 50, 'font-size': 12, 'font-weight': 700, fill }, verdict.text));
}

// Time axis + gridlines under the lanes.
export function drawAxis(g, geo) {
  g.appendChild(el('line', { x1: geo.padL, y1: geo.axisY, x2: geo.w - geo.padR, y2: geo.axisY, stroke: C.hairline, 'stroke-width': 1 }));
  for (let k = 0; k <= 4; k++) {
    const t = geo.dur * k / 4, x = geo.xOf(t);
    g.appendChild(el('line', { x1: x, y1: geo.padT - 6, x2: x, y2: geo.axisY, stroke: C.softline, 'stroke-width': 1 }));
    g.appendChild(el('text', { x, y: geo.axisY + 16, 'text-anchor': 'middle', class: 'mono', 'font-size': 10, fill: C.faint }, `${Math.round(t)}ms`));
  }
}

// One lane's base: label, volatile baseline, red down windows (seam + storage
// crashes merged). Returns the down windows so a mode can annotate them.
export function drawLaneBase(g, geo, run, i, label) {
  const y = geo.laneY(i);
  const wins = downWindowsAll(run, i);
  g.appendChild(el('line', { x1: geo.padL, y1: y, x2: geo.w - geo.padR, y2: y, stroke: '#3d444d', 'stroke-width': 2 }));
  for (const win of wins) {
    const x0 = geo.xOf(win.start), x1 = Math.max(geo.xOf(win.end), x0 + 6);
    g.appendChild(el('rect', { x: x0, y: y - 13, width: x1 - x0, height: 26, fill: PHASE.nack, 'fill-opacity': 0.14, stroke: PHASE.nack, 'stroke-opacity': 0.5, 'stroke-width': 1, 'stroke-dasharray': '3 3' }));
    if (x1 - x0 > 30) g.appendChild(el('text', { x: (x0 + x1) / 2, y: y - 16, 'text-anchor': 'middle', 'font-size': 9, fill: PHASE.nack }, 'down'));
  }
  g.appendChild(el('text', { x: 12, y: y - 2, class: 'mono', 'font-size': 12, 'font-weight': 700, fill: C.text }, `N${i}`));
  if (label) g.appendChild(el('text', { x: 12, y: y + 13, class: 'mono', 'font-size': 9, fill: C.muted }, label));
  return wins;
}

// A legend row built from vector marks; items are { mark: (x, y) => node, label }.
export function drawLegend(g, x, y, items) {
  for (const it of items) {
    g.appendChild(it.mark(x + 5, y));
    g.appendChild(el('text', { x: x + 14, y: y + 4, 'font-size': 11, fill: C.muted }, it.label));
    x += 14 + it.label.length * 6 + 26;
  }
}

// Stacked tag rows so labels landing close in time do not overprint (three
// rows above the lane, same trick as the crash mode).
export function tagStacker() {
  const rowEnd = [-Infinity, -Infinity, -Infinity];
  return (x, tag) => {
    const half = tag.length * 5.4 / 2;
    let row = rowEnd.findIndex((end) => x - half > end);
    if (row < 0) row = rowEnd.indexOf(Math.min(...rowEnd));
    rowEnd[row] = x + half;
    return row;
  };
}
