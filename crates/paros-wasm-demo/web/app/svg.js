// svg.js — minimal SVG construction helpers plus the vector glyph library.
//
// All state/fault badges are drawn as vector polygons/paths (never font icons),
// so a reader on a machine without an emoji font never sees a missing-glyph box.
// Everything here is pure DOM construction; no drawing state, no globals.

import { C, PHASE } from './tokens.js';

const NS = 'http://www.w3.org/2000/svg';

// smoothstep easing, and a point at parameter t along a quadratic arc from→to.
// The control point is offset along the chord's left normal so the two directions
// of a node pair (e.g. Prepare 0→1 and Promise 1→0) curve on opposite sides.
export const ease = (t) => t * t * (3 - 2 * t);
export function arcPoint(from, to, t) {
  const mx = (from.x + to.x) / 2, my = (from.y + to.y) / 2;
  const dx = to.x - from.x, dy = to.y - from.y, len = Math.hypot(dx, dy) || 1;
  const curve = Math.min(0.22 * len, 60);
  const cx = mx + (-dy / len) * curve, cy = my + (dx / len) * curve;
  const u = 1 - t;
  return {
    x: u * u * from.x + 2 * u * t * cx + t * t * to.x,
    y: u * u * from.y + 2 * u * t * cy + t * t * to.y,
  };
}
// Build an SVG quadratic-arc path string matching arcPoint's control point.
export function arcPath(from, to) {
  const mx = (from.x + to.x) / 2, my = (from.y + to.y) / 2;
  const dx = to.x - from.x, dy = to.y - from.y, len = Math.hypot(dx, dy) || 1;
  const curve = Math.min(0.22 * len, 60);
  const cx = mx + (-dy / len) * curve, cy = my + (dx / len) * curve;
  return `M ${from.x} ${from.y} Q ${cx.toFixed(2)} ${cy.toFixed(2)} ${to.x} ${to.y}`;
}

// Create an SVG element and set attributes. Children may be nodes or strings.
export function el(name, attrs = {}, children = []) {
  const node = document.createElementNS(NS, name);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === null || v === undefined) continue;
    node.setAttribute(k, String(v));
  }
  for (const c of [].concat(children)) {
    if (c == null) continue;
    node.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
  }
  return node;
}

export function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
  return node;
}

// A soft outer glow: a blurred filled circle behind an element. Used sparingly —
// the acting node's halo and the message particle's halo are the only glows.
export function glow(cx, cy, r, color, opacity = 0.1) {
  return el('circle', {
    cx, cy, r, fill: color, opacity,
    filter: 'url(#paros-blur)',
  });
}

// The blur filter definition (added once to the stage <defs>).
export function defs() {
  const f = el('filter', {
    id: 'paros-blur', x: '-60%', y: '-60%', width: '220%', height: '220%',
  }, [el('feGaussianBlur', { in: 'SourceGraphic', stdDeviation: 3.2 })]);
  return el('defs', {}, [f]);
}

// A quiet node: filled disc + a ring. `ring` is the ring colour; when `active`
// the ring takes the phase colour and gets a blurred halo behind it.
export function nodeDisc(cx, cy, r, label, { ring = C.neutralRing, active = false, glowColor } = {}) {
  const g = el('g', { class: 'paros-node' });
  if (active && glowColor) g.appendChild(glow(cx, cy, r + 7, glowColor, 0.14));
  g.appendChild(el('circle', { cx, cy, r, fill: C.nodeFill, stroke: ring, 'stroke-width': 2 }));
  g.appendChild(el('text', {
    x: cx, y: cy, 'text-anchor': 'middle', 'dominant-baseline': 'central',
    class: 'mono', 'font-size': r * 0.66, 'font-weight': 700, fill: active ? glowColor || ring : C.text,
  }, label));
  return g;
}

// A glowing message particle at (x,y): a blurred halo, a phase-coloured dot, and
// a small white core. Exactly one active particle *set* per step.
export function particle(x, y, color) {
  return el('g', {}, [
    glow(x, y, 6, color, 0.5),
    el('circle', { cx: x, cy: y, r: 3.4, fill: color }),
    el('circle', { cx: x, cy: y, r: 1.4, fill: C.white }),
  ]);
}

// A hairline message link between two points.
export function link(x1, y1, x2, y2, color, opacity = 0.32) {
  return el('line', { x1, y1, x2, y2, stroke: color, 'stroke-width': 1.2, opacity });
}

// ---- vector badge glyphs (top-right of a node, offset ≈ (+dx,-dy)) ----------
// Each returns a <g> centred on (cx,cy); the caller positions it.

export function badgeCrash(cx, cy, color = PHASE.nack) {
  // lightning bolt polygon
  const p = `${cx - 2},${cy - 6} ${cx + 3},${cy - 6} ${cx - 1},${cy} ${cx + 2},${cy} ${cx - 3},${cy + 7} ${cx - 1},${cy + 1} ${cx - 4},${cy + 1}`;
  return el('g', {}, [el('polygon', { points: p, fill: color })]);
}

export function badgeReboot(cx, cy, color = PHASE.prepare) {
  // ~300° refresh arc with an arrowhead
  const r = 5;
  const a0 = -0.3, a1 = Math.PI * 1.7;
  const x0 = cx + r * Math.cos(a0), y0 = cy + r * Math.sin(a0);
  const x1 = cx + r * Math.cos(a1), y1 = cy + r * Math.sin(a1);
  const arc = el('path', {
    d: `M ${x0.toFixed(2)} ${y0.toFixed(2)} A ${r} ${r} 0 1 1 ${x1.toFixed(2)} ${y1.toFixed(2)}`,
    fill: 'none', stroke: color, 'stroke-width': 1.6,
  });
  const head = el('polygon', {
    points: `${x1 - 3},${y1 - 1} ${x1 + 1},${y1 - 3} ${x1 + 1},${y1 + 3}`, fill: color,
  });
  return el('g', {}, [arc, head]);
}

export function badgeChosen(cx, cy, color = PHASE.chosen) {
  // filled diamond
  const p = `${cx},${cy - 5} ${cx + 5},${cy} ${cx},${cy + 5} ${cx - 5},${cy}`;
  return el('g', {}, [el('polygon', { points: p, fill: color })]);
}

export function badgeDrop(cx, cy, color = PHASE.nack) {
  // an X of two strokes
  const s = 4;
  return el('g', { stroke: color, 'stroke-width': 1.6, 'stroke-linecap': 'round' }, [
    el('line', { x1: cx - s, y1: cy - s, x2: cx + s, y2: cy + s }),
    el('line', { x1: cx + s, y1: cy - s, x2: cx - s, y2: cy + s }),
  ]);
}

export function badgeDisk(cx, cy, color = C.muted) {
  // a small disk cylinder: top ellipse + body
  const rx = 5, ry = 2, h = 7;
  return el('g', { stroke: color, 'stroke-width': 1.3, fill: 'none' }, [
    el('ellipse', { cx, cy: cy - h / 2, rx, ry }),
    el('path', { d: `M ${cx - rx} ${cy - h / 2} V ${cy + h / 2} A ${rx} ${ry} 0 0 0 ${cx + rx} ${cy + h / 2} V ${cy - h / 2}` }),
  ]);
}

export function badgeRot(cx, cy, color = PHASE.nack) {
  // a warning triangle with an exclamation stroke — silent corruption injected
  const s = 6;
  return el('g', {}, [
    el('polygon', {
      points: `${cx},${cy - s} ${cx + s},${cy + s * 0.8} ${cx - s},${cy + s * 0.8}`,
      fill: 'none', stroke: color, 'stroke-width': 1.5, 'stroke-linejoin': 'round',
    }),
    el('line', { x1: cx, y1: cy - s * 0.45, x2: cx, y2: cy + s * 0.25, stroke: color, 'stroke-width': 1.6, 'stroke-linecap': 'round' }),
    el('circle', { cx, cy: cy + s * 0.55, r: 0.9, fill: color }),
  ]);
}

export function badgeDetect(cx, cy, color = PHASE.promise) {
  // a magnifier: detection — the checksum caught the rot on read
  const r = 3.6;
  return el('g', { stroke: color, 'stroke-width': 1.5, fill: 'none', 'stroke-linecap': 'round' }, [
    el('circle', { cx: cx - 1, cy: cy - 1, r }),
    el('line', { x1: cx + 1.8, y1: cy + 1.8, x2: cx + 4.6, y2: cy + 4.6 }),
  ]);
}

export function badgeHeal(cx, cy, color = PHASE.chosen) {
  // a check mark — the record was repaired from a correct peer copy
  return el('g', { stroke: color, 'stroke-width': 2, fill: 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round' }, [
    el('path', { d: `M ${cx - 4.5} ${cy} L ${cx - 1} ${cy + 3.5} L ${cx + 5} ${cy - 3.5}` }),
  ]);
}

export function badgeWait(cx, cy, color = PHASE.accept) {
  // two pause bars — the node waits rather than fabricate or truncate
  return el('g', { fill: color }, [
    el('rect', { x: cx - 4, y: cy - 5, width: 2.6, height: 10, rx: 1 }),
    el('rect', { x: cx + 1.4, y: cy - 5, width: 2.6, height: 10, rx: 1 }),
  ]);
}

export function badgeSnapshot(cx, cy, color = PHASE.promise) {
  // a box with a down arrow into it — an installed application snapshot
  return el('g', {}, [
    el('path', { d: `M ${cx - 5} ${cy} V ${cy + 5} H ${cx + 5} V ${cy}`, fill: 'none', stroke: color, 'stroke-width': 1.5, 'stroke-linejoin': 'round' }),
    el('line', { x1: cx, y1: cy - 6, x2: cx, y2: cy + 1.5, stroke: color, 'stroke-width': 1.6, 'stroke-linecap': 'round' }),
    el('polygon', { points: `${cx - 2.6},${cy - 0.5} ${cx + 2.6},${cy - 0.5} ${cx},${cy + 2.8}`, fill: color }),
  ]);
}

export function badgeLeader(cx, cy, color = PHASE.leader) {
  // gold ring + a small gold dot badge
  return el('g', {}, [
    el('circle', { cx, cy, r: 6, fill: 'none', stroke: color, 'stroke-width': 1.6 }),
    el('circle', { cx: cx + 5, cy: cy - 5, r: 2.2, fill: color }),
  ]);
}
