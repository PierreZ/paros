// tokens.js — the paros demo design system, in one place.
//
// A minimalist dark "instrument": calm, generous spacing, hairline dividers,
// neutral nodes, and one bold element — a single glowing, phase-coloured message
// particle. Colour is not decoration: each phase colour encodes a protocol phase,
// and only the *active* phase is ever coloured. Everything the renderers and the
// shell draw pulls its palette, type, spacing and radii from here, so the look
// stays coherent and a restyle touches exactly one file.

export const C = {
  // neutral slate surfaces
  canvas: '#0A0C10',
  panel: '#0F131A',
  stage: '#0B0E14',
  inset: '#0E121A',
  hairline: '#222835',
  softline: '#1A2029',
  // text
  text: '#E7EBF2',
  muted: '#7E8798',
  faint: '#49515F',
  dim: '#343C4A',
  // nodes
  nodeFill: '#0E131C',
  neutralRing: '#5B6472',
  white: '#FFFFFF',
};

// Phase colours — the brand. Keyed so a renderer can look up by protocol `kind`
// or by phase name interchangeably.
export const PHASE = {
  prepare: '#5B93FF',
  promise: '#34C6F4',
  accept: '#F5A524',
  accepted: '#C97A12',
  chosen: '#35D07F',
  nack: '#F0544F',
  leader: '#E9B949',
  // synthetic / neutral steps
  propose: '#8FA0BE',
  commit: '#35D07F',
  idle: C.neutralRing,
};

// Human labels for each phase, for the phase track and narration titles.
export const PHASE_LABEL = {
  prepare: 'Prepare',
  promise: 'Promise',
  accept: 'Accept',
  accepted: 'Accepted',
  chosen: 'Chosen',
  nack: 'Nack',
  leader: 'Leader',
  propose: 'Propose',
  commit: 'Commit',
};

// The single-decree phase track: five evenly-spaced stops, grouped into the two
// round-trips of Paxos. `phase` matches a Frame's phase so the shell can light
// the active stop.
export const PHASE_TRACK = [
  { phase: 'prepare', label: 'Prepare', group: 1 },
  { phase: 'promise', label: 'Promise', group: 1 },
  { phase: 'accept', label: 'Accept', group: 2 },
  { phase: 'accepted', label: 'Accepted', group: 2 },
  { phase: 'chosen', label: 'Chosen', group: 2 },
];

// A small, quiet categorical palette for hashing opaque values (the demo only
// ever sees a u64 `vhash`, never the bytes). Muted so a filled log column reads
// as "same colour = agreement" without shouting. Same colour system in single
// (accepted-value swatch) and multi (log cells).
export const VALUE_COLORS = [
  '#6E8BD8', // slate blue
  '#3FA9A0', // teal
  '#C9974A', // amber
  '#57A66B', // sage green
  '#A87BC4', // muted violet
  '#C77B84', // dusty rose
  '#7F9BB0', // steel
  '#B0A24E', // olive
];

// Map an opaque value hash to one of the quiet categorical colours. Deterministic
// (same vhash → same colour everywhere), so agreement is visible across nodes.
export function valueColor(vhash) {
  const n = Number(vhash);
  const i = ((n % VALUE_COLORS.length) + VALUE_COLORS.length) % VALUE_COLORS.length;
  return VALUE_COLORS[i];
}

// Spacing scale (4px base) and radii — used by the CSS-in-JS the shell writes and
// by SVG geometry so padding is consistent everywhere.
export const SP = { xs: 4, sm: 8, md: 12, lg: 16, xl: 24, xxl: 32 };
export const RADIUS = { panel: 20, stage: 13, card: 14, cell: 3 };
export const HAIRLINE = 1;

// Type — two families, two weights. Hierarchy comes from size / spacing / colour,
// never extra weights. IBM Plex if the fonts stage; graceful system fallback.
export const FONT_SANS =
  "'IBM Plex Sans', system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif";
export const FONT_MONO =
  "'IBM Plex Mono', ui-monospace, 'SF Mono', 'JetBrains Mono', Menlo, Consolas, monospace";

// Colour for a protocol `kind` (falls back to accept-amber for unknowns).
export function kindColor(kind) {
  return PHASE[kind] || PHASE.accept;
}
