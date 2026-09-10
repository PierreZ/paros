// URL routing: the level map, or one level.
//
// A level id is a path (`act1/choose-a-value`), so the hash is the id itself:
// `#act1/choose-a-value`. No hash — or a bare `#` — is the level map. A leading
// slash is tolerated because a hand-typed `#/act1/…` is the obvious mistake.

/** Where the URL points. */
export type Route = { kind: 'map' } | { kind: 'level'; id: string };

/** Read a route out of a `location.hash` value. */
export function parseHash(hash: string): Route {
  let raw = hash.startsWith('#') ? hash.slice(1) : hash;
  try {
    raw = decodeURIComponent(raw);
  } catch {
    // A malformed escape is not a level id either way.
  }
  raw = raw.trim();
  while (raw.startsWith('/')) raw = raw.slice(1);
  while (raw.endsWith('/')) raw = raw.slice(0, -1);
  if (raw === '') return { kind: 'map' };
  return { kind: 'level', id: raw };
}

/** The hash a route is reached by. */
export function hashFor(route: Route): string {
  return route.kind === 'map' ? '#' : `#${route.id}`;
}

/** Whether two routes point at the same thing. */
export function sameRoute(a: Route, b: Route): boolean {
  if (a.kind !== b.kind) return false;
  return a.kind !== 'level' || b.kind !== 'level' || a.id === b.id;
}
