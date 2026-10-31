// Page options and replay links.
//
// A run of the game is a function of two things: the level (its id is the
// config) and the action log. The engine draws no randomness, so it has no
// seed, and the same log on the same level always gives the same world. The
// log travels in the query string as base64url JSON, so a link replays a run
// exactly:
//
//   play/?log=<base64url JSON of Action[]>#act2/elect-a-leader
//
// `embed` mounts one level alone, for a scenario on a site page (the
// `scenario` shortcode). `theme` (`light` or `dark`) follows the page that
// holds the frame; without it, the reader's system setting is used.

import type { Action } from './types';

/** The theme a page asks for. */
export type Theme = 'light' | 'dark';

/** What the query string asks for. */
export interface PageOptions {
  /** Mount one level alone, with no level map and no progress. */
  embed: boolean;
  /** The actions to replay when the level opens. */
  log: Action[] | null;
  /** The theme the holding page uses, if it said. */
  theme: Theme | null;
}

/** Encode an action log as base64url JSON. */
export function encodeLog(actions: readonly Action[]): string {
  const bytes = new TextEncoder().encode(JSON.stringify(actions));
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

/** Decode an action log. A value that does not decode gives `null`. */
export function decodeLog(encoded: string): Action[] | null {
  try {
    const base64 = encoded.replace(/-/g, '+').replace(/_/g, '/');
    const padded = base64 + '='.repeat((4 - (base64.length % 4)) % 4);
    const binary = atob(padded);
    const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
    const parsed: unknown = JSON.parse(new TextDecoder().decode(bytes));
    if (!Array.isArray(parsed)) return null;
    const ok = parsed.every(
      (entry: unknown) =>
        typeof entry === 'object' && entry !== null && typeof (entry as { kind?: unknown }).kind === 'string',
    );
    return ok ? (parsed as Action[]) : null;
  } catch {
    return null;
  }
}

/** Read the page options out of a `location.search` value. */
export function parseOptions(search: string): PageOptions {
  const params = new URLSearchParams(search);
  const rawTheme = params.get('theme');
  const rawLog = params.get('log');
  return {
    embed: params.has('embed'),
    log: rawLog ? decodeLog(rawLog) : null,
    theme: rawTheme === 'light' || rawTheme === 'dark' ? rawTheme : null,
  };
}

/**
 * The link that replays a run in the full game.
 *
 * `base` is the game's own page (`location.pathname` of the frame), so the
 * link is right on the deployed site and on the dev server.
 */
export function runLink(base: string, levelId: string, actions: readonly Action[]): string {
  const query = actions.length > 0 ? `?log=${encodeLog(actions)}` : '';
  return `${base}${query}#${levelId}`;
}
