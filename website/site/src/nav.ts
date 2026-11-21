// The sections of the site, in the order of the top bar.
export const NAV = [
  { name: "Paxos", path: "paxos/" },
  { name: "Consensus", path: "consensus/" },
  { name: "Journal", path: "journal/" },
  { name: "parosd", path: "parosd/" },
  { name: "Engineering", path: "engineering/" },
] as const;

export const REPO = "https://github.com/PierreZ/paros";

/** A link inside the site, with the base path in front. */
export function url(path = ""): string {
  const base = import.meta.env.BASE_URL.replace(/\/?$/, "/");
  return base + path.replace(/^\//, "");
}
