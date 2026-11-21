// The link check of the built site (#1362 (browser smoke test and link
// check)). Run after `npm run build`: `npm run check-links`.
//
// It reads every HTML file of dist/ and checks each `href` and `src`:
//   - a link inside the site (under /paros/, or relative) names a file of
//     dist/, and its `#anchor` names an `id` of the target page;
//   - a link to this repository on GitHub (`blob/<ref>/<path>` or
//     `tree/<ref>/<path>`) names a file or folder that exists at that ref:
//     the working tree for `main` (so a pull request is checked against its
//     own tree), `git ls-tree` for any other ref (the tag v0.2.0-alpha.1;
//     CI fetches it). Its `#anchor` on a Markdown file names a heading of
//     that file, with the anchor GitHub makes for it (`§J4.2 The journal's
//     sans-IO contract` is `#j42-the-journals-sans-io-contract`).
// Links to other sites are not fetched: the check runs with no network.
// Every broken link fails the check. There is no list of links to skip.
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const SITE = fileURLToPath(new URL("..", import.meta.url));
const DIST = join(SITE, "dist");
const ROOT = join(SITE, "..", "..");
const BASE = "/paros/";
const ORIGIN = "https://pierrez.github.io";
const REPO = /^https:\/\/github\.com\/PierreZ\/paros\/(blob|tree)\/([^/#?]+)(?:\/([^#?]*))?(?:#(.*))?$/;

if (!existsSync(DIST)) {
  console.error("dist/ is missing: run `npm run build` first.");
  process.exit(1);
}

function walk(dir) {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    return statSync(path).isDirectory() ? walk(path) : [path];
  });
}

function decode(text) {
  return text
    .replace(/&amp;/g, "&").replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&#x27;/g, "'");
}

// The ids of a page, and the links it holds (without the text of <script>
// and <style>, which hold code, not links).
const pages = new Map();
function page(file) {
  if (pages.has(file)) return pages.get(file);
  const html = readFileSync(file, "utf8");
  const ids = new Set([...html.matchAll(/\s(?:id|name)="([^"]*)"/g)].map((m) => decode(m[1])));
  const body = html.replace(/<script\b[^>]*>[\s\S]*?<\/script>/g, (tag) => tag.replace(/>[\s\S]*<\/script>$/, "></script>"))
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/g, "");
  const links = [...body.matchAll(/\s(href|src)="([^"]*)"/g)].map((m) => decode(m[2]));
  const refresh = /<meta http-equiv="refresh" content="\d+;url=([^"]*)"/.exec(html);
  if (refresh) links.push(decode(refresh[1]));
  const p = { ids, links };
  pages.set(file, p);
  return p;
}

// The file of dist/ that serves `path` (a path under /paros/), or null.
function served(path) {
  const rel = decodeURIComponent(path.slice(BASE.length));
  const file = join(DIST, rel);
  if (!file.startsWith(DIST)) return null;
  if (existsSync(file) && statSync(file).isFile()) return file;
  const index = join(file, "index.html");
  if (existsSync(index)) return index;
  return null;
}

// GitHub's anchor of a Markdown heading: lower case, drop every character
// but letters, digits, marks, `_`, `-` and spaces, then each space is a
// `-`. A heading met again gets `-1`, `-2`, ...
function slug(text) {
  return text.toLowerCase().replace(/[^\p{L}\p{N}\p{M}\p{Pc}\- ]/gu, "").replace(/ /g, "-");
}
function headingText(line) {
  return line
    .replace(/^#{1,6}\s+/, "").replace(/\s+#+\s*$/, "")
    .replace(/!?\[([^\]]*)\]\([^)]*\)/g, "$1")
    .replace(/<[^>]+>/g, "")
    .replace(/[`*]/g, "")
    .replace(/(^|\W)_+|_+(\W|$)/g, "$1$2")
    .trim();
}
function anchorsOf(markdown) {
  const out = new Set();
  const seen = new Map();
  let fence = false;
  for (const line of markdown.split("\n")) {
    if (/^\s*(```|~~~)/.test(line)) { fence = !fence; continue; }
    if (fence) continue;
    for (const m of line.matchAll(/<a\s+(?:id|name)="([^"]+)"/g)) out.add(m[1]);
    if (!/^#{1,6}\s/.test(line)) continue;
    const base = slug(headingText(line));
    const n = seen.get(base) || 0;
    seen.set(base, n + 1);
    out.add(n ? `${base}-${n}` : base);
  }
  return out;
}

// What exists at a git ref: the working tree for `main`, else `git ls-tree`.
const trees = new Map();
const missingRefs = new Set();
function at(ref) {
  if (ref === "main") {
    return {
      kind(path) {
        const full = join(ROOT, path);
        if (!existsSync(full)) return null;
        return statSync(full).isDirectory() ? "tree" : "blob";
      },
      read: (path) => readFileSync(join(ROOT, path), "utf8"),
    };
  }
  if (!trees.has(ref)) {
    let listing;
    try {
      listing = execFileSync("git", ["-C", ROOT, "ls-tree", "-r", "-t", ref], { encoding: "utf8", maxBuffer: 1 << 28, stdio: ["ignore", "pipe", "ignore"] });
    } catch {
      // Not in this clone: every link to it is broken. CI fetches each ref
      // that the site names (pages.yml), so a ref missing there is missing
      // on GitHub too.
      listing = "";
      missingRefs.add(ref);
    }
    const kinds = new Map(listing ? [["", "tree"]] : []);
    for (const line of listing.split("\n")) {
      const m = /^\d+ (blob|tree) \w+\t(.*)$/.exec(line);
      if (m) kinds.set(m[2], m[1]);
    }
    trees.set(ref, {
      kind: (path) => kinds.get(path) || null,
      read: (path) => execFileSync("git", ["-C", ROOT, "show", `${ref}:${path}`], { encoding: "utf8" }),
    });
  }
  return trees.get(ref);
}

const broken = [];
let checked = 0;
const files = walk(DIST).filter((f) => f.endsWith(".html"));
for (const file of files) {
  const from = "/" + relative(DIST, file).replace(/index\.html$/, "");
  const here = BASE + relative(DIST, file);
  for (const link of page(file).links) {
    if (!link || /^(mailto:|data:|javascript:)/.test(link)) continue;
    const bad = (why) => broken.push(`${from}: ${link}  (${why})`);
    checked++;
    const repo = REPO.exec(link);
    if (repo) {
      const [, kind, ref, rawPath, anchor] = repo;
      const path = decodeURIComponent(rawPath || "").replace(/\/$/, "");
      const tree = at(ref);
      const found = tree.kind(path);
      if (!found) { bad(missingRefs.has(ref) ? `no ref ${ref} in this clone` : `no ${path} at ${ref}`); continue; }
      if (kind === "blob" && found !== "blob") { bad(`${path} is a folder: use tree/`); continue; }
      if (anchor && /\.md$/.test(path) && !/^L\d+/.test(anchor)) {
        if (!anchorsOf(tree.read(path)).has(decodeURIComponent(anchor))) bad(`no heading #${anchor} in ${path}`);
      }
      continue;
    }
    let url;
    try { url = new URL(link, ORIGIN + here); } catch { bad("not a URL"); continue; }
    if (url.origin !== ORIGIN) continue;
    if (!url.pathname.startsWith(BASE)) { bad(`outside ${BASE}`); continue; }
    const target = served(url.pathname);
    if (!target) { bad("no such page or file"); continue; }
    if (url.hash && url.hash !== "#" && target.endsWith(".html")) {
      const id = decodeURIComponent(url.hash.slice(1));
      // `#run=…`, `#lab=…`: an address a script reads, not an element.
      if (!/^\w+=/.test(id) && !page(target).ids.has(id)) bad(`no id "${id}" on the page`);
    }
  }
}

if (broken.length) {
  console.error(`${broken.length} broken link(s) of ${checked}:`);
  for (const b of broken) console.error("  " + b);
  process.exit(1);
}
console.log(`${checked} links in ${files.length} pages: none broken.`);
