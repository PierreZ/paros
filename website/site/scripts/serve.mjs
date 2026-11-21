// Serve the built site (dist/) at /paros/, as GitHub Pages does, for the
// browser smoke test (tests/, playwright.config.mjs). A path that names a
// folder serves its index.html; a folder without the trailing slash
// redirects to it. No other behaviour: a missing file is a 404, which the
// smoke test counts as a failed request.
//
//   node scripts/serve.mjs [port]     default port 4322
import { createReadStream, statSync } from "node:fs";
import { createServer } from "node:http";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";

const DIST = fileURLToPath(new URL("../dist/", import.meta.url));
const BASE = "/paros/";
const PORT = Number(process.argv[2] || 4322);
const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json",
  ".wasm": "application/wasm",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
  ".woff2": "font/woff2",
  ".woff": "font/woff",
  ".txt": "text/plain; charset=utf-8",
  ".xml": "application/xml",
};

function isFile(path) {
  try { return statSync(path).isFile(); } catch { return false; }
}
function isDir(path) {
  try { return statSync(path).isDirectory(); } catch { return false; }
}

createServer((req, res) => {
  const url = new URL(req.url, "http://localhost");
  let path = decodeURIComponent(url.pathname);
  if (path === "/paros") { res.writeHead(301, { location: BASE }); res.end(); return; }
  if (!path.startsWith(BASE)) { res.writeHead(404); res.end("not under " + BASE); return; }
  let file = normalize(join(DIST, path.slice(BASE.length)));
  if (!file.startsWith(DIST)) { res.writeHead(403); res.end(); return; }
  if (isDir(file)) {
    if (!path.endsWith("/")) { res.writeHead(301, { location: path + "/" + url.search }); res.end(); return; }
    file = join(file, "index.html");
  }
  if (!isFile(file)) {
    res.writeHead(404, { "content-type": TYPES[".html"] });
    createReadStream(join(DIST, "404.html")).pipe(res);
    return;
  }
  res.writeHead(200, { "content-type": TYPES[extname(file)] || "application/octet-stream" });
  createReadStream(file).pipe(res);
}).listen(PORT, "127.0.0.1", () => {
  console.log(`dist/ served at http://127.0.0.1:${PORT}${BASE}`);
});
