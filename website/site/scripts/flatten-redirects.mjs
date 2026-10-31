// An old mdbook URL ends in `.html` (`/safety.html`). Astro writes its
// redirect as `safety.html/index.html`; move it to the file `safety.html` so
// GitHub Pages serves it at the old address.
import { readdirSync, renameSync, rmdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const dist = fileURLToPath(new URL("../dist/", import.meta.url));
for (const name of readdirSync(dist)) {
  const dir = join(dist, name);
  if (!name.endsWith(".html") || !statSync(dir).isDirectory()) continue;
  renameSync(join(dir, "index.html"), dir + ".tmp");
  rmdirSync(dir);
  renameSync(dir + ".tmp", dir);
}
