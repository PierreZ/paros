// Copy the shared design system (website/shared/: tokens, components, theme,
// fonts, logo) into public/shared/, so every page of the site and the
// playground load the same files. public/shared/ is a build output (ignored).
import { cpSync, rmSync } from "node:fs";
import { fileURLToPath } from "node:url";

const from = fileURLToPath(new URL("../../shared/", import.meta.url));
const to = fileURLToPath(new URL("../public/shared/", import.meta.url));
rmSync(to, { recursive: true, force: true });
cpSync(from, to, { recursive: true, dereference: true });
