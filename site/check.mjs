import { readFile, stat } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const siteRoot = dirname(fileURLToPath(import.meta.url));
const sourceFiles = ["index.html", "styles.css", "script.js"];
const source = (
  await Promise.all(sourceFiles.map((file) => readFile(join(siteRoot, file), "utf8")))
).join("\n");

const checks = [
  [!source.includes("—") && !source.includes("–"), "visible copy uses regular hyphens"],
  [!source.includes('href="#"'), "all links have real destinations"],
  [source.includes('name="description"'), "description metadata is present"],
  [source.includes('property="og:image"'), "Open Graph image metadata is present"],
  [source.includes("prefers-reduced-motion"), "reduced motion is supported"],
  [source.includes("prefers-color-scheme: dark"), "dark mode is supported"],
  [source.includes("aria-pressed"), "package selectors expose state"],
  [source.includes("aria-expanded"), "mobile navigation exposes state"],
];

const failed = checks.filter(([passed]) => !passed).map(([, message]) => message);
if (failed.length > 0) {
  throw new Error(`site preflight failed: ${failed.join(", ")}`);
}

const hero = await stat(join(siteRoot, "assets", "rutomq-data-path.webp"));
if (hero.size > 200_000) {
  throw new Error(`hero image is too large: ${hero.size} bytes`);
}

console.log(`site preflight passed (${checks.length} checks, ${hero.size} byte hero)`);
