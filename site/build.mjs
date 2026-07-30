import { cp, mkdir, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const siteRoot = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(siteRoot, "..");
const outputRoot = join(siteRoot, "dist");

await rm(outputRoot, { recursive: true, force: true });
await mkdir(join(outputRoot, "assets"), { recursive: true });

for (const file of ["index.html", "styles.css", "script.js"]) {
  await cp(join(siteRoot, file), join(outputRoot, file));
}

await cp(join(siteRoot, "assets"), join(outputRoot, "assets"), {
  recursive: true,
});
await cp(
  join(repoRoot, "docs", "assets", "rutomq-mark.svg"),
  join(outputRoot, "assets", "rutomq-mark.svg"),
);
await cp(
  join(siteRoot, "node_modules", "@primer", "css", "dist", "primer.css"),
  join(outputRoot, "primer.css"),
);
await writeFile(join(outputRoot, ".nojekyll"), "");
