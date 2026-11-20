import { copyFile, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import { join } from "node:path"
import { fileURLToPath } from "node:url"
import tailwindcss from "@tailwindcss/vite"
import { build } from "vite"
import { expect, it } from "vitest"

it("只从入口 HTML 和产品源码生成 Tailwind 样式", async () => {
  const root = fileURLToPath(new URL("../", import.meta.url))
  const fixture = await mkdtemp(join(root, "tailwind-sources-test-"))
  try {
    const src = join(fixture, "src")
    await mkdir(src)
    await copyFile(join(root, "src/index.css"), join(src, "index.css"))
    await Promise.all([
      writeFile(join(fixture, "index.html"), '<main class="z-[31001]"></main>'),
      writeFile(join(src, "view.tsx"), '<div className="z-[31002]" />'),
      writeFile(join(src, "classes.ts"), 'export const layer = "z-[31003]"'),
      writeFile(join(fixture, "notes.md"), "z-[31004]"),
      writeFile(join(src, "view.test.tsx"), 'const fixture = "z-[31005]"'),
      writeFile(join(src, "view.bench.ts"), 'const fixture = "z-[31006]"'),
    ])

    const bundle = await build({
      root: fixture,
      configFile: false,
      logLevel: "silent",
      plugins: [tailwindcss()],
      build: {
        write: false,
        cssMinify: false,
        rollupOptions: { input: join(src, "index.css") },
      },
    })
    const css = bundle.output.find((file) => file.fileName.endsWith(".css"))
    const layers = Array.from(
      css.source.matchAll(/z-index:\s*(3100[1-6])\b/g),
      (match) => match[1]
    )
    expect(layers.sort()).toEqual(["31001", "31002", "31003"])
  } finally {
    await rm(fixture, { recursive: true, force: true })
  }
})
