import { readFileSync } from "node:fs"
import { dirname, resolve } from "node:path"
import { fileURLToPath, pathToFileURL } from "node:url"

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..")
const read = (path) => readFileSync(resolve(root, path), "utf8")
const readJson = (path) => JSON.parse(read(path))

const semverPattern =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/

export const isValidSemver = (version) => semverPattern.test(version)

export function parseArgs(args) {
  let tag

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index]

    if (argument === "--tag") {
      if (tag !== undefined) {
        throw new Error("--tag 不能重复提供")
      }

      const value = args[index + 1]
      if (!value || value.startsWith("-")) {
        throw new Error("--tag 后必须提供标签")
      }

      tag = value
      index += 1
    } else if (argument.startsWith("--tag=")) {
      if (tag !== undefined) {
        throw new Error("--tag 不能重复提供")
      }

      tag = argument.slice("--tag=".length)
      if (!tag) {
        throw new Error("--tag 后必须提供标签")
      }
    } else if (argument.startsWith("-")) {
      throw new Error(`未知参数：${argument}`)
    } else {
      throw new Error(`不允许多余的位置参数：${argument}`)
    }
  }

  return { tag }
}

export function readCargoPackageVersion(toml) {
  const packageSection = toml.match(
    /^\[package\]\s*$([\s\S]*?)(?=^\[|(?![\s\S]))/m
  )?.[1]
  return packageSection?.match(/^version\s*=\s*"([^"]+)"\s*$/m)?.[1]
}

export function readCargoLockPackageVersion(lockfile, packageName) {
  const matchingPackages = lockfile
    .split(/^\[\[package\]\]\s*$/m)
    .slice(1)
    .filter((section) => {
      const name = section.match(/^name\s*=\s*"([^"]+)"\s*$/m)?.[1]
      return name === packageName && !/^source\s*=/m.test(section)
    })

  if (matchingPackages.length !== 1) return undefined
  return matchingPackages[0].match(/^version\s*=\s*"([^"]+)"\s*$/m)?.[1]
}

export function checkVersions({ versions, expected, tag }) {
  if (!isValidSemver(expected)) {
    throw new Error(`package.json 中的版本不是有效的 SemVer：${expected}`)
  }

  const mismatches = Object.entries(versions).filter(
    ([, version]) => version !== expected
  )
  if (mismatches.length > 0) {
    const details = mismatches
      .map(([file, version]) => `- ${file}: ${version ?? "<missing>"}`)
      .join("\n")
    throw new Error(`版本不一致，期望 ${expected}：\n${details}`)
  }

  if (tag && tag !== `v${expected}`) {
    throw new Error(`发布标签 ${tag} 与项目版本 v${expected} 不一致`)
  }
}

function main() {
  const packageJson = readJson("package.json")
  const cargoToml = read("src-tauri/Cargo.toml")
  const cargoPackage = readCargoPackageVersion(cargoToml)

  if (!cargoPackage) {
    throw new Error("无法读取 src-tauri/Cargo.toml 的 [package] version")
  }

  const { tag: argumentTag } = parseArgs(process.argv.slice(2))
  const tag =
    argumentTag ??
    (process.env.GITHUB_REF_TYPE === "tag"
      ? process.env.GITHUB_REF_NAME
      : undefined)
  const expected = packageJson.version
  const versions = {
    "package.json": expected,
    "src-tauri/Cargo.toml": cargoPackage,
    "src-tauri/Cargo.lock (root package)": readCargoLockPackageVersion(
      read("src-tauri/Cargo.lock"),
      packageJson.name
    ),
    "src-tauri/tauri.conf.json": readJson("src-tauri/tauri.conf.json").version,
  }

  checkVersions({
    versions,
    expected,
    tag,
  })
  console.log(
    tag
      ? `版本检查通过：${expected}（标签 ${tag}）`
      : `版本检查通过：${expected}`
  )
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main()
}
