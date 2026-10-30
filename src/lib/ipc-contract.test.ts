import { describe, expect, it } from "vitest"
import ts from "typescript"

import { commandInventory } from "@/lib/ipc"
import backendSource from "../../src-tauri/src/lib.rs?raw"
import ipcSource from "./ipc.ts?raw"

const commandSources = Object.values(
  import.meta.glob("../../src-tauri/src/commands/*.rs", {
    query: "?raw",
    import: "default",
    eager: true,
  })
).join("\n")
const ipcAst = ts.createSourceFile(
  "ipc.ts",
  ipcSource,
  ts.ScriptTarget.Latest,
  true
)
const commandMap = ipcAst.statements.find(
  (node): node is ts.TypeAliasDeclaration =>
    ts.isTypeAliasDeclaration(node) && node.name.text === "CommandMap"
)!
const frontendArguments = new Map(
  (commandMap.type as ts.TypeLiteralNode).members.map((node) => {
    const member = node as ts.PropertySignature
    const args = (member.type as ts.TypeReferenceNode).typeArguments![0]!
    return [
      member.name.getText(ipcAst),
      ts.isTypeLiteralNode(args)
        ? args.members
            .map((arg) => (arg as ts.PropertySignature).name.getText(ipcAst))
            .sort()
        : [],
    ] as const
  })
)

function backendArguments(command: string) {
  const signature = commandSources.match(
    new RegExp(`\\bfn ${command}\\s*\\(([\\s\\S]*?)\\)\\s*->`)
  )?.[1]
  if (signature === undefined) throw new Error(`Missing command: ${command}`)
  const parameters: string[] = []
  let depth = 0
  let start = 0
  for (let index = 0; index <= signature.length; index++) {
    const char = signature[index]
    if (char === "<" || char === "(") depth++
    if (char === ">" || char === ")") depth--
    if (index === signature.length || (char === "," && depth === 0)) {
      const parameter = signature.slice(start, index).trim()
      if (parameter) parameters.push(parameter)
      start = index + 1
    }
  }
  return parameters
    .flatMap((parameter) => {
      const [, name, type] = parameter.match(/^(\w+)\s*:\s*([\s\S]+)$/)!
      return type!.startsWith("State<") || type === "tauri::AppHandle"
        ? []
        : [name!.replace(/_([a-z])/g, (_, char: string) => char.toUpperCase())]
    })
    .sort()
}

describe("IPC command contract", () => {
  it.each(commandInventory)(
    "matches the frontend argument names for %s",
    (command) => {
      expect(backendArguments(command)).toEqual(frontendArguments.get(command))
    }
  )
  it("keeps frontend commands aligned with registered Tauri handlers", () => {
    const handler = backendSource.match(
      /\.invoke_handler\(tauri::generate_handler!\[([\s\S]*?)\]\)/
    )?.[1]
    expect(handler).toBeDefined()

    const registered = [...handler!.matchAll(/commands::\w+::(\w+)/g)].map(
      ([, name]) => name
    )

    expect(new Set(registered).size).toBe(registered.length)
    expect(new Set(commandInventory).size).toBe(commandInventory.length)
    expect([...commandInventory].sort()).toEqual(registered.sort())
  })
})
