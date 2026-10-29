/// <reference types="node" />

import { createContext, runInContext } from "node:vm"
import { afterEach, describe, expect, it } from "vitest"

import bridgeSource from "../../src-tauri/src/session_bridge.js?raw"

type RpcCall = { method: string; params: Record<string, unknown> }
type NativeThread = {
  id: string
  name: string
  status: { type: string }
  parentThreadId?: string
  archived?: boolean
}
type MutationOutcome = { dispatched: boolean; error?: string; value?: unknown }
type Bridge = {
  version: number
  dispose?: () => void
  list(): Promise<Array<{ id: string; archived: boolean }>>
  affectedScope(id: string): Promise<{
    threads: Array<{ id: string }>
    blockedReason: string | null
  }>
  guardAndMutate(
    method: string,
    id: string,
    affected: string[]
  ): Promise<MutationOutcome>
}
type Listener = (event: { data: unknown }) => void

const cleanups: Array<() => void> = []
afterEach(() => {
  for (const cleanup of cleanups.splice(0)) cleanup()
})

function thread(id: string, state = "idle", parent?: string): NativeThread {
  return { id, name: id, status: { type: state }, parentThreadId: parent }
}

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

function harness(
  threads = [thread("root")],
  handle?: (call: RpcCall) => unknown | Promise<unknown>
) {
  const calls: RpcCall[] = []
  const events: Array<Record<string, unknown>> = []
  const listeners = new Set<Listener>()
  const timers = new Set<ReturnType<typeof setTimeout>>()
  let sequence = 0
  const emit = (data: unknown) => {
    for (const listener of [...listeners]) listener({ data })
  }
  const defaultResponse = ({ method, params }: RpcCall) => {
    if (method === "thread/list") {
      const spawnedOnly =
        (params.sourceKinds as string[]).length === 1 &&
        (params.sourceKinds as string[])[0] === "subAgentThreadSpawn"
      return {
        data: threads.filter(
          (item) =>
            !!item.archived === params.archived &&
            (!spawnedOnly || !!item.parentThreadId)
        ),
        nextCursor: null,
      }
    }
    if (method === "thread/loaded/list") {
      return { data: threads.map((item) => item.id) }
    }
    if (method === "thread/read") {
      return { thread: threads.find((item) => item.id === params.threadId) }
    }
    return {}
  }
  const window = {
    addEventListener: (_type: string, listener: Listener) =>
      listeners.add(listener),
    removeEventListener: (_type: string, listener: Listener) =>
      listeners.delete(listener),
    __codexToolsSessionEvent: (payload: string) => {
      events.push(JSON.parse(payload) as Record<string, unknown>)
    },
    electronBridge: {
      sendMessageFromView: (message: {
        hostId: string
        request: { id: string; method: string; params: Record<string, unknown> }
      }) => {
        expect(message.hostId).toBe("local")
        const { id, ...call } = message.request
        calls.push(call)
        Promise.resolve()
          .then(() => handle?.(call) ?? defaultResponse(call))
          .then(
            (result) => emit({ type: "mcp-response", message: { id, result } }),
            (error: Error) =>
              emit({
                type: "mcp-response",
                message: { id, error: { message: error.message } },
              })
          )
      },
    },
  }
  const context = createContext({
    window,
    crypto: { randomUUID: () => `request-${++sequence}` },
    setTimeout: (callback: () => void, delay: number) => {
      const timer = setTimeout(callback, delay)
      timers.add(timer)
      return timer
    },
    clearTimeout: (timer: ReturnType<typeof setTimeout>) => {
      clearTimeout(timer)
      timers.delete(timer)
    },
  })
  const install = () => runInContext(bridgeSource, context) as boolean
  expect(install()).toBe(true)
  cleanups.push(() => {
    for (const timer of timers) clearTimeout(timer)
    listeners.clear()
  })
  return {
    api: (window as typeof window & { __codexToolsSessionsV1: Bridge })
      .__codexToolsSessionsV1,
    calls,
    events,
    listeners,
    install,
    notify: (
      method: string,
      params: Record<string, unknown>,
      hostId = "local"
    ) => emit({ type: "mcp-notification", hostId, method, params }),
  }
}

const writes = (calls: RpcCall[]) =>
  calls.filter((call) =>
    ["thread/archive", "thread/unarchive", "thread/delete"].includes(
      call.method
    )
  )

describe("desktop session bridge script", () => {
  it.each(["idle", "notLoaded"])(
    "allows a %s root using metadata reads without resuming or loading turns",
    async (state) => {
      const bridge = harness([thread("root", state)])
      const result = await bridge.api.guardAndMutate("thread/archive", "root", [
        "root",
      ])
      expect(result).toEqual({ dispatched: true, value: {} })
      expect(writes(bridge.calls)).toEqual([
        { method: "thread/archive", params: { threadId: "root" } },
      ])
      expect(
        bridge.calls.filter((call) => call.method === "thread/read")
      ).toEqual([
        {
          method: "thread/read",
          params: { threadId: "root", includeTurns: false },
        },
      ])
      expect(bridge.listeners.size).toBe(1)
    }
  )

  it.each(["root", "child"])(
    "rejects an active loaded %s before a destructive RPC is dispatched",
    async (activeId) => {
      const bridge = harness([
        thread("root", activeId === "root" ? "active" : "idle"),
        {
          ...thread("child", activeId === "child" ? "active" : "idle", "root"),
          archived: true,
        },
      ])
      const result = await bridge.api.guardAndMutate("thread/delete", "root", [
        "root",
        "child",
      ])
      expect(result.dispatched).toBe(false)
      expect(result.error).toContain("正在运行")
      expect(writes(bridge.calls)).toEqual([])
      expect(bridge.listeners.size).toBe(1)
    }
  )

  it("rejects a root that starts running while a later child metadata read is pending", async () => {
    const entered = deferred<void>()
    const finish = deferred<unknown>()
    const bridge = harness(
      [thread("root"), thread("child", "idle", "root")],
      (call) => {
        if (call.method === "thread/read" && call.params.threadId === "child") {
          entered.resolve()
          return finish.promise
        }
        if (call.method === "thread/list") {
          return { data: [], nextCursor: null }
        }
      }
    )
    const operation = bridge.api.guardAndMutate("thread/delete", "root", [
      "root",
      "child",
    ])
    await entered.promise
    bridge.notify("thread/status/changed", {
      threadId: "root",
      status: { type: "active" },
    })
    finish.resolve({ thread: thread("child", "idle", "root") })
    const result = await operation
    expect(result.dispatched).toBe(false)
    expect(result.error).toContain("已开始运行")
    expect(writes(bridge.calls)).toEqual([])
  })

  it("rejects a newly spawned descendant during preflight", async () => {
    const entered = deferred<void>()
    const finish = deferred<unknown>()
    const bridge = harness([thread("root")], (call) => {
      if (call.method === "thread/read") {
        entered.resolve()
        return finish.promise
      }
    })
    const operation = bridge.api.guardAndMutate("thread/delete", "root", [
      "root",
    ])
    await entered.promise
    bridge.notify("thread/started", {
      thread: thread("new-child", "idle", "root"),
    })
    finish.resolve({ thread: thread("root") })
    const result = await operation
    expect(result.dispatched).toBe(false)
    expect(result.error).toContain("派生会话范围已变化")
    expect(writes(bridge.calls)).toEqual([])
  })

  it("rejects a changed descendant range before dispatching deletion", async () => {
    const bridge = harness([
      thread("root"),
      thread("new-child", "idle", "root"),
    ])
    const result = await bridge.api.guardAndMutate("thread/delete", "root", [
      "root",
    ])
    expect(result.dispatched).toBe(false)
    expect(result.error).toContain("派生会话范围已变化")
    expect(writes(bridge.calls)).toEqual([])
    expect(
      bridge.calls.filter((call) => call.method === "thread/loaded/list")
    ).toHaveLength(1)
  })

  it("marks a native error after dispatch distinctly and never retries the write", async () => {
    const bridge = harness([thread("root")], (call) => {
      if (call.method === "thread/delete") throw Error("native deletion failed")
    })
    expect(
      await bridge.api.guardAndMutate("thread/delete", "root", ["root"])
    ).toEqual({
      dispatched: true,
      error: "native deletion failed",
    })
    expect(writes(bridge.calls)).toHaveLength(1)
    expect(bridge.listeners.size).toBe(1)
  })

  it.each(["thread/archive", "thread/delete"])(
    "covers a hidden active descendant through a missing unloaded ancestor before %s",
    async (method) => {
      const bridge = harness(
        [
          thread("root"),
          thread("middle", "notLoaded", "root"),
          thread("hidden-child", "active", "middle"),
        ],
        (call) => {
          if (call.method === "thread/list")
            return { data: [], nextCursor: null }
          if (call.method === "thread/loaded/list")
            return { data: ["root", "hidden-child"] }
        }
      )
      const preview = await bridge.api.affectedScope("root")
      expect(preview.threads.map((item) => item.id)).toEqual([
        "root",
        "middle",
        "hidden-child",
      ])
      expect(preview.blockedReason).toContain("正在运行")
      const before = bridge.calls.length
      const result = await bridge.api.guardAndMutate(method, "root", [
        "root",
        "middle",
        "hidden-child",
      ])
      expect(result.dispatched).toBe(false)
      expect(result.error).toContain("正在运行")
      expect(writes(bridge.calls)).toEqual([])
      const reads = bridge.calls
        .slice(before)
        .filter((call) => call.method === "thread/read")
      expect(reads.map((call) => call.params.threadId).sort()).toEqual([
        "hidden-child",
        "root",
      ])
      expect(
        bridge.calls
          .slice(before)
          .filter((call) => call.method === "thread/loaded/list")
      ).toHaveLength(1)
      for (const read of bridge.calls.filter(
        (call) => call.method === "thread/read"
      )) {
        expect(read.params.includeTurns).toBe(false)
      }
    }
  )

  it("reads every active and archived page with explicit complete source kinds", async () => {
    const bridge = harness([], (call) => {
      if (call.method !== "thread/list") throw Error("unexpected non-list RPC")
      const { archived, cursor } = call.params
      return {
        data: [
          thread(
            `${archived ? "archived" : "active"}-${cursor ? "two" : "one"}`
          ),
        ],
        nextCursor: cursor ? null : "next-page",
      }
    })
    const result = await bridge.api.list()
    expect(result.map((item) => [item.id, item.archived])).toEqual([
      ["active-one", false],
      ["active-two", false],
      ["archived-one", true],
      ["archived-two", true],
    ])
    expect(
      bridge.calls.map((call) => [call.params.archived, call.params.cursor])
    ).toEqual([
      [false, null],
      [false, "next-page"],
      [true, null],
      [true, "next-page"],
    ])
    for (const call of bridge.calls) {
      expect(call.params.modelProviders).toEqual([])
      expect(call.params.sourceKinds).toEqual([
        "cli",
        "vscode",
        "exec",
        "appServer",
        "subAgent",
        "subAgentReview",
        "subAgentCompact",
        "subAgentThreadSpawn",
        "subAgentOther",
        "unknown",
      ])
    }
  })

  it("forwards only local lifecycle notifications and installs its listener once", () => {
    const bridge = harness()
    expect(bridge.install()).toBe(true)
    expect(bridge.install()).toBe(true)
    expect(bridge.listeners.size).toBe(1)
    bridge.notify("thread/deleted", { threadId: "remote-thread" }, "remote")
    bridge.notify("item/started", { threadId: "root" })
    expect(bridge.events).toEqual([])
    bridge.notify("thread/archived", { threadId: "root" })
    expect(bridge.events).toEqual([
      { method: "thread/archived", threadId: "root" },
    ])
    bridge.notify("thread/started", { thread: thread("child", "idle", "root") })
    expect(bridge.events[1]?.thread).toMatchObject({
      id: "child",
      parent: "root",
      archived: false,
    })
  })

  it("preserves native names in local rename notifications", () => {
    const bridge = harness()
    bridge.notify(
      "thread/name/updated",
      { threadId: "root", name: "旧名称" },
      "remote"
    )
    bridge.notify("thread/name/updated", { threadId: "root", name: "新名称" })
    expect(bridge.events).toEqual([
      { method: "thread/name/updated", threadId: "root", name: "新名称" },
    ])
  })

  it("preserves empty native previews and forwards turn boundaries for catalog reconciliation", () => {
    const bridge = harness()
    bridge.notify("thread/started", {
      thread: { ...thread("empty"), preview: "" },
    })
    bridge.notify("turn/started", { threadId: "empty", turn: { id: "turn" } })
    bridge.notify("turn/completed", { threadId: "empty", turn: { id: "turn" } })
    expect(bridge.events[0]?.thread).toMatchObject({ id: "empty", preview: "" })
    expect(bridge.events.slice(1)).toEqual([
      { method: "turn/started", threadId: "empty" },
      { method: "turn/completed", threadId: "empty" },
    ])
  })

  it("forwards cleared names for fallback handling", () => {
    const bridge = harness()
    bridge.notify("thread/name/updated", { threadId: "root", name: null })
    bridge.notify("thread/name/updated", { threadId: "root", name: "" })
    expect(bridge.events).toEqual([
      { method: "thread/name/updated", threadId: "root", name: null },
      { method: "thread/name/updated", threadId: "root", name: "" },
    ])
  })

  it("disposes an earlier version before replacing its notification listener", () => {
    const bridge = harness()
    bridge.api.version = 1
    expect(bridge.install()).toBe(true)
    expect(bridge.listeners.size).toBe(1)
    bridge.notify("thread/name/updated", {
      threadId: "root",
      name: "new title",
    })
    expect(bridge.events).toHaveLength(1)
    expect(bridge.events[0]?.name).toBe("new title")
  })

  it("requires a clean renderer for legacy scripts without a removable listener", () => {
    const bridge = harness()
    bridge.api.version = 0
    delete bridge.api.dispose
    expect(() => bridge.install()).toThrow("完全退出并重启 Codex")
    expect(bridge.listeners.size).toBe(1)
  })
})
