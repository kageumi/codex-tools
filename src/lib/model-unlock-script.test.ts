/// <reference types="node" />

import { createContext, runInContext } from "node:vm"
import { describe, expect, it, vi } from "vitest"

import unlockModule from "../../src-tauri/src/model_unlock.rs?raw"

const script = unlockModule.match(
  /const UNLOCK_SCRIPT: &str = r#"([\s\S]*?)"#;/
)![1]!
const sixLevels = ["low", "medium", "high", "xhigh", "max", "ultra"]

type Model = {
  model: string
  supportedReasoningEfforts?: Array<{ reasoningEffort: string }>
  defaultReasoningEffort?: string
  [key: string]: unknown
}
type ModelData = {
  models: Model[]
  marker?: string
  defaultModel?: Model | null
  hasModelSupportingMaxReasoningEffort?: boolean
  hasModelSupportingUltraReasoningEffort?: boolean
}
type ObserverOptions = {
  queryKey: unknown[]
  select: (data: ModelData) => ModelData
}
type Observer = {
  options: ObserverOptions
  setOptions: (options: ObserverOptions) => void
  getOptimisticResult: (options: ObserverOptions) => { data: ModelData }
  getCurrentResult: () => { data: ModelData }
}
type CatalogEntry = {
  slug: string
  supported_reasoning_levels?: Array<{ effort: string }>
  default_reasoning_level?: string
  service_tiers?: Array<{ id: string; name: string; description: string }>
  additional_speed_tiers?: string[]
  default_service_tier?: string | null
}

function entry(levels: string[] = sixLevels): CatalogEntry {
  return {
    slug: "gpt-6-astra",
    supported_reasoning_levels: levels.map((effort) => ({ effort })),
    default_reasoning_level: levels.includes("low") ? "low" : undefined,
  }
}

function stale(): Model {
  return {
    model: "gpt-6-astra",
    supportedReasoningEfforts: sixLevels
      .slice(0, 4)
      .map((reasoningEffort) => ({ reasoningEffort })),
    defaultReasoningEffort: "high",
    preserved: "original",
  }
}

function efforts(model: Model | undefined) {
  expect(model).toBeDefined()
  return model?.supportedReasoningEfforts?.map((level) => level.reasoningEffort)
}

function harness(
  catalog = [entry()],
  {
    scoped = false,
    nativeFilter = false,
    late = false,
    queryKey = [
      "models",
      "list",
      "local",
      "apikey",
      100,
      "custom",
      "model-catalogs/codex-tools.json",
    ],
  }: {
    scoped?: boolean
    nativeFilter?: boolean
    late?: boolean
    queryKey?: unknown[]
  } = {}
) {
  class TestResponse {
    payload: ModelData
    constructor(payload: ModelData) {
      this.payload = payload
    }
    async json() {
      return this.payload
    }
  }
  const timers = new Map<number, () => void>()
  let nextTimer = 0
  const listeners = new Set<(event: { type: string; query: unknown }) => void>()
  const query = {
    queryKey,
    observers: [] as Observer[],
    state: { data: { models: [stale()] } as ModelData },
  }
  const select = (data: ModelData): ModelData => {
    if (!nativeFilter) return data
    const models = data.models.map((model) => ({
      ...model,
      supportedReasoningEfforts: model.supportedReasoningEfforts?.filter(
        (level) => level.reasoningEffort !== "ultra"
      ),
    }))
    return {
      ...data,
      models,
      defaultModel: models[0],
      hasModelSupportingMaxReasoningEffort: false,
      hasModelSupportingUltraReasoningEffort: false,
    }
  }
  const makeObserver = (marker: string) => {
    const options = {
      queryKey,
      select: vi.fn((data: ModelData) => ({ ...select(data), marker })),
    }
    let current: { data: ModelData } = {
      data: options.select(query.state.data),
    }
    const observer: Observer = {
      options,
      setOptions(options) {
        this.options = options
        current = { data: options.select(query.state.data) }
        for (const listener of listeners) {
          listener({ type: "observerOptionsUpdated", query })
        }
      },
      getOptimisticResult(options) {
        return { data: options.select(query.state.data) }
      },
      getCurrentResult: () => current,
    }
    return observer
  }
  const observers = ["first", "second"].map(makeObserver)
  if (!late) query.observers.push(...observers)
  const cache = {
    getAll: () => [query],
    subscribe: vi.fn(
      (listener: (event: { type: string; query: unknown }) => void) => {
        listeners.add(listener)
        return () => listeners.delete(listener)
      }
    ),
  }
  const client = {
    getQueryCache: () => cache,
    setQueryData: vi.fn(
      (_key: string[], update: (data: ModelData) => ModelData) => {
        query.state.data = update(query.state.data)
      }
    ),
  }
  const window = {
    __CODEX_TOOLS_MODEL_CATALOG__: { models: catalog },
    setInterval: (callback: () => void) => {
      timers.set(++nextTimer, callback)
      return nextTimer
    },
    clearInterval: (id: number) => timers.delete(id),
  }
  const context = createContext({
    window,
    Response: TestResponse,
    document: {
      getElementById: () => ({
        __reactContainerTest: {
          memoizedProps: scoped
            ? { value: new Map([["scope", { queryClient: client }]]) }
            : { client },
        },
      }),
    },
  })
  const install = () => runInContext(script, context)
  const tick = () => {
    for (const callback of [...timers.values()]) callback()
  }
  install()
  return {
    TestResponse,
    window,
    observers,
    query,
    client,
    timers,
    install,
    tick,
    listeners,
    cache,
    makeObserver,
    addObservers: () => {
      query.observers.push(...observers)
      for (const listener of listeners) {
        listener({ type: "observerAdded", query })
      }
    },
    runtime: () =>
      (
        window as typeof window & {
          __CODEX_TOOLS_MODEL_UNLOCK_RUNTIME__: {
            dispose: () => void
            status: () => { queryClients: number; patchedObservers: number }
          }
        }
      ).__CODEX_TOOLS_MODEL_UNLOCK_RUNTIME__,
  }
}

describe("model unlock reasoning levels", () => {
  it("adds new models with all declared levels", async () => {
    const { TestResponse } = harness()
    const data = await new TestResponse({ models: [] }).json()
    expect(efforts(data.models[0])).toEqual(sixLevels)
  })

  it("refreshes existing response models without dropping other attributes", async () => {
    const { TestResponse } = harness()
    const data = await new TestResponse({ models: [stale()] }).json()
    expect(efforts(data.models[0])).toEqual(sixLevels)
    expect(data.models[0]).toMatchObject({
      defaultReasoningEffort: "low",
      preserved: "original",
    })
  })

  it("refreshes multiple observers using each original select without mutating input", () => {
    const { observers, tick } = harness()
    tick()
    const input = { models: [stale()] }
    for (const [index, observer] of observers.entries()) {
      const selected = observer.options.select(input)
      expect(selected.marker).toBe(index === 0 ? "first" : "second")
      expect(efforts(selected.models[0])).toEqual(sixLevels)
      expect(selected.models[0]?.preserved).toBe("original")
    }
    expect(efforts(input.models[0])).toEqual(sixLevels.slice(0, 4))
  })

  it("refreshes cached models after reinjection without stacking wrappers or timers", async () => {
    const h = harness()
    h.tick()
    const responseWrapper = h.TestResponse.prototype.json
    const setOptionsWrapper = h.observers[0]!.setOptions
    const optimisticWrapper = h.observers[0]!.getOptimisticResult
    h.window.__CODEX_TOOLS_MODEL_CATALOG__.models = [entry(["low", "max"])]
    h.install()
    h.tick()
    expect(h.TestResponse.prototype.json).toBe(responseWrapper)
    expect(h.observers[0]!.setOptions).toBe(setOptionsWrapper)
    expect(h.observers[0]!.getOptimisticResult).toBe(optimisticWrapper)
    expect(h.timers.size).toBe(1)
    expect(efforts(h.observers[0]!.getCurrentResult().data.models[0])).toEqual([
      "low",
      "max",
    ])
    const selected = h.observers[0]!.options.select({ models: [stale()] })
    expect(efforts(selected.models[0])).toEqual(["low", "max"])
    const selectWrapper = h.observers[0]!.options.select
    h.install()
    expect(h.observers[0]!.options.select).toBe(selectWrapper)
  })

  it("preserves explicit empty capabilities and clears an obsolete default", async () => {
    const { TestResponse } = harness([entry([])])
    const data = await new TestResponse({ models: [stale()] }).json()
    expect(efforts(data.models[0])).toEqual([])
    expect(data.models[0]?.defaultReasoningEffort).toBeUndefined()
  })

  it("uses the existing four-level fallback when metadata is missing", async () => {
    const { TestResponse } = harness([{ slug: "gpt-6-astra" }])
    const data = await new TestResponse({ models: [] }).json()
    expect(efforts(data.models[0])).toEqual(sixLevels.slice(0, 4))
    expect(data.models[0]?.defaultReasoningEffort).toBe("low")
  })

  it("leaves models outside the provider catalog unchanged", async () => {
    const { TestResponse } = harness()
    const other = { ...stale(), model: "other" }
    const data = await new TestResponse({ models: [other] }).json()
    expect(data.models[0]).toBe(other)
    expect(efforts(other)).toEqual(sixLevels.slice(0, 4))
  })

  it("patches a replaced select even when the observer was patched before", () => {
    const h = harness()
    h.observers[0]!.options.select = vi.fn((data: ModelData) => ({
      ...data,
      marker: "replacement",
    }))
    h.tick()
    const selected = h.observers[0]!.options.select({ models: [stale()] })
    expect(selected.marker).toBe("replacement")
    expect(efforts(selected.models[0])).toEqual(sixLevels)
  })

  it("does not repeatedly refresh unchanged query data on timer ticks", () => {
    const h = harness()
    const calls = h.client.setQueryData.mock.calls.length
    h.tick()
    h.tick()
    expect(h.client.setQueryData).toHaveBeenCalledTimes(calls)
  })

  it("finds the modern Scope query client and updates the observer's actual result", () => {
    const h = harness([entry()], { scoped: true, nativeFilter: true })
    const result = h.observers[0]!.getCurrentResult().data
    expect(efforts(result.models[0])).toEqual(sixLevels)
    expect(result.defaultModel).toBe(result.models[0])
    expect(result.hasModelSupportingMaxReasoningEffort).toBe(true)
    expect(result.hasModelSupportingUltraReasoningEffort).toBe(true)
    expect(h.client.setQueryData).not.toHaveBeenCalled()
  })

  it("retains Ultra when Scope replaces options or requests an optimistic result", () => {
    const h = harness([entry()], { scoped: true, nativeFilter: true })
    const observer = h.observers[0]!
    const replacement = h.makeObserver("replacement").options
    expect(
      efforts(observer.getOptimisticResult(replacement).data.models[0])
    ).toEqual(sixLevels)
    observer.setOptions(replacement)
    expect(efforts(observer.getCurrentResult().data.models[0])).toEqual(
      sixLevels
    )
    expect(observer.getCurrentResult().data.marker).toBe("replacement")
  })

  it("patches observers created after initialization through the cache subscription", () => {
    const h = harness([entry()], {
      scoped: true,
      nativeFilter: true,
      late: true,
    })
    h.addObservers()
    expect(efforts(h.observers[0]!.getCurrentResult().data.models[0])).toEqual(
      sixLevels
    )
    h.install()
    expect(h.listeners.size).toBe(1)
  })

  it.each([
    ["models", "list", "remote-host", "apikey", 100],
    ["models", "list", "local", "chatgpt", 100],
    ["models", "list", "local", "team", "account"],
    ["models", "list", "local", "apikey", 100],
    ["models", "list", "local", "apikey", 100, "other/catalog.json"],
    ["unrelated", "models", "list", "local", "apikey"],
  ])("does not change unrelated model queries: %j", (...queryKey) => {
    const h = harness([entry()], { scoped: true, nativeFilter: true, queryKey })
    h.tick()
    const data = h.observers[0]!.getCurrentResult().data
    expect(efforts(data.models[0])).toEqual(sixLevels.slice(0, 4))
    expect(data.hasModelSupportingUltraReasoningEffort).toBe(false)
    expect(h.client.setQueryData).not.toHaveBeenCalled()
  })

  it("never adds Ultra to a Max-only model even when the native gate is closed", () => {
    const h = harness([entry(["low", "max"])], {
      scoped: true,
      nativeFilter: true,
    })
    const data = h.observers[0]!.getCurrentResult().data
    expect(efforts(data.models[0])).toEqual(["low", "max"])
    expect(data.hasModelSupportingUltraReasoningEffort).toBe(false)
  })

  it("stops overriding a reused observer when it switches to a remote query", () => {
    const h = harness([entry()], { scoped: true, nativeFilter: true })
    const observer = h.observers[0]!
    const localOptions = h.makeObserver("local").options
    observer.setOptions({
      ...localOptions,
      queryKey: ["models", "list", "remote", "apikey", 100],
    })
    expect(efforts(observer.getCurrentResult().data.models[0])).toEqual(
      sixLevels.slice(0, 4)
    )
    expect(h.runtime().status().patchedObservers).toBe(1)
    observer.setOptions(localOptions)
    expect(efforts(observer.getCurrentResult().data.models[0])).toEqual(
      sixLevels
    )
  })

  it("restores observers and removes subscriptions when disposed", () => {
    const h = harness([entry()], { scoped: true, nativeFilter: true })
    h.runtime().dispose()
    expect(h.listeners.size).toBe(0)
    expect(h.timers.size).toBe(0)
    expect(efforts(h.observers[0]!.getCurrentResult().data.models[0])).toEqual(
      sixLevels.slice(0, 4)
    )
    h.install()
    expect(h.listeners.size).toBe(1)
    expect(efforts(h.observers[0]!.getCurrentResult().data.models[0])).toEqual(
      sixLevels
    )
  })
})

describe("model unlock fast mode", () => {
  const priorityTier = {
    id: "priority",
    name: "Fast",
    description: "Provider fast tier",
  }

  it("adds provider-declared speed tiers to the local model selector", () => {
    const h = harness(
      [
        {
          ...entry(),
          service_tiers: [
            { id: "priority", name: "Fast", description: "Provider fast tier" },
          ],
          additional_speed_tiers: ["fast"],
          default_service_tier: null,
        },
      ],
      { scoped: true }
    )
    const result = h.observers[0]!.getCurrentResult().data.models[0]
    expect(result).toMatchObject({
      serviceTiers: [
        { id: "priority", name: "Fast", description: "Provider fast tier" },
      ],
      additionalSpeedTiers: ["fast"],
      defaultServiceTier: null,
      preserved: "original",
    })
    expect(efforts(result)).toEqual(sixLevels)
  })

  it("adds missing models using camelCase without selecting the declared default", () => {
    const h = harness(
      [
        {
          ...entry(),
          service_tiers: [priorityTier],
          additional_speed_tiers: ["fast"],
          default_service_tier: "priority",
        },
      ],
      { scoped: true }
    )
    const input = { models: [] }
    const result = h.observers[0]!.options.select(input).models[0]
    expect(result).toMatchObject({
      serviceTiers: [priorityTier],
      additionalSpeedTiers: ["fast"],
      defaultServiceTier: null,
    })
    expect(result).not.toHaveProperty("service_tiers")
    expect(input.models).toEqual([])
  })

  it("clears withdrawn capabilities on reinjection without stacking wrappers", () => {
    const h = harness(
      [
        {
          ...entry(),
          service_tiers: [priorityTier],
          additional_speed_tiers: ["fast"],
        },
      ],
      { scoped: true }
    )
    const setOptions = h.observers[0]!.setOptions
    expect(
      h.observers[0]!.getCurrentResult().data.models[0]?.serviceTiers
    ).toEqual([priorityTier])
    h.window.__CODEX_TOOLS_MODEL_CATALOG__.models = [entry()]
    h.install()
    h.tick()
    expect(h.observers[0]!.getCurrentResult().data.models[0]).toMatchObject({
      serviceTiers: [],
      additionalSpeedTiers: [],
      defaultServiceTier: null,
    })
    expect(h.observers[0]!.setOptions).toBe(setOptions)
    expect(h.listeners.size).toBe(1)
    expect(h.timers.size).toBe(1)
  })

  it("does not infer fast mode from a GPT name or an existing native descriptor", () => {
    const h = harness([entry()], { scoped: true })
    const input = {
      models: [
        {
          ...stale(),
          serviceTiers: [priorityTier],
          additionalSpeedTiers: ["fast"],
          defaultServiceTier: "priority",
        },
      ],
    }
    const selected = h.observers[0]!.options.select(input)
    expect(selected.models[0]).toMatchObject({
      serviceTiers: [],
      additionalSpeedTiers: [],
      defaultServiceTier: null,
    })
    expect(input.models[0]?.serviceTiers).toEqual([priorityTier])
    expect(input.models[0]?.defaultServiceTier).toBe("priority")
  })

  it.each([
    ["models", "list", "local", "chatgpt", 100],
    ["models", "list", "remote", "apikey", 100],
    ["models", "list", "local", "apikey", 100, "other/catalog.json"],
  ])(
    "leaves speed tiers in unrelated model queries untouched: %j",
    (...queryKey) => {
      const h = harness(
        [
          {
            ...entry(),
            service_tiers: [priorityTier],
            additional_speed_tiers: ["fast"],
          },
        ],
        { scoped: true, queryKey }
      )
      const original = {
        ...stale(),
        serviceTiers: [],
        defaultServiceTier: "default",
      }
      const selected = h.observers[0]!.options.select({ models: [original] })
      expect(selected.models[0]).toBe(original)
      expect(selected.models[0]?.serviceTiers).toEqual([])
    }
  )

  it("never patches speed tiers through the unscoped Response.json hook", async () => {
    const h = harness([
      {
        ...entry(),
        service_tiers: [priorityTier],
        additional_speed_tiers: ["fast"],
      },
    ])
    const original = {
      ...stale(),
      serviceTiers: [],
      additionalSpeedTiers: [],
      defaultServiceTier: "default",
    }
    const result = await new h.TestResponse({ models: [original] }).json()
    expect(result.models[0]).toMatchObject({
      serviceTiers: [],
      additionalSpeedTiers: [],
      defaultServiceTier: "default",
    })
    const added = await new h.TestResponse({ models: [] }).json()
    expect(added.models[0]).not.toHaveProperty("serviceTiers")
  })
})
