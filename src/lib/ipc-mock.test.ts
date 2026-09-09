import { afterAll, beforeAll, describe, expect, it, vi } from "vitest"

import { filterMockSelectedModels, mockCall } from "./ipc-mock"
import type {
  Provider,
  ProviderOverview,
  ProviderSaveInput,
  ResetCreditConsumeResult,
  ResetCreditDetails,
} from "../types"

describe("filterMockSelectedModels", () => {
  it("keeps the default all-models selection undefined", () => {
    expect(filterMockSelectedModels(undefined, ["new-model"])).toBeUndefined()
  })

  it("filters an explicit selection against refreshed models", () => {
    expect(
      filterMockSelectedModels(
        ["new-model", "stale-model"],
        ["new-model", "other-model"]
      )
    ).toEqual(["new-model"])
  })
})

describe("mockCall", () => {
  beforeAll(() => vi.useFakeTimers())
  afterAll(() => vi.useRealTimers())

  const saveProvider = async (provider: ProviderSaveInput) => {
    const result = mockCall("connections_save_provider", { provider })
    await vi.runAllTimersAsync()
    return result as Promise<Provider>
  }

  it("saves and filters an explicit selection after a source change", async () => {
    const input = {
      id: "provider-mock-selection",
      name: "Mock selection",
      baseUrl: "https://mock.example/v1",
      headers: {},
      timeoutSecs: 30,
      enabled: true,
      apiType: "responses" as const,
      apiKey: "secret",
      selectedModels: ["gpt-5.6", "stale-model"],
    }
    await saveProvider(input)
    const saved = await saveProvider({
      ...input,
      baseUrl: "https://changed-mock.example/v1",
    })

    expect(saved.availableModels).toEqual(["gpt-5.6"])
    expect(saved.selectedModels).toEqual(["gpt-5.6"])
  })

  it("keeps undefined as the default all-models selection", async () => {
    const saved = await saveProvider({
      id: "provider-mock-all-models",
      name: "Mock all models",
      baseUrl: "https://mock-all.example/v1",
      headers: {},
      timeoutSecs: 30,
      enabled: true,
      apiType: "responses",
      apiKey: "secret",
      selectedModels: null,
    })

    expect(saved.selectedModels).toBeUndefined()
  })

  it("persists, clears, and isolates a provider context-window override", async () => {
    const first = {
      id: "provider-mock-context-one",
      name: "Mock context one",
      baseUrl: "https://mock-context-one.example/v1",
      headers: {},
      timeoutSecs: 30,
      enabled: true,
      apiType: "responses" as const,
      apiKey: "secret",
      selectedModels: null,
      contextWindowOverride: 262_144,
    }
    const second = await saveProvider({
      ...first,
      id: "provider-mock-context-two",
      name: "Mock context two",
      contextWindowOverride: 65_536,
    })
    expect((await saveProvider(first)).contextWindowOverride).toBe(262_144)
    expect(second.contextWindowOverride).toBe(65_536)
    expect(
      (await saveProvider({ ...first, contextWindowOverride: null }))
        .contextWindowOverride
    ).toBeUndefined()
    expect(
      (
        await saveProvider({
          ...first,
          id: "provider-mock-context-two",
          name: "Mock context two",
          contextWindowOverride: 65_536,
        })
      ).contextWindowOverride
    ).toBe(65_536)
  })

  it("returns server-summary card counts and consumes only the selected mock card", async () => {
    const detailsRequest = mockCall("connections_get_reset_credits", {
      accountId: "account-work",
    })
    await vi.runAllTimersAsync()
    const details = (await detailsRequest) as ResetCreditDetails
    expect(details.summary.availableCount).toBe(4)
    const target = details.credits.find(
      (credit) => credit.status === "available"
    )!

    const consumeRequest = mockCall("connections_consume_reset_credit", {
      accountId: "account-work",
      creditId: target.id,
      idempotencyKey: "reset-credit-mock-operation-0001",
    })
    await vi.runAllTimersAsync()
    const result = (await consumeRequest) as ResetCreditConsumeResult
    expect(result.outcome).toBe("reset")
    expect(
      result.details.credits.find((credit) => credit.id === target.id)?.status
    ).toBe("redeemed")
    expect(result.details.summary.availableCount).toBe(3)
  })

  it("consumes a non-current account card without switching the active connection", async () => {
    const request = mockCall("connections_consume_reset_credit", {
      accountId: "account-personal",
      creditId: "personal-available",
      idempotencyKey: "reset-credit-personal-operation-0001",
    })
    await vi.runAllTimersAsync()
    await request
    const overviewRequest = mockCall("connections_list", {})
    await vi.runAllTimersAsync()
    const overview = (await overviewRequest) as ProviderOverview
    expect(
      overview.officialAccounts.find((account) => account.id === "account-work")
        ?.active
    ).toBe(true)
    expect(
      overview.officialAccounts.find(
        (account) => account.id === "account-personal"
      )?.active
    ).toBe(false)
  })
})
