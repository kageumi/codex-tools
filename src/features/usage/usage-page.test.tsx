// @vitest-environment jsdom

import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react"
import { afterEach, beforeEach, expect, it, vi } from "vitest"

import { mockCall } from "@/lib/ipc-mock"
import { formatUsd } from "@/lib/format"
import type { PricingRule, UsageOverview } from "@/types"
import { UsagePage } from "./usage-page"

const callMock = vi.hoisted(() => vi.fn())
vi.mock("@/lib/ipc", () => ({ call: callMock }))

const rule: PricingRule = {
  id: "delete-test",
  version: 1,
  active: true,
  scopeKind: "global_model",
  modelPattern: "test-model",
  matchKind: "exact",
  billingMode: "unpriced",
  cacheWriteIncludedInInput: true,
  effectiveFromMs: 0,
  createdAtMs: 0,
  updatedAtMs: 0,
}

beforeEach(() => {
  callMock.mockReset()
  callMock.mockImplementation((command, args) =>
    command === "usage_list_pricing_rules"
      ? Promise.resolve([rule])
      : mockCall(command, args)
  )
})
afterEach(cleanup)

it("价目哈希相同时，自动和手动同步仅在重算费用后刷新概览", async () => {
  const base = {
    status: "cached",
    sourceUrl: "",
    modelCount: 0,
    models: [],
    rates: [],
    contentSha256: "same",
  }
  const repriced = [true, true, false]
  let finishAutomatic!: () => void
  const automatic = new Promise<void>((resolve) => {
    finishAutomatic = resolve
  })
  let requests = 0
  let amount = 0
  const sample = (await mockCall(
    "usage_get_overview",
    undefined
  )) as UsageOverview
  callMock.mockImplementation((command, args) => {
    if (command === "usage_get_overview") {
      amount += 1_000_000
      return Promise.resolve({
        ...sample,
        totals: { ...sample.totals, estimatedCostMicrousd: amount },
      })
    }
    if (command === "usage_get_official_pricing")
      return Promise.resolve({ ...base, usageRepriced: false })
    if (command === "usage_refresh_official_pricing") {
      const index = requests++
      return (index === 0 ? automatic : Promise.resolve()).then(() => ({
        ...base,
        usageRepriced: repriced[index],
      }))
    }
    return mockCall(command, args)
  })
  render(
    <UsagePage
      refreshRevision={0}
      days={7}
      groupBy="model"
      providers={[]}
      onRefresh={vi.fn()}
    />
  )
  const reads = () =>
    callMock.mock.calls.filter(([command]) => command === "usage_get_overview")
      .length
  fireEvent.click(await screen.findByRole("tab", { name: "价格规则" }))
  const sync = () =>
    screen.getByRole("button", { name: /同步$/ }) as HTMLButtonElement
  expect(sync().disabled).toBe(true)
  fireEvent.click(sync())
  expect(requests).toBe(1)
  await act(async () => {
    finishAutomatic()
  })
  expect(reads()).toBe(2)
  expect(await screen.findByText(formatUsd(2_000_000))).toBeDefined()
  await act(async () => {
    fireEvent.click(sync())
  })
  expect(reads()).toBe(3)
  expect(await screen.findByText(formatUsd(3_000_000))).toBeDefined()
  await waitFor(() => expect(sync().disabled).toBe(false))
  await act(async () => {
    fireEvent.click(sync())
  })
  expect(requests).toBe(3)
  expect(reads()).toBe(3)
})

it("删除规则先确认历史重算范围，取消不写入，确认只提交一次", async () => {
  render(
    <UsagePage
      refreshRevision={0}
      days={7}
      groupBy="model"
      providers={[]}
      onRefresh={vi.fn()}
    />
  )
  fireEvent.click(await screen.findByRole("tab", { name: "价格规则" }))
  fireEvent.click(await screen.findByRole("button", { name: "删除规则" }))
  let dialog = within(screen.getByRole("alertdialog"))
  expect(dialog.getByText(/重算.*历史费用估算/)).toBeDefined()
  expect(
    callMock.mock.calls.filter(([name]) => name === "usage_delete_pricing_rule")
  ).toHaveLength(0)
  fireEvent.click(dialog.getByRole("button", { name: "取消" }))
  expect(screen.queryByRole("alertdialog")).toBeNull()
  fireEvent.click(screen.getByRole("button", { name: "删除规则" }))
  dialog = within(screen.getByRole("alertdialog"))
  const confirm = dialog.getByRole("button", { name: "删除并重算" })
  fireEvent.click(confirm)
  fireEvent.click(confirm)
  const query = callMock.mock.calls.find(
    ([name]) => name === "usage_get_overview"
  )![1].query
  await waitFor(() =>
    expect(callMock).toHaveBeenCalledWith("usage_reprice", {
      range: query.range,
    })
  )
  expect(
    callMock.mock.calls.filter(([name]) => name === "usage_delete_pricing_rule")
  ).toEqual([["usage_delete_pricing_rule", { id: rule.id }]])
})
