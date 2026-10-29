// @vitest-environment jsdom

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react"
import { afterEach, beforeEach, expect, it, vi } from "vitest"

import { mockCall } from "@/lib/ipc-mock"
import type { PricingRule } from "@/types"
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
