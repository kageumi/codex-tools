// @vitest-environment jsdom

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import type { PricingRule, Provider, UsageRange } from "@/types"
import { PricingEditor } from "./pricing-editor-dialog"

const callMock = vi.hoisted(() => vi.fn())
vi.mock("@/lib/ipc", () => ({ call: callMock }))

const range: UsageRange = {
  startAtMs: 1_800_000_000_000,
  endAtMs: 1_800_604_800_000,
}
const provider: Provider = {
  id: "provider-test",
  name: "测试服务",
  baseUrl: "https://example.com/v1",
  headers: {},
  timeoutSecs: 30,
  enabled: true,
  active: false,
  apiType: "responses",
  createdAt: 0,
  updatedAt: 0,
}
const rule: PricingRule = {
  id: "rule-test",
  version: 2,
  active: true,
  scopeKind: "provider_model",
  providerId: provider.id,
  modelPattern: "test-model",
  matchKind: "exact",
  billingMode: "token",
  inputUsdPerMillion: "2.50",
  outputUsdPerMillion: "10.00",
  cacheWriteIncludedInInput: true,
  effectiveFromMs: 0,
  createdAtMs: 123,
  updatedAtMs: 456,
}

function mount(editingRule?: PricingRule, rules: PricingRule[] = [rule]) {
  render(
    <PricingEditor
      open
      range={range}
      modelOptions={[rule.modelPattern]}
      providers={[provider]}
      rules={rules}
      editingRule={editingRule}
      onOpenChange={vi.fn()}
      onSaved={vi.fn()}
    />
  )
}

beforeEach(() => {
  callMock.mockReset()
  callMock.mockResolvedValue({})
})
afterEach(cleanup)

describe("价格规则的费用范围", () => {
  it("在价格字段分组标明币种和计量单位，并展示生效与重算范围", () => {
    mount(rule)
    expect(
      screen.getByRole("group", { name: "价格（美元／百万 Token）" })
    ).toBeDefined()
    expect(screen.getByText("生效时间")).toBeDefined()
    expect(screen.getByText("重算范围")).toBeDefined()
    expect(screen.getByRole("button", { name: "保存并重算" })).toBeDefined()
  })

  it("编辑已有规则时不随当前筛选范围改写生效时间和创建时间", async () => {
    mount(rule)
    fireEvent.click(screen.getByRole("button", { name: /保存/ }))
    await waitFor(() =>
      expect(callMock).toHaveBeenCalledWith("usage_reprice", { range })
    )
    const saved = callMock.mock.calls.find(
      ([name]) => name === "usage_save_pricing_rule"
    )![1].input
    expect(saved.effectiveFromMs).toBe(rule.effectiveFromMs)
    expect(saved.createdAtMs).toBe(rule.createdAtMs)
    expect(saved.id).toBe(rule.id)
  })

  it.each([true, false])(
    "添加规则时复用已有生效时间，已有等价规则：%s",
    async (equivalent) => {
      mount(undefined, equivalent ? [rule] : [])
      fireEvent.click(screen.getByRole("combobox", { name: "来源" }))
      const source = await screen.findByRole("option", { name: provider.name })
      fireEvent.pointerDown(source, { pointerType: "mouse" })
      fireEvent.click(source)
      fireEvent.change(screen.getByLabelText("模型", { exact: true }), {
        target: { value: rule.modelPattern },
      })
      fireEvent.click(screen.getByRole("button", { name: /保存/ }))
      await waitFor(() =>
        expect(callMock).toHaveBeenCalledWith("usage_reprice", { range })
      )
      const saved = callMock.mock.calls.find(
        ([name]) => name === "usage_save_pricing_rule"
      )![1].input
      expect(saved.effectiveFromMs).toBe(
        equivalent ? rule.effectiveFromMs : range.startAtMs
      )
      expect(saved.id).toBe(equivalent ? rule.id : "")
    }
  )
})
