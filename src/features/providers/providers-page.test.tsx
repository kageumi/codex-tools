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
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import { mockCall } from "@/lib/ipc-mock"
import type { ProviderOverview } from "@/types"
import { ProvidersPage } from "./providers-page"

const callMock = vi.hoisted(() => vi.fn())
vi.mock("@/lib/ipc", () => ({ call: callMock }))

let connections: ProviderOverview
const onRefresh = vi.fn()
const onSelectedIdChange = vi.fn()

function mount(
  overview = connections,
  selectedId = overview.officialAccounts[0]?.id
) {
  return render(
    <ProvidersPage
      connections={overview}
      selectedId={selectedId}
      refreshRevision={0}
      onRefresh={onRefresh}
      onSelectedIdChange={onSelectedIdChange}
    />
  )
}

async function open(label: string) {
  fireEvent.click(screen.getByRole("button", { name: label }))
  return within(await screen.findByRole("dialog", { name: "添加连接" }))
}

beforeEach(async () => {
  vi.clearAllMocks()
  callMock.mockImplementation(mockCall)
  connections = (await mockCall(
    "connections_list",
    undefined
  )) as ProviderOverview
})

afterEach(() => {
  cleanup()
  vi.useRealTimers()
})

describe("连接添加入口", () => {
  it.each([
    ["OpenAI 授权", "官方授权"],
    ["导入 Cookie", "Cookie 导入"],
    ["添加 API 服务", "API 服务"],
  ])("%s 直接打开对应标签", async (label, tab) => {
    mount()
    const dialog = await open(label)
    expect(dialog.getAllByRole("tab")).toHaveLength(3)
    expect(
      dialog.getByRole("tab", { name: tab }).getAttribute("aria-selected")
    ).toBe("true")
  })

  it("空状态也使用三个快捷入口", async () => {
    mount({ providers: [], officialAccounts: [] })
    expect(screen.getByText("还没有连接")).toBeDefined()
    const dialog = await open("添加 API 服务")
    expect(dialog.getByLabelText("API Key")).toBeDefined()
  })

  it("切换标签保留草稿，关闭后清理所有新建输入", async () => {
    mount()
    let dialog = await open("导入 Cookie")
    fireEvent.change(dialog.getByLabelText("显示名称（可选）"), {
      target: { value: "Cookie 草稿" },
    })
    fireEvent.input(dialog.getByLabelText("Cookie 登录数据"), {
      target: { value: "test-token" },
    })
    fireEvent.click(dialog.getByRole("tab", { name: "API 服务" }))
    fireEvent.change(dialog.getByLabelText("名称"), {
      target: { value: "API 草稿" },
    })
    fireEvent.change(dialog.getByLabelText("API Key"), {
      target: { value: "test-key" },
    })
    fireEvent.change(dialog.getByLabelText("自定义上下文窗口（token，可选）"), {
      target: { value: "12345" },
    })
    fireEvent.click(dialog.getByRole("tab", { name: "Cookie 导入" }))
    expect(
      (dialog.getByLabelText("Cookie 登录数据") as HTMLTextAreaElement).value
    ).toBe("test-token")
    fireEvent.click(dialog.getByRole("tab", { name: "API 服务" }))
    expect((dialog.getByLabelText("名称") as HTMLInputElement).value).toBe(
      "API 草稿"
    )
    expect(
      (
        dialog.getByLabelText(
          "自定义上下文窗口（token，可选）"
        ) as HTMLInputElement
      ).value
    ).toBe("12345")
    fireEvent.click(dialog.getByRole("button", { name: "取消" }))
    expect(screen.queryByRole("dialog", { name: "添加连接" })).toBeNull()
    dialog = await open("添加 API 服务")
    expect((dialog.getByLabelText("名称") as HTMLInputElement).value).toBe("")
    expect((dialog.getByLabelText("API Key") as HTMLInputElement).value).toBe(
      ""
    )
    fireEvent.click(dialog.getByRole("tab", { name: "Cookie 导入" }))
    expect(
      (dialog.getByLabelText("Cookie 登录数据") as HTMLTextAreaElement).value
    ).toBe("")
    expect(
      (dialog.getByLabelText("显示名称（可选）") as HTMLInputElement).value
    ).toBe("")
  })

  it("保存期间禁用切换和关闭，连续点击只保存一次", async () => {
    let finish!: (value: unknown) => void
    callMock.mockImplementation((command, args) =>
      command === "connections_save_provider"
        ? new Promise((resolve) => {
            finish = resolve
          })
        : mockCall(command, args)
    )
    mount()
    const dialog = await open("添加 API 服务")
    fireEvent.change(dialog.getByLabelText("名称"), {
      target: { value: "测试服务" },
    })
    const save = dialog.getByRole("button", { name: "保存" })
    fireEvent.click(save)
    fireEvent.click(save)
    expect(
      callMock.mock.calls.filter(
        ([command]) => command === "connections_save_provider"
      )
    ).toHaveLength(1)
    expect(
      dialog
        .getAllByRole("tab")
        .every((tab) => tab.getAttribute("aria-disabled") === "true")
    ).toBe(true)
    expect(
      (dialog.getByRole("button", { name: "取消" }) as HTMLButtonElement)
        .disabled
    ).toBe(true)
    expect(dialog.queryByRole("button", { name: "关闭" })).toBeNull()
    fireEvent.keyDown(screen.getByRole("dialog", { name: "添加连接" }), {
      key: "Escape",
    })
    expect(screen.getByRole("dialog", { name: "添加连接" })).toBeDefined()
    await act(async () => {
      finish({ ...connections.providers[0], name: "测试服务" })
    })
    expect(screen.queryByRole("dialog", { name: "添加连接" })).toBeNull()
    expect(onRefresh).toHaveBeenCalledTimes(1)
    expect(onSelectedIdChange).not.toHaveBeenCalled()
  })

  it("保存失败保留输入并恢复交互", async () => {
    callMock.mockRejectedValueOnce(new Error("保存失败测试"))
    mount()
    const dialog = await open("添加 API 服务")
    fireEvent.change(dialog.getByLabelText("名称"), {
      target: { value: "保留草稿" },
    })
    await act(async () => {
      fireEvent.click(dialog.getByRole("button", { name: "保存" }))
    })
    expect((dialog.getByLabelText("名称") as HTMLInputElement).value).toBe(
      "保留草稿"
    )
    expect(
      (dialog.getByRole("button", { name: "保存" }) as HTMLButtonElement)
        .disabled
    ).toBe(false)
    expect(onRefresh).not.toHaveBeenCalled()
  })

  it("Cookie 导入成功后关闭弹窗并刷新账号", async () => {
    mount()
    const dialog = await open("导入 Cookie")
    fireEvent.input(dialog.getByLabelText("Cookie 登录数据"), {
      target: { value: "test-token" },
    })
    await act(async () => {
      fireEvent.click(dialog.getByRole("button", { name: "导入 Cookie 数据" }))
    })
    expect(
      callMock.mock.calls.filter(
        ([command]) => command === "connections_import_cookie"
      )
    ).toHaveLength(1)
    await waitFor(() => expect(onRefresh).toHaveBeenCalledTimes(1))
    expect(screen.queryByRole("dialog", { name: "添加连接" })).toBeNull()
    expect(onSelectedIdChange).toHaveBeenCalledTimes(1)
  })

  it("切离官方授权清理授权状态并停止轮询", async () => {
    callMock.mockImplementation((command, args) => {
      if (command === "connections_login_start")
        return Promise.resolve({
          operationId: "test-operation",
          userCode: "TEST-CODE",
          intervalSecs: 10,
        })
      if (command === "connections_login_poll")
        return Promise.resolve({ status: "pending" })
      return mockCall(command, args)
    })
    mount()
    const dialog = await open("OpenAI 授权")
    vi.useFakeTimers()
    await act(async () => {
      fireEvent.click(dialog.getByRole("button", { name: "获取授权码" }))
    })
    expect(dialog.getByText("TEST-CODE")).toBeDefined()
    fireEvent.click(dialog.getByRole("tab", { name: "API 服务" }))
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000)
    })
    expect(
      callMock.mock.calls.filter(
        ([command]) => command === "connections_login_poll"
      )
    ).toHaveLength(0)
    fireEvent.click(dialog.getByRole("tab", { name: "官方授权" }))
    expect(dialog.queryByText("TEST-CODE")).toBeNull()
    expect(dialog.getByRole("button", { name: "获取授权码" })).toBeDefined()
  })

  it("官方授权成功后关闭弹窗并选中保存的账号", async () => {
    const account = connections.officialAccounts[0]!
    callMock.mockImplementation((command, args) => {
      if (command === "connections_login_start")
        return Promise.resolve({
          operationId: "test-operation",
          userCode: "TEST-CODE",
          intervalSecs: 10,
        })
      if (command === "connections_login_poll")
        return Promise.resolve({ status: "complete", account })
      return mockCall(command, args)
    })
    mount()
    const dialog = await open("OpenAI 授权")
    await act(async () => {
      fireEvent.click(dialog.getByRole("button", { name: "获取授权码" }))
    })
    await act(async () => {
      fireEvent.click(dialog.getByRole("button", { name: "检查授权结果" }))
    })
    expect(screen.queryByRole("dialog", { name: "添加连接" })).toBeNull()
    expect(onSelectedIdChange).toHaveBeenCalledWith(account.id)
    expect(onRefresh).toHaveBeenCalledTimes(1)
  })

  it("Cookie 导入失败显示错误并保留原始草稿", async () => {
    callMock.mockRejectedValueOnce(new Error("Cookie 验证失败测试"))
    mount()
    const dialog = await open("导入 Cookie")
    fireEvent.input(dialog.getByLabelText("Cookie 登录数据"), {
      target: { value: "test-token" },
    })
    await act(async () => {
      fireEvent.click(dialog.getByRole("button", { name: "导入 Cookie 数据" }))
    })
    expect(dialog.getByText("Cookie 验证失败测试")).toBeDefined()
    expect(
      (dialog.getByLabelText("Cookie 登录数据") as HTMLTextAreaElement).value
    ).toBe("test-token")
    expect(onRefresh).not.toHaveBeenCalled()
  })

  it("现有 API 编辑入口保留共享表单", async () => {
    const provider = connections.providers[0]!
    mount(connections, provider.id)
    fireEvent.click(screen.getByRole("button", { name: "编辑 API 服务" }))
    const dialog = within(
      await screen.findByRole("dialog", { name: "编辑 API 服务" })
    )
    expect((dialog.getByLabelText("名称") as HTMLInputElement).value).toBe(
      provider.name
    )
    expect(
      dialog.getByLabelText("自定义上下文窗口（token，可选）")
    ).toBeDefined()
    expect(dialog.getByRole("button", { name: "保存" })).toBeDefined()
  })
})
