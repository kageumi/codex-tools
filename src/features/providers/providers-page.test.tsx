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

import App from "@/App"
import { mockCall } from "@/lib/ipc-mock"
import type { DeviceAuthorization, ProviderOverview } from "@/types"
import { ProvidersPage } from "./providers-page"

const callMock = vi.hoisted(() => vi.fn())
vi.mock("@/lib/ipc", () => ({ call: callMock }))

const authorization = {
  operationId: "test-operation",
  userCode: "TEST-CODE",
  verificationUri: "https://example.test/device",
  expiresAt: Math.floor(Date.now() / 1000) + 900,
  intervalSecs: 10,
} satisfies DeviceAuthorization
let connections: ProviderOverview
const onRefresh = vi.fn()
const onAccountsSaved = vi.fn()

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
      onAccountsSaved={onAccountsSaved}
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
    { flow: "OpenAI 授权", entry: "OpenAI 授权", includesExisting: false },
    { flow: "导入 Cookie", entry: "导入 Cookie", includesExisting: false },
    {
      flow: "混合导入新旧 Cookie 账号",
      entry: "导入 Cookie",
      includesExisting: true,
    },
  ])(
    "$flow 保存新账号后，列表延迟刷新仍选中该账号",
    async ({ entry, includesExisting }) => {
      HTMLElement.prototype.scrollTo ??= () => {}
      const dashboard = await mockCall("dashboard_get", undefined)
      const account = {
        ...connections.officialAccounts[0]!,
        id: "new-account",
        name: "新保存的账号",
        remark: "",
        active: false,
      }
      const saved = includesExisting
        ? [connections.officialAccounts[0]!, account]
        : [account]
      const finishReads: Array<(result: ProviderOverview) => void> = []
      const readConnections = vi
        .fn()
        .mockResolvedValueOnce(connections)
        .mockImplementation(
          () =>
            new Promise<ProviderOverview>((resolve) =>
              finishReads.push(resolve)
            )
        )
      callMock.mockImplementation((command, args) => {
        if (command === "dashboard_get") return Promise.resolve(dashboard)
        if (command === "connections_list") return readConnections()
        if (command === "connections_login_start") {
          return Promise.resolve(authorization)
        }
        if (command === "connections_login_poll") {
          return Promise.resolve({ status: "complete", account })
        }
        if (command === "connections_import_cookie") {
          return Promise.resolve({
            accounts: saved,
            savedCount: saved.length,
            detectedFormats: [],
          })
        }
        return mockCall(command, args)
      })

      render(<App />)
      fireEvent.click(screen.getByRole("button", { name: "连接" }))
      await screen.findByRole("button", { name: entry })
      fireEvent.click(screen.getByRole("button", { name: "刷新当前页面" }))
      expect(readConnections).toHaveBeenCalledTimes(2)
      const dialog = await open(entry)
      if (entry === "OpenAI 授权") {
        await act(async () => {
          fireEvent.click(dialog.getByRole("button", { name: "获取授权码" }))
        })
        fireEvent.click(dialog.getByRole("button", { name: "检查授权结果" }))
      } else {
        fireEvent.input(dialog.getByLabelText("Cookie 登录数据"), {
          target: {
            value: includesExisting
              ? JSON.stringify([
                  { access_token: "existing" },
                  { access_token: "new" },
                ])
              : "test-token",
          },
        })
        fireEvent.click(
          dialog.getByRole("button", { name: "导入 Cookie 数据" })
        )
      }

      await waitFor(() =>
        expect(screen.queryByRole("dialog", { name: "添加连接" })).toBeNull()
      )
      const details = within(screen.getByRole("main"))
      expect(details.getByTitle(account.name)).toBeDefined()
      expect(readConnections).toHaveBeenCalledTimes(3)
      await act(async () => finishReads[0]!(connections))
      expect(details.getByTitle(account.name)).toBeDefined()
      expect(details.queryByText("当前连接")).toBeNull()
      expect(
        screen.getByRole("button", { name: `切换或管理连接：${account.name}` })
      ).toBeDefined()
      const refreshed = { ...account, name: "刷新后的新账号" }
      await act(async () => {
        finishReads[1]!({
          ...connections,
          officialAccounts: [...connections.officialAccounts, refreshed],
        })
      })
      expect(await details.findByTitle(refreshed.name)).toBeDefined()
      expect(
        screen.getByRole("button", {
          name: `切换或管理连接：${refreshed.name}`,
        })
      ).toBeDefined()
    }
  )

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
    expect(onAccountsSaved).not.toHaveBeenCalled()
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
    expect(onAccountsSaved).toHaveBeenCalledTimes(1)
  })

  it("切离官方授权清理授权状态并停止轮询", async () => {
    callMock.mockImplementation((command, args) => {
      if (command === "connections_login_start")
        return Promise.resolve(authorization)
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
        return Promise.resolve(authorization)
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
    expect(onAccountsSaved).toHaveBeenCalledWith([account])
    expect(onRefresh).toHaveBeenCalledTimes(1)
  })

  it("部分导入立即显示已保存账号，重试仅提交可编辑的剩余凭据", async () => {
    const saved = connections.officialAccounts[0]!
    const message = "前 2 项已保存，请核对剩余凭据后重试。"
    callMock.mockResolvedValueOnce({
      accounts: [saved],
      savedCount: 2,
      detectedFormats: [],
      partial: { message, retryFrom: 2 },
    })
    mount()
    const dialog = await open("导入 Cookie")
    const field = dialog.getByLabelText(
      "Cookie 登录数据"
    ) as HTMLTextAreaElement
    const wrapped = {
      label: "export",
      accounts: [{ refresh_token: "rotated" }, { refresh_token: "failed" }],
    }
    fireEvent.change(dialog.getByLabelText("显示名称（可选）"), {
      target: { value: "团队" },
    })
    fireEvent.input(field, {
      target: { value: JSON.stringify([[{ access_token: "saved" }], wrapped]) },
    })
    const submit = dialog.getByRole("button", { name: "导入 Cookie 数据" })
    await act(async () => {
      fireEvent.click(submit)
    })
    expect(dialog.getByText(message)).toBeDefined()
    expect(onAccountsSaved).toHaveBeenCalledWith([saved])
    expect(onRefresh).toHaveBeenCalledTimes(1)
    expect(JSON.parse(field.value)).toEqual([
      { label: "export", accounts: [{ refresh_token: "failed" }] },
    ])
    const corrected = field.value.replace("failed", "corrected")
    fireEvent.input(field, {
      target: { value: corrected },
      inputType: "insertFromPaste",
      data: "corrected",
    })
    callMock.mockResolvedValueOnce({ accounts: [saved], detectedFormats: [] })
    await act(async () => {
      fireEvent.click(submit)
    })
    expect(callMock).toHaveBeenLastCalledWith(
      "connections_import_cookie",
      expect.objectContaining({
        name: "团队",
        nameOffset: 2,
        content: corrected,
      })
    )
    expect(
      callMock.mock.calls
        .filter(([command]) => command === "connections_import_cookie")
        .map(([, args]) => args.content)
    ).toEqual([
      JSON.stringify([[{ access_token: "saved" }], wrapped]),
      corrected,
    ])
    expect(screen.queryByRole("dialog", { name: "添加连接" })).toBeNull()
  })

  it.each([
    {
      scenario: "用键盘全选粘贴单个新账号",
      source: "native paste",
      content: "rt-new-account",
    },
    {
      scenario: "用编辑菜单全选粘贴一批新账号",
      source: "menu paste",
      content:
        '[\r\n{"refresh_token":"rt-new-1"},\r\n{"refresh_token":"rt-new-2"}\r\n]',
    },
    {
      scenario: "部分导入清空草稿后输入新账号",
      source: "typing",
      content: "rt-new-account",
    },
  ])("$scenario时从头编号", async ({ source, content }) => {
    const saved = connections.officialAccounts[0]!
    const retryFrom = source === "typing" ? 2 : 1
    callMock.mockResolvedValueOnce({
      accounts: [saved],
      savedCount: 1,
      detectedFormats: [],
      partial: { message: "本批次导入未完成。", retryFrom },
    })
    mount()
    const dialog = await open("导入 Cookie")
    const field = dialog.getByLabelText(
      "Cookie 登录数据"
    ) as HTMLTextAreaElement
    fireEvent.change(dialog.getByLabelText("显示名称（可选）"), {
      target: { value: "团队" },
    })
    fireEvent.input(field, {
      target: {
        value: '[{"access_token":"at-saved"},{"refresh_token":"rt-previous"}]',
      },
    })
    const submit = dialog.getByRole("button", { name: "导入 Cookie 数据" })
    await act(async () => {
      fireEvent.click(submit)
    })
    expect(field.value).toBe(
      retryFrom === 2 ? "" : '[{"refresh_token":"rt-previous"}]'
    )

    field.select()
    if (source === "native paste") {
      fireEvent.paste(field, { clipboardData: { getData: () => content } })
    }
    fireEvent.input(field, {
      target: { value: content },
      inputType: source === "typing" ? "insertText" : "insertFromPaste",
      data: source === "native paste" ? null : content,
    })
    callMock.mockResolvedValueOnce({ accounts: [saved], detectedFormats: [] })
    await act(async () => {
      fireEvent.click(submit)
    })
    expect(callMock).toHaveBeenLastCalledWith("connections_import_cookie", {
      name: "团队",
      accountId: undefined,
      content: content.replace(/\r\n/g, "\n"),
    })
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

  it("更换服务地址后要求新密钥，同源路径仍可保留旧密钥", async () => {
    const provider = {
      ...connections.providers[0]!,
      baseUrl: "https://one.test/v1",
      headers: { "x-tenant": "" },
      apiKey: "",
      hasApiKey: true,
    }
    mount({ ...connections, providers: [provider] }, provider.id)
    fireEvent.click(screen.getByRole("button", { name: "编辑 API 服务" }))
    const dialog = within(
      await screen.findByRole("dialog", { name: "编辑 API 服务" })
    )
    const url = dialog.getByLabelText("Base URL")
    const key = dialog.getByLabelText("API Key") as HTMLInputElement
    const save = dialog.getByRole("button", {
      name: "保存",
    }) as HTMLButtonElement
    fireEvent.change(url, { target: { value: "https://one.test:443/v2" } })
    expect(save.disabled).toBe(false)
    expect(dialog.queryByText("保存后会移除旧地址的自定义请求头。")).toBeNull()
    fireEvent.change(url, { target: { value: "https://two.test/v1" } })
    expect(save.disabled).toBe(true)
    expect(dialog.getByText("保存后会移除旧地址的自定义请求头。")).toBeDefined()
    expect(key.placeholder).toBe("填写新地址的 API Key")
    fireEvent.change(key, { target: { value: "fixture-key" } })
    expect(save.disabled).toBe(false)
  })
})
