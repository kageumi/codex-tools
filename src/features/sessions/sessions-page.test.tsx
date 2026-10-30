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

import type { Session, SessionSyncStatus } from "@/types"
import { SessionsPage } from "./sessions-page"

const { callMock, listenMock, stopMock } = vi.hoisted(() => ({
  callMock: vi.fn(),
  listenMock: vi.fn(),
  stopMock: vi.fn(),
}))
vi.mock("@/lib/ipc", () => ({ call: callMock }))
vi.mock("@tauri-apps/api/core", () => ({ isTauri: () => true }))
vi.mock("@tauri-apps/api/event", () => ({ listen: listenMock }))

let sessions: Session[]
let sync: SessionSyncStatus
let notify: (event?: { payload?: { kind: string } }) => void
const onRefresh = vi.fn()

function mount() {
  return render(
    <SessionsPage refreshRevision={0} query="" onRefresh={onRefresh} />
  )
}

function commands(command: string) {
  return callMock.mock.calls.filter(([name]) => name === command)
}

beforeEach(() => {
  vi.clearAllMocks()
  sessions = ["空闲会话", "运行会话"].map((title, index) => ({
    identity: `state.sqlite#${index}`,
    id: `thread-${index}`,
    title,
    provider: "openai",
    cwd: "D:/projects/a-very-long-path-to-the-workspace",
    archived: false,
    updatedAt: 1_780_000_000_000,
    sourceDb: "state.sqlite",
    originalProvider: "openai",
    hasUserEvent: true,
  }))
  sync = {
    connected: true,
    message: "已连接 Codex，会话操作将自动同步。",
    threads: [
      { threadId: "thread-0", state: "idle" },
      { threadId: "thread-1", state: "running" },
    ],
  }
  listenMock.mockImplementation(async (_event, handler) => {
    notify = handler
    return stopMock
  })
  callMock.mockImplementation(async (command, args) => {
    if (command === "sessions_list") {
      const items = sessions.filter(
        (session) => session.archived === (args.status === "archived")
      )
      return {
        items: structuredClone(items),
        total: items.length,
        page: 1,
        pageSize: 6,
      }
    }
    if (command === "sessions_scan")
      return {
        currentProvider: "openai",
        targets: [],
        rolloutFiles: 2,
        sessionMetaCount: 2,
        databases: [],
        warnings: [],
      }
    if (command === "sessions_sync_status") return sync
    if (command === "sessions_prepare_delete")
      return {
        threadId: args.threadId,
        affectedThreads: [
          { id: args.threadId, title: "空闲会话" },
          { id: "child-1", title: "派生会话" },
        ],
      }
    if (command === "sessions_mutate") {
      const target = sessions.find((session) => session.id === args.threadId)
      if (args.action === "delete")
        sessions = sessions.filter((session) => session !== target)
      else if (target) target.archived = args.action === "archive"
      return { message: "操作已同步。" }
    }
    throw new Error(`Unexpected command ${command}`)
  })
})

afterEach(cleanup)

describe("会话操作", () => {
  it("空闲会话可操作，运行中的会话显示可聚焦的禁用原因，渲染不重复查询", async () => {
    const view = mount()
    await waitFor(() =>
      expect(
        (
          screen.getByRole("button", {
            name: "归档：空闲会话",
          }) as HTMLButtonElement
        ).disabled
      ).toBe(false)
    )
    expect(
      (
        screen.getByRole("button", {
          name: "归档：运行会话",
        }) as HTMLButtonElement
      ).disabled
    ).toBe(true)
    expect(
      screen.getByLabelText("归档不可用：会话正在运行，请等待完成后再操作。")
        .tabIndex
    ).toBe(0)
    view.rerender(
      <SessionsPage refreshRevision={0} query="" onRefresh={onRefresh} />
    )
    expect(commands("sessions_sync_status")).toHaveLength(1)
    expect(commands("sessions_list")[0]?.[1]).toMatchObject({ pageSize: 6 })
  })

  it("连接失败时所有操作禁用并展示重新启动指引", async () => {
    sync = {
      connected: false,
      message: "请完全退出 Codex，再从 CodexTools 启动。",
      threads: [],
    }
    mount()
    expect(await screen.findByText(sync.message)).toBeDefined()
    expect(
      (
        screen.getByRole("button", {
          name: "删除：空闲会话",
        }) as HTMLButtonElement
      ).disabled
    ).toBe(true)
    expect(commands("sessions_mutate")).toHaveLength(0)
  })

  it("归档立即执行，归档页只显示取消归档按钮", async () => {
    mount()
    await waitFor(() =>
      expect(
        (
          screen.getByRole("button", {
            name: "归档：空闲会话",
          }) as HTMLButtonElement
        ).disabled
      ).toBe(false)
    )
    fireEvent.click(screen.getByRole("button", { name: "归档：空闲会话" }))
    await waitFor(() =>
      expect(
        screen.queryByRole("button", { name: "归档：空闲会话" })
      ).toBeNull()
    )
    expect(commands("sessions_mutate")[0]?.[1]).toEqual({
      threadId: "thread-0",
      action: "archive",
    })
    fireEvent.click(screen.getByRole("button", { name: "已归档" }))
    await screen.findByRole("button", {
      name: "取消归档：空闲会话",
    })
    expect(screen.queryByRole("button", { name: "删除：空闲会话" })).toBeNull()
    await waitFor(() =>
      expect(
        (
          screen.getByRole("button", {
            name: "取消归档：空闲会话",
          }) as HTMLButtonElement
        ).disabled
      ).toBe(false)
    )
    fireEvent.click(screen.getByRole("button", { name: "取消归档：空闲会话" }))
    await waitFor(() => expect(commands("sessions_mutate")).toHaveLength(2))
    expect(commands("sessions_mutate")[1]?.[1]).toEqual({
      threadId: "thread-0",
      action: "unarchive",
    })
    expect(onRefresh).not.toHaveBeenCalled()
  })

  it("删除展示派生会话，默认聚焦取消，二次确认携带完整范围且重复点击只提交一次", async () => {
    let finish: (result: { message: string }) => void = () => {}
    const implementation = callMock.getMockImplementation()!
    callMock.mockImplementation((command, args) =>
      command === "sessions_mutate"
        ? new Promise((resolve) => {
            finish = resolve
          })
        : implementation(command, args)
    )
    mount()
    await waitFor(() =>
      expect(
        (
          screen.getByRole("button", {
            name: "删除：空闲会话",
          }) as HTMLButtonElement
        ).disabled
      ).toBe(false)
    )
    fireEvent.click(screen.getByRole("button", { name: "删除：空闲会话" }))
    const dialog = within(
      await screen.findByRole("alertdialog", { name: "永久删除会话？" })
    )
    expect(await dialog.findByText("派生会话")).toBeDefined()
    await waitFor(() =>
      expect(document.activeElement).toBe(
        dialog.getByRole("button", { name: "取消" })
      )
    )
    expect(commands("sessions_mutate")).toHaveLength(0)
    const confirm = dialog.getByRole("button", { name: "永久删除" })
    fireEvent.click(confirm)
    fireEvent.click(confirm)
    expect(commands("sessions_mutate")).toHaveLength(1)
    expect(commands("sessions_mutate")[0]?.[1]).toEqual({
      threadId: "thread-0",
      action: "delete",
      confirmedThreadIds: ["thread-0", "child-1"],
    })
    await act(async () => finish({ message: "已删除" }))
    await waitFor(() => expect(screen.queryByRole("alertdialog")).toBeNull())
  })

  it("删除失败保留确认弹窗与原因，不自动重试", async () => {
    const implementation = callMock.getMockImplementation()!
    callMock.mockImplementation((command, args) =>
      command === "sessions_mutate"
        ? Promise.reject(new Error("删除范围已变化，请重新确认。"))
        : implementation(command, args)
    )
    mount()
    await waitFor(() =>
      expect(
        (
          screen.getByRole("button", {
            name: "删除：空闲会话",
          }) as HTMLButtonElement
        ).disabled
      ).toBe(false)
    )
    fireEvent.click(screen.getByRole("button", { name: "删除：空闲会话" }))
    const dialog = within(await screen.findByRole("alertdialog"))
    await dialog.findByText("派生会话")
    fireEvent.click(dialog.getByRole("button", { name: "永久删除" }))
    expect(
      await dialog.findByText("删除范围已变化，请重新确认。")
    ).toBeDefined()
    expect(screen.getByRole("alertdialog")).toBeDefined()
    expect(commands("sessions_mutate")).toHaveLength(1)
  })

  it("原生通知合并刷新，监听正确清理", async () => {
    const view = mount()
    await screen.findByText(sync.message)
    act(() => {
      notify()
      notify()
      notify()
    })
    await waitFor(() => expect(commands("sessions_list")).toHaveLength(2))
    await waitFor(() =>
      expect(commands("sessions_sync_status")).toHaveLength(2)
    )
    expect(commands("sessions_list")[1]?.[1].refresh).toBe(true)
    expect(commands("sessions_scan")).toHaveLength(2)
    view.unmount()
    expect(stopMock).toHaveBeenCalledTimes(1)
  })

  it("运行和连接事件只查询状态，不重复读取列表或扫描目录", async () => {
    mount()
    await screen.findByText(sync.message)
    sync.threads[0] = { threadId: "thread-0", state: "running" }
    act(() => {
      notify({ payload: { kind: "runtime" } })
      notify({ payload: { kind: "connection" } })
    })
    await waitFor(() =>
      expect(commands("sessions_sync_status")).toHaveLength(2)
    )
    await waitFor(() =>
      expect(
        (
          screen.getByRole("button", {
            name: "归档：空闲会话",
          }) as HTMLButtonElement
        ).disabled
      ).toBe(true)
    )
    expect(commands("sessions_list")).toHaveLength(1)
    expect(commands("sessions_scan")).toHaveLength(1)
  })

  it("混合通知优先合并为目录刷新", async () => {
    mount()
    await screen.findByText(sync.message)
    act(() => {
      notify({ payload: { kind: "runtime" } })
      notify({ payload: { kind: "catalog" } })
      notify({ payload: { kind: "connection" } })
    })
    await waitFor(() => expect(commands("sessions_list")).toHaveLength(2))
    expect(commands("sessions_scan")).toHaveLength(2)
    expect(commands("sessions_sync_status")).toHaveLength(2)
  })

  it("操作失败后仅核对状态，使结果不确定的会话不可重复操作", async () => {
    const implementation = callMock.getMockImplementation()!
    callMock.mockImplementation((command, args) => {
      if (command === "sessions_mutate") {
        sync.threads[0] = {
          threadId: "thread-0",
          state: "unknown",
          blockedReason: "操作结果不确定，正在核对。",
        }
        return Promise.reject(new Error("操作超时，正在核对结果。"))
      }
      return implementation(command, args)
    })
    mount()
    await waitFor(() =>
      expect(
        (
          screen.getByRole("button", {
            name: "归档：空闲会话",
          }) as HTMLButtonElement
        ).disabled
      ).toBe(false)
    )
    fireEvent.click(screen.getByRole("button", { name: "归档：空闲会话" }))
    await waitFor(() =>
      expect(commands("sessions_sync_status")).toHaveLength(2)
    )
    expect(
      await screen.findByLabelText("归档不可用：操作结果不确定，正在核对。")
    ).toBeDefined()
    expect(commands("sessions_mutate")).toHaveLength(1)
    expect(commands("sessions_list")).toHaveLength(1)
    expect(commands("sessions_scan")).toHaveLength(1)
  })

  it("成功的原生通知与本地刷新合并为一次，不重复扫描", async () => {
    const implementation = callMock.getMockImplementation()!
    callMock.mockImplementation(async (command, args) => {
      const response = await implementation(command, args)
      if (command === "sessions_mutate") notify()
      return response
    })
    mount()
    await waitFor(() =>
      expect(
        (
          screen.getByRole("button", {
            name: "归档：空闲会话",
          }) as HTMLButtonElement
        ).disabled
      ).toBe(false)
    )
    fireEvent.click(screen.getByRole("button", { name: "归档：空闲会话" }))
    await waitFor(() =>
      expect(
        screen.queryByRole("button", { name: "归档：空闲会话" })
      ).toBeNull()
    )
    expect(commands("sessions_list")).toHaveLength(2)
    expect(commands("sessions_scan")).toHaveLength(2)
    expect(onRefresh).not.toHaveBeenCalled()
  })

  it("运行状态读取失败时保持按钮禁用", async () => {
    const implementation = callMock.getMockImplementation()!
    callMock.mockImplementation((command, args) =>
      command === "sessions_sync_status"
        ? Promise.reject(new Error("Codex 连接已断开。"))
        : implementation(command, args)
    )
    mount()
    await screen.findByText("Codex 连接已断开。")
    expect(
      (
        screen.getByRole("button", {
          name: "归档：空闲会话",
        }) as HTMLButtonElement
      ).disabled
    ).toBe(true)
    expect(commands("sessions_mutate")).toHaveLength(0)
  })

  it("挂载取消后才完成注册的监听也会释放", async () => {
    let finish: (stop: () => void) => void = () => {}
    listenMock.mockImplementation(
      () =>
        new Promise((resolve) => {
          finish = resolve
        })
    )
    const view = mount()
    await screen.findByText(sync.message)
    view.unmount()
    await act(async () => finish(stopMock))
    expect(stopMock).toHaveBeenCalledTimes(1)
  })

  it("取消时的退出动画被主题切换中止后仍卸载弹窗", async () => {
    const visited = new WeakSet<HTMLElement>()
    const original = HTMLElement.prototype.getAnimations
    HTMLElement.prototype.getAnimations = function () {
      if (this.hasAttribute("data-ending-style") && !visited.has(this)) {
        visited.add(this)
        return [
          {
            finished: Promise.reject(new Error("theme changed")),
            pending: false,
            playState: "idle",
          } as unknown as Animation,
        ]
      }
      return []
    }
    try {
      mount()
      await waitFor(() =>
        expect(
          (
            screen.getByRole("button", {
              name: "删除：空闲会话",
            }) as HTMLButtonElement
          ).disabled
        ).toBe(false)
      )
      fireEvent.click(screen.getByRole("button", { name: "删除：空闲会话" }))
      const dialog = within(await screen.findByRole("alertdialog"))
      await dialog.findByText("派生会话")
      fireEvent.click(dialog.getByRole("button", { name: "取消" }))
      await waitFor(() =>
        expect(
          document.querySelector('[data-slot="alert-dialog-overlay"]')
        ).toBeNull()
      )
      expect(
        document.querySelector('[data-slot="alert-dialog-content"]')
      ).toBeNull()
    } finally {
      HTMLElement.prototype.getAnimations = original
    }
  })
})
