import { afterEach, describe, expect, it, vi } from "vitest"

import type { OfficialAccountView, Provider, RepairResult } from "@/types"

vi.mock("@/lib/ipc", () => ({ call: vi.fn() }))

import {
  addCustomModelTo,
  allModelsSelected,
  accountDescription,
  accountWorkspaceIsDeactivated,
  buildFallbackCandidates,
  credentialMaintenanceMessage,
  credentialRefreshText,
  loginVerificationText,
  effectiveModelsOf,
  contextWindowOverrideInputOf,
  contextWindowOverrideEditorSessionOf,
  isValidContextWindowOverride,
  noModelsSelected,
  providerSaveInputOf,
  removeCustomModelFrom,
  repairWarning,
  quotaStatusText,
  switchActiveConnection,
  toggleModelSelected,
} from "./connection-utils"
import { call } from "@/lib/ipc"

const makeAccount = (
  overrides: Partial<OfficialAccountView> = {}
): OfficialAccountView => ({
  id: "a1",
  name: "账号",
  remark: "",
  accountId: "workspace-1",
  email: "account@example.com",
  source: "open_ai_oauth",
  expiresAt: null,
  credentialRefresh: { status: "unknown" },
  quota: { status: "never" },
  active: false,
  createdAt: 0,
  updatedAt: 0,
  ...overrides,
})

const makeQuotaWindow = (
  windowSeconds: number,
  remainingPercent: number,
  resetAt?: number
) => ({
  usedPercent: 100 - remainingPercent,
  remainingPercent,
  windowSeconds,
  ...(resetAt === undefined ? {} : { resetAt }),
})

const makeWindowedQuota = (
  primary?: ReturnType<typeof makeQuotaWindow>,
  secondary?: ReturnType<typeof makeQuotaWindow>
): OfficialAccountView["quota"] => ({
  status: "success",
  data: { kind: "windowed", primary, secondary },
})

const makeProvider = (overrides: Partial<Provider> = {}): Provider => ({
  id: "p1",
  name: "服务",
  baseUrl: "https://api.example.com/v1",
  headers: {},
  timeoutSecs: 30,
  enabled: true,
  active: false,
  apiType: "responses",
  createdAt: 0,
  updatedAt: 0,
  ...overrides,
})

const repair = (overrides: Partial<RepairResult> = {}): RepairResult => ({
  targetProvider: "openai",
  filesScanned: 0,
  filesCached: 0,
  filesOpened: 0,
  filesModified: 0,
  filesSkipped: 0,
  filesFailed: 0,
  sessionMetaUpdated: 0,
  rowsUpdated: 0,
  databasesScanned: 0,
  databasesUpdated: 0,
  warnings: [],
  repairComplete: true,
  verificationPassed: true,
  elapsedMs: 0,
  ...overrides,
})

describe("providerSaveInputOf", () => {
  it("将默认全选明确序列化为 null", () => {
    const input = providerSaveInputOf(
      makeProvider({
        availableModels: ["a", "b"],
        selectedModels: undefined,
      })
    )
    expect(input.selectedModels).toBeNull()
    expect(input.customModels).toEqual([])
    expect(input.contextWindowOverride).toBeNull()
  })

  it("保留显式模型子集", () => {
    expect(
      providerSaveInputOf(
        makeProvider({
          availableModels: ["a", "b"],
          selectedModels: ["b"],
        })
      ).selectedModels
    ).toEqual(["b"])
  })

  it("保留服务级上下文窗口覆盖", () => {
    expect(
      providerSaveInputOf(makeProvider({ contextWindowOverride: 262_144 }))
        .contextWindowOverride
    ).toBe(262_144)
  })
})

describe("contextWindowOverrideInputOf", () => {
  it("留空恢复自动匹配，正整数转换为 token", () => {
    expect(contextWindowOverrideInputOf("  ")).toBeNull()
    expect(contextWindowOverrideInputOf("262144")).toBe(262_144)
  })

  it("拒绝零、负数、小数和超过安全整数的输入", () => {
    expect(isValidContextWindowOverride("0")).toBe(false)
    expect(isValidContextWindowOverride("-1")).toBe(false)
    expect(isValidContextWindowOverride("1.5")).toBe(false)
    expect(isValidContextWindowOverride("9007199254740992")).toBe(false)
    expect(isValidContextWindowOverride("128000")).toBe(true)
  })

  it("同一 API 重新打开编辑器时创建新会话并从已保存值回填", () => {
    const provider = makeProvider({
      id: "same-provider",
      contextWindowOverride: 262_144,
    })
    expect(contextWindowOverrideEditorSessionOf(provider, false)).not.toBe(
      contextWindowOverrideEditorSessionOf(provider, true)
    )
    expect(contextWindowOverrideInputOf("262144")).toBe(
      provider.contextWindowOverride
    )
  })
})

describe("repairWarning", () => {
  it("stays silent after a complete repair", () => {
    expect(repairWarning(repair())).toBeUndefined()
  })

  it("reports a rolled-back switch when repair did not complete", () => {
    expect(repairWarning(repair({ repairComplete: false }))).toContain(
      "切换已回滚：会话归属修复未完成，切换已回滚"
    )
  })

  it("surfaces partial repair failures and backend warnings", () => {
    expect(
      repairWarning(
        repair({ filesFailed: 2, warnings: ["数据库被占用", "索引刷新失败"] })
      )
    ).toContain("2 个会话文件修复失败；数据库被占用；索引刷新失败")
  })
})

describe("switchActiveConnection", () => {
  afterEach(() => {
    vi.mocked(call).mockReset()
  })

  it("switches to the first candidate whose repair completes", async () => {
    vi.mocked(call)
      .mockResolvedValueOnce(repair({ repairComplete: true }))
      .mockResolvedValueOnce(repair({ repairComplete: true }))
    const result = await switchActiveConnection([
      { kind: "provider", id: "p1" },
      { kind: "account", id: "a1" },
    ])
    expect(result.switchedId).toBe("p1")
    expect(result.repair?.repairComplete).toBe(true)
    expect(call).toHaveBeenCalledTimes(1)
  })

  it("treats an incomplete repair as a failed candidate and tries the next", async () => {
    vi.mocked(call)
      .mockResolvedValueOnce(
        repair({ repairComplete: false, warnings: ["数据库被占用"] })
      )
      .mockResolvedValueOnce(repair({ repairComplete: true }))
    const result = await switchActiveConnection([
      { kind: "account", id: "a1" },
      { kind: "provider", id: "p2" },
    ])
    expect(result.switchedId).toBe("p2")
    expect(call).toHaveBeenCalledTimes(2)
  })

  it("returns the last error when every candidate fails to repair", async () => {
    vi.mocked(call).mockResolvedValue(
      repair({ repairComplete: false, warnings: ["会话文件被占用"] })
    )
    const result = await switchActiveConnection([
      { kind: "account", id: "a1" },
      { kind: "provider", id: "p2" },
    ])
    expect(result.switchedId).toBeUndefined()
    expect(result.repair).toBeUndefined()
    expect(String(result.error)).toContain("切换已回滚")
  })
})

describe("账号额度错误", () => {
  it("优先展示后端给出的具体 HTTP 错误说明", () => {
    const account = makeAccount({
      quota: {
        status: "rate_limited",
        error: "OpenAI 请求过于频繁（HTTP 429：触发速率限制），请稍后重试",
      },
    })
    expect(quotaStatusText(account)).toContain("HTTP 429：触发速率限制")
  })

  it("识别已停用工作区并从自动切换候选中排除", () => {
    const deactivated = makeAccount({
      id: "deactivated",
      quota: {
        status: "unauthorized",
        errorCode: "deactivated_workspace",
        error: "账号所属工作区已停用（HTTP 402）",
      },
    })
    const usable = makeAccount({ id: "usable" })

    expect(accountWorkspaceIsDeactivated(deactivated)).toBe(true)
    expect(
      buildFallbackCandidates([deactivated, usable], []).map(
        (candidate) => candidate.id
      )
    ).toEqual(["usable"])
  })
})

describe("账号额度摘要", () => {
  it.each([
    {
      name: "双窗口按 5H 后 7D 排序",
      quota: makeWindowedQuota(
        makeQuotaWindow(604_800, 20, 1_800_604_800),
        makeQuotaWindow(18_000, 80, 1_800_018_000)
      ),
      expected: ["5H 剩余 80.0% · 重置", "7D 剩余 20.0% · 重置"],
    },
    {
      name: "仅 5H",
      quota: makeWindowedQuota(makeQuotaWindow(18_000, 75, 1_800_018_000)),
      expected: ["5H 剩余 75.0% · 重置"],
      absent: ["7D"],
    },
    {
      name: "仅 7D",
      quota: makeWindowedQuota(
        undefined,
        makeQuotaWindow(604_800, 65, 1_800_604_800)
      ),
      expected: ["7D 剩余 65.0% · 重置"],
      absent: ["5H"],
    },
    {
      name: "保留 0.0%",
      quota: makeWindowedQuota(makeQuotaWindow(18_000, 0, 1_800_018_000)),
      expected: ["5H 剩余 0.0% · 重置"],
    },
    {
      name: "缺少 resetAt",
      quota: makeWindowedQuota(makeQuotaWindow(18_000, 50)),
      expected: ["5H 剩余 50.0% · 重置 —"],
    },
    {
      name: "无窗口 never 状态",
      quota: { status: "never" },
      expected: ["尚未刷新"],
    },
    {
      name: "错误状态保留后端错误",
      quota: {
        status: "error",
        error: "额度接口暂时不可用（HTTP 500）",
      },
      expected: ["额度接口暂时不可用（HTTP 500）"],
    },
  ] as const)("$name", ({ quota, expected, absent = [] }) => {
    const summary = accountDescription(makeAccount({ quota }))
    const indexes = expected.map((fragment) => summary.indexOf(fragment))

    expect(indexes.every((index) => index >= 0)).toBe(true)
    for (let index = 1; index < indexes.length; index += 1) {
      expect(indexes[index - 1]!).toBeLessThan(indexes[index]!)
    }
    for (const fragment of absent) {
      expect(summary).not.toContain(fragment)
    }
  })
})

describe("凭据自动维护状态", () => {
  it("只显示脱敏维护结果", () => {
    expect(
      credentialRefreshText(
        makeAccount({ credentialRefresh: { status: "waiting_retry" } })
      )
    ).toBe("等待重试")
  })

  it("旧 healthy 没有真实刷新时间时不冒充自动刷新正常", () => {
    expect(
      credentialRefreshText(
        makeAccount({ credentialRefresh: { status: "healthy" } })
      )
    ).toBe("未到刷新时间")
    expect(
      credentialRefreshText(
        makeAccount({
          credentialRefresh: { status: "healthy", lastRefreshAt: 1 },
        })
      )
    ).toBe("自动刷新正常")
  })

  it("按实际 outcome 显示检查结果而非一律声称已更新", () => {
    expect(
      credentialMaintenanceMessage({
        account: makeAccount({ credentialRefresh: { status: "healthy" } }),
        outcome: "synced_from_codex",
      })
    ).toBe("已同步 Codex 最新凭据")
    expect(
      credentialMaintenanceMessage({
        account: makeAccount({
          credentialRefresh: { status: "reauthentication_required" },
        }),
        outcome: "unchanged",
      })
    ).toBe("需要重新登录")
  })

  it("不可刷新时不误报未到刷新时间", () => {
    expect(
      credentialMaintenanceMessage({
        account: makeAccount({
          credentialRefresh: { status: "not_refreshable" },
        }),
        outcome: "not_refreshable",
      })
    ).toBe("不可自动刷新：缺少可刷新凭据")
  })

  it("不把本地维护成功误显示为在线登录有效", () => {
    expect(
      loginVerificationText(
        makeAccount({ credentialRefresh: { status: "healthy" } })
      )
    ).toBe("尚未在线验证")
    expect(
      loginVerificationText(
        makeAccount({
          credentialRefresh: { status: "healthy", verification: "valid" },
        })
      )
    ).toBe("在线验证有效")
  })
})

describe("effectiveModelsOf", () => {
  it("合并同步模型与自定义模型并保持顺序去重", () => {
    const provider = makeProvider({
      availableModels: ["gpt-4o", "gpt-4o-mini"],
      customModels: ["gpt-4o", "my-model"],
    })
    expect(effectiveModelsOf(provider)).toEqual([
      "gpt-4o",
      "gpt-4o-mini",
      "my-model",
    ])
  })
})

describe("allModelsSelected / noModelsSelected", () => {
  it("undefined（未设置）视为全选", () => {
    const provider = makeProvider({
      availableModels: ["a", "b"],
      selectedModels: undefined,
    })
    expect(allModelsSelected(provider)).toBe(true)
    expect(noModelsSelected(provider)).toBe(false)
  })

  it("空数组不是全选而是无选中", () => {
    const provider = makeProvider({
      availableModels: ["a", "b"],
      selectedModels: [],
    })
    expect(allModelsSelected(provider)).toBe(false)
    expect(noModelsSelected(provider)).toBe(true)
  })

  it("选中全部时视为全选", () => {
    const provider = makeProvider({
      availableModels: ["a", "b"],
      selectedModels: ["a", "b"],
    })
    expect(allModelsSelected(provider)).toBe(true)
    expect(noModelsSelected(provider)).toBe(false)
  })
})

describe("null 处理（后端 Option::None 序列化为 null）", () => {
  it("null 等同于 undefined（全选）", () => {
    const provider = makeProvider({
      availableModels: ["a", "b"],
      selectedModels: null as unknown as string[] | undefined,
    })
    expect(allModelsSelected(provider)).toBe(true)
    expect(noModelsSelected(provider)).toBe(false)
  })

  it("null 时 toggleModelSelected 基于有效模型全选计算", () => {
    const provider = makeProvider({
      availableModels: ["a", "b"],
      selectedModels: null as unknown as string[] | undefined,
    })
    // 取消 a，应返回仅含 b 的数组
    expect(toggleModelSelected(provider, "a", false)).toEqual(["b"])
  })

  it("null 时 addCustomModelTo 保持全选语义", () => {
    const provider = makeProvider({
      availableModels: ["a", "b"],
      selectedModels: null as unknown as string[] | undefined,
    })
    const next = addCustomModelTo(provider, "custom")
    expect(next.customModels).toEqual(["custom"])
    expect(next.selectedModels).toBeUndefined()
  })
})

describe("toggleModelSelected", () => {
  it("未设置 selectedModels 时首次取消返回空数组（BUG1 单次反选）", () => {
    const provider = makeProvider({
      availableModels: ["a", "b", "c"],
      selectedModels: undefined,
    })
    const next = toggleModelSelected(provider, "a", false)
    expect(next).toEqual(["b", "c"])
  })

  it("取消后重新勾选返回 undefined（全选语义）", () => {
    const provider = makeProvider({
      availableModels: ["a", "b", "c"],
      selectedModels: ["b", "c"],
    })
    expect(toggleModelSelected(provider, "a", true)).toBeUndefined()
  })

  it("连续基于最新状态切换多个模型正确（BUG2 回归）", () => {
    let provider = makeProvider({
      availableModels: ["a", "b", "c", "d"],
      selectedModels: undefined,
    })
    // 第一次取消 a
    provider = {
      ...provider,
      selectedModels: toggleModelSelected(provider, "a", false),
    }
    expect(provider.selectedModels).toEqual(["b", "c", "d"])
    // 第二次取消 c（模拟搜索词改变后再切换第二个模型）
    provider = {
      ...provider,
      selectedModels: toggleModelSelected(provider, "c", false),
    }
    expect(provider.selectedModels).toEqual(["b", "d"])
  })
})

describe("addCustomModelTo", () => {
  it("在非全选态添加时保留 customModels 与 selectedModels（回归双 update bug）", () => {
    let provider = makeProvider({
      availableModels: ["a", "b"],
      selectedModels: ["b"],
    })
    provider = addCustomModelTo(provider, "my-model")
    expect(provider.customModels).toEqual(["my-model"])
    expect(provider.selectedModels).toEqual(["b", "my-model"])
    expect(effectiveModelsOf(provider)).toEqual(["a", "b", "my-model"])
  })

  it("全选态（undefined）添加时保持默认全选语义", () => {
    const provider = makeProvider({
      availableModels: ["a", "b"],
      selectedModels: undefined,
    })
    const next = addCustomModelTo(provider, "my-model")
    expect(next.customModels).toEqual(["my-model"])
    expect(next.selectedModels).toBeUndefined()
  })
})

describe("removeCustomModelFrom", () => {
  it("移除时从 selectedModels 剔除并回退 undefined", () => {
    let provider = makeProvider({
      availableModels: ["a", "b"],
      customModels: ["my-model"],
      selectedModels: ["a", "b", "my-model"],
    })
    provider = removeCustomModelFrom(provider, "my-model")
    expect(provider.customModels).toEqual([])
    // 剔除后选中 a+b = 剩余全部有效模型，回退 undefined（全选）。
    expect(provider.selectedModels).toBeUndefined()
  })

  it("移除后若还有未选模型则保留 selectedModels", () => {
    let provider = makeProvider({
      availableModels: ["a", "b", "c"],
      customModels: ["my-model"],
      selectedModels: ["b", "my-model"],
    })
    provider = removeCustomModelFrom(provider, "my-model")
    expect(provider.customModels).toEqual([])
    expect(provider.selectedModels).toEqual(["b"])
  })
})
