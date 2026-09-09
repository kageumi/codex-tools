import type { Command } from "@/lib/ipc"
import type {
  AccountQuota,
  ConfigPatchPreview,
  Dashboard,
  DeviceAuthPollResult,
  DeviceAuthorization,
  ModelUnlockStatus,
  OfficialAccountView,
  OfficialPricingCatalog,
  PricingRule,
  Provider,
  ProviderOverview,
  ProviderSaveInput,
  ProviderTestResult,
  ResetCredit,
  ResetCreditConsumeResult,
  ResetCreditDetails,
  RepairResult,
  RepairScan,
  Session,
  SettingsOverview,
  SupportDiagnostics,
  UsageOverview,
} from "@/types"

const mockEmptyConnections =
  typeof window !== "undefined" &&
  new URLSearchParams(window.location.search).has("empty-connections")
const mockDualQuota =
  typeof window !== "undefined" &&
  new URLSearchParams(window.location.search).has("dual-quota")

const now = Date.now()
const day = 86_400_000
const mockQuota: AccountQuota = {
  status: "success",
  fetchedAt: Math.floor(now / 1000),
  data: {
    kind: "windowed",
    primary: {
      usedPercent: mockDualQuota ? 42.5 : 23.9,
      remainingPercent: mockDualQuota ? 57.5 : 76.1,
      windowSeconds: mockDualQuota ? 18_000 : 604_800,
      resetAt: Math.floor(
        (now + (mockDualQuota ? 3 * 3_600_000 : 6 * day)) / 1000
      ),
    },
    ...(mockDualQuota && {
      secondary: {
        usedPercent: 68,
        remainingPercent: 32,
        windowSeconds: 604_800,
        resetAt: Math.floor((now + 5 * day) / 1000),
      },
    }),
  },
  resetCredits: { availableCount: 4, detailsStatus: "partial" },
}

const mockResetCredits: Record<string, ResetCredit[]> = {
  "account-work": [
    {
      id: "work-early",
      title: "优先重置卡",
      status: "available",
      expiresAt: Math.floor((now + day) / 1000),
      description: "演示可用重置卡",
    },
    {
      id: "work-no-expiry",
      resetType: "rate_limit",
      status: "available",
      expiresAt: undefined,
    },
    {
      id: "work-redeemed",
      title: "已使用卡",
      status: "redeemed",
      expiresAt: Math.floor((now + 2 * day) / 1000),
    },
    {
      id: "work-expired",
      title: "已过期卡",
      status: "expired",
      expiresAt: Math.floor((now - day) / 1000),
    },
  ],
  "account-personal": [
    { id: "personal-available", title: "个人重置卡", status: "available" },
  ],
}

const mockProviders: Provider[] = [
  {
    id: "provider-team",
    name: "团队网关",
    baseUrl: "https://api.team.example/v1",
    headers: {},
    timeoutSecs: 30,
    enabled: true,
    active: false,
    apiType: "responses",
    availableModels: ["deepseek-v4-pro", "gpt-5.6"],
    hasApiKey: true,
    createdAt: now - 30 * day,
    updatedAt: now - day,
  },
  {
    id: "provider-lab",
    name: "开发环境",
    baseUrl: "https://api.lab.example/v1",
    headers: {},
    timeoutSecs: 30,
    enabled: true,
    active: false,
    apiType: "chat",
    availableModels: ["qwen3-coder"],
    hasApiKey: true,
    createdAt: now - 12 * day,
    updatedAt: now - 2 * day,
  },
]

function mockModelsForProvider(provider: Provider) {
  if (provider.id === "provider-team") {
    return ["deepseek-v4-pro", "gpt-5.6"]
  }
  if (provider.id === "provider-lab") return ["qwen3-coder"]
  return provider.apiType === "chat" ? ["qwen3-coder"] : ["gpt-5.6"]
}

export function filterMockSelectedModels(
  selectedModels: string[] | null | undefined,
  availableModels: string[]
) {
  return selectedModels == null
    ? selectedModels
    : selectedModels.filter((model) => availableModels.includes(model))
}

function refreshMockProviderModels(provider: Provider) {
  const models = mockModelsForProvider(provider)
  provider.availableModels = [...models]
  const selected = filterMockSelectedModels(provider.selectedModels, models)
  provider.selectedModels = selected ?? undefined
  provider.updatedAt = Date.now()
  return [...models]
}

function activeMockProvider() {
  return mockProviders.find((candidate) => candidate.active)
}

function activeMockProviderModels() {
  return [...(activeMockProvider()?.availableModels ?? [])]
}

function activeMockAccount() {
  return mockAccounts.find((candidate) => candidate.active)
}

function mockActiveState(): {
  kind: "none" | "provider" | "official"
  providerId: string | null
  accountId: string | null
} {
  const provider = activeMockProvider()
  if (provider) {
    return { kind: "provider", providerId: provider.id, accountId: null }
  }
  const account = activeMockAccount()
  if (account) {
    return { kind: "official", providerId: null, accountId: account.id }
  }
  return { kind: "none", providerId: null, accountId: null }
}

function sameHeaders(
  left: Record<string, string>,
  right: Record<string, string>
) {
  const leftEntries = Object.entries(left).sort(([a], [b]) =>
    a.localeCompare(b)
  )
  const rightEntries = Object.entries(right).sort(([a], [b]) =>
    a.localeCompare(b)
  )
  return JSON.stringify(leftEntries) === JSON.stringify(rightEntries)
}

const mockAccounts: OfficialAccountView[] = [
  {
    id: "account-work",
    name: "工作账号",
    remark: "主力账号",
    accountId: "workspace",
    email: "work@example.com",
    source: "open_ai_oauth",
    expiresAt: null,
    credentialRefresh: { status: "healthy" },
    quota: mockQuota,
    active: true,
    createdAt: now - 60 * day,
    updatedAt: now,
  },
  {
    id: "account-personal",
    name: "个人账号",
    remark: "",
    accountId: "personal",
    email: "me@example.com",
    source: "open_ai_oauth",
    expiresAt: null,
    credentialRefresh: { status: "managed_by_codex" },
    quota: {
      status: "never",
      resetCredits: { availableCount: 1, detailsStatus: "complete" },
    },
    active: false,
    createdAt: now - 50 * day,
    updatedAt: now - 4 * day,
  },
]

const mockTrend = [28_000, 40_000, 31_000, 51_000, 34_000, 25_000, 38_000].map(
  (input, index) => ({
    dayStartMs: now - (6 - index) * day,
    tokens: {
      inputTokens: input,
      cachedInputTokens: 0,
      cacheWriteInputTokens: 0,
      outputTokens: Math.round(input * 0.42),
      reasoningOutputTokens: 0,
      totalTokens: Math.round(input * 1.42),
    },
    requests: 120 + index * 17,
    estimatedCostMicrousd: 310_000 + index * 32_000,
    unpricedTokens: 0,
    partialTokens: 0,
    unattributedTokens: 0,
  })
)

const mockUsage: UsageOverview = {
  range: { startAtMs: now - 7 * day, endAtMs: now + day },
  totals: {
    tokens: {
      inputTokens: 171_234,
      cachedInputTokens: 19_800,
      cacheWriteInputTokens: 4_100,
      outputTokens: 67_487,
      reasoningOutputTokens: 12_200,
      totalTokens: 238_721,
    },
    requests: 1_245,
    estimatedCostMicrousd: 3_423_000,
    subscriptionTokens: 0,
    unpricedTokens: 0,
    partialTokens: 0,
    unattributedTokens: 0,
  },
  rows: [
    {
      key: "gpt-5.6-work",
      model: "gpt-5.6",
      sourceKind: "official",
      accountId: "account-work",
      sourceName: "工作账号",
      tokens: {
        inputTokens: 121_234,
        cachedInputTokens: 19_800,
        cacheWriteInputTokens: 4_100,
        outputTokens: 47_487,
        reasoningOutputTokens: 10_200,
        totalTokens: 172_521,
      },
      requests: 830,
      estimatedCostMicrousd: 2_430_000,
      costStatus: "estimated",
      pricingRuleName: "OpenAI 官方参考价",
    },
    {
      key: "deepseek-team",
      model: "deepseek-v4-pro",
      sourceKind: "provider",
      providerId: "provider-team",
      sourceName: "团队网关",
      tokens: {
        inputTokens: 50_000,
        cachedInputTokens: 0,
        cacheWriteInputTokens: 0,
        outputTokens: 20_000,
        reasoningOutputTokens: 2_000,
        totalTokens: 66_200,
      },
      requests: 415,
      estimatedCostMicrousd: 993_000,
      costStatus: "estimated",
      pricingRuleName: "团队价格",
    },
  ],
  models: ["gpt-5.6-sol", "gpt-5.6-luna"],
  trendPoints: mockTrend,
  warnings: [],
  lastRefreshedAtMs: now,
  collectionStartedAtMs: now - 30 * day,
  collectionStartedVersion: "0.4.0",
}

const mockSessionTitles = [
  "重构账号切换流程",
  "实现用量图表",
  "排查模型同步问题",
  "发布检查",
]

const mockSessions: Session[] = Array.from({ length: 16 }, (_, index) => ({
  identity: `session-${index}`,
  id: `session-${index}`,
  title: mockSessionTitles[index % mockSessionTitles.length] ?? "未命名会话",
  provider: index % 3 === 0 ? "custom" : "openai",
  cwd: index % 2 === 0 ? "/Users/demo/project" : "/Users/demo/codex-tools",
  archived: false,
  updatedAt: now - index * 7_200_000,
  sourceDb: "state.sqlite",
  originalProvider: index % 3 === 0 ? "custom" : "openai",
  hasUserEvent: true,
}))

const mockRepair: RepairResult = {
  targetProvider: "openai",
  filesScanned: 8,
  filesCached: 0,
  filesOpened: 8,
  filesModified: 3,
  filesSkipped: 5,
  filesFailed: 0,
  sessionMetaUpdated: 3,
  rowsUpdated: 3,
  databasesScanned: 2,
  databasesUpdated: 2,
  warnings: [],
  repairComplete: true,
  verificationPassed: true,
  elapsedMs: 12,
}

export async function mockCall(
  command: Command,
  args: unknown
): Promise<unknown> {
  await new Promise((resolve) => globalThis.setTimeout(resolve, 120))
  switch (command) {
    case "dashboard_get": {
      const activeAccount = mockEmptyConnections
        ? undefined
        : mockAccounts.find((account) => account.active)
      const activeProvider = mockEmptyConnections
        ? undefined
        : mockProviders.find((provider) => provider.active)
      return {
        providerCount: mockEmptyConnections ? 0 : mockProviders.length,
        activeProvider: activeAccount
          ? `OpenAI · ${activeAccount.remark || activeAccount.name}`
          : activeProvider?.name,
        activeKind: activeAccount
          ? "official"
          : activeProvider
            ? "provider"
            : "none",
        activeAccountId: activeAccount?.id,
        activeAccount: activeAccount?.remark || activeAccount?.name,
        activeQuota: activeAccount?.quota,
        codexHome: "/Users/demo/.codex",
        databaseCount: 1,
        sessionCount: mockSessions.length,
        databaseHealth: "可以读取",
        todayUsage: mockUsage.totals.tokens,
        todayRequests: mockUsage.totals.requests,
        todayEstimatedCostMicrousd: mockUsage.totals.estimatedCostMicrousd,
        todaySubscriptionTokens: 0,
        todayUnpricedTokens: 0,
        todayPartialTokens: 0,
        todayUnattributedTokens: 0,
      } satisfies Dashboard
    }
    case "connections_list":
      if (mockEmptyConnections) {
        return { providers: [], officialAccounts: [] }
      }
      return structuredClone({
        providers: mockProviders,
        officialAccounts: mockAccounts,
      }) satisfies ProviderOverview
    case "usage_get_overview":
    case "usage_refresh":
      return mockUsage
    case "usage_get_trend":
      return { range: mockUsage.range, points: mockTrend }
    case "sessions_list": {
      const input = (args ?? {}) as {
        query?: string
        page?: number
        pageSize?: number
        status?: "active" | "archived"
      }
      const query = input.query?.toLowerCase() ?? ""
      const status = input.status ?? "active"
      const filtered = mockSessions.filter(
        (session) =>
          session.archived === (status === "archived") &&
          `${session.title} ${session.cwd}`.toLowerCase().includes(query)
      )
      const requestedPage = input.page ?? 1
      const pageSize = input.pageSize ?? 8
      const pageCount = Math.max(1, Math.ceil(filtered.length / pageSize))
      const page = Math.min(Math.max(1, requestedPage), pageCount)
      return {
        items: filtered.slice((page - 1) * pageSize, page * pageSize),
        total: filtered.length,
        page,
        pageSize,
      }
    }
    case "sessions_scan":
      return {
        currentProvider: "openai",
        targets: [
          { id: "openai", sources: ["openai"], current: true, count: 12 },
          { id: "custom", sources: ["custom"], current: false, count: 4 },
        ],
        rolloutFiles: 16,
        sessionMetaCount: 16,
        databases: [
          {
            path: "/Users/demo/.codex/state.sqlite",
            schema: "threads",
            threadCount: 16,
          },
        ],
        warnings: [],
      } satisfies RepairScan
    case "sessions_repair":
      return mockRepair
    case "connections_activate_official": {
      const account =
        activeMockAccount() ??
        [...mockAccounts].sort((a, b) => b.updatedAt - a.updatedAt)[0]
      if (!account) throw new Error("请先登录 OpenAI 账号")
      mockProviders.forEach((provider) => {
        provider.active = false
      })
      mockAccounts.forEach((candidate) => {
        candidate.active = candidate.id === account.id
      })
      return mockRepair
    }
    case "connections_activate": {
      const { id } = args as { id: string }
      mockProviders.forEach((provider) => {
        provider.active = provider.id === id
      })
      mockAccounts.forEach((account) => {
        account.active = false
      })
      return mockRepair
    }
    case "connections_activate_account": {
      const { id } = args as { id: string }
      mockAccounts.forEach((account) => {
        account.active = account.id === id
      })
      mockProviders.forEach((provider) => {
        provider.active = false
      })
      return mockRepair
    }
    case "settings_get_overview": {
      const provider = activeMockProvider()
      return {
        inspection: {
          path: "/Users/demo/.codex/config.toml",
          valid: true,
          activeProvider: provider?.id,
          managedProviderPresent: true,
          warnings: [],
        },
        diagnostics: {
          dataDirectory: "/Users/demo/Library/Application Support/Codex Tools",
          active: mockActiveState(),
        },
        canPreviewCustom: Boolean(provider),
      } satisfies SettingsOverview
    }
    case "settings_get_diagnostics": {
      const provider = activeMockProvider()
      const account = activeMockAccount()
      const activeState = mockActiveState()
      return {
        schemaVersion: 1,
        generatedAt: new Date(now).toISOString(),
        app: { name: "Codex Tools", version: "0.4.0", buildProfile: "debug" },
        system: { os: "macos", architecture: "aarch64", family: "unix" },
        paths: {
          dataDirectory: "~/Library/Application Support/Codex Tools",
          codexHome: "~/.codex",
          configFile: "~/.codex/config.toml",
        },
        configuration: {
          valid: true,
          activeProvider: provider?.id,
          managedProviderPresent: true,
          warnings: [],
        },
        connection: {
          activeKind: activeState.kind,
          providerCount: 2,
          officialAccountCount: 2,
          activeModel:
            provider?.availableModels?.[0] ?? (account ? "gpt-5.6" : undefined),
        },
        storage: {
          files: [
            { name: "app.json", exists: true, readable: true, sizeBytes: 512 },
            {
              name: "connections.json",
              exists: true,
              readable: true,
              sizeBytes: 2048,
            },
            {
              name: "credentials.json",
              exists: true,
              readable: true,
              sizeBytes: 1024,
            },
          ],
          usageDatabase: {
            exists: true,
            sizeBytes: 65536,
            schemaVersion: 6,
            quickCheck: "ok",
            eventCount: 1245,
            cursorCount: 16,
          },
          sessionDatabaseCount: 1,
          indexedSessionCount: 16,
        },
        network: {
          environmentProxyConfigured: false,
          noProxyConfigured: false,
          systemProxyConfigured: false,
          tlsBackend: "rustls",
        },
        warnings: [],
        privacy: {
          homePathsRedacted: true,
          omitted: [
            "API keys",
            "OAuth and Cookie tokens",
            "custom header values",
            "account identifiers and email addresses",
            "proxy addresses",
          ],
        },
      } satisfies SupportDiagnostics
    }
    case "settings_get_codex_app":
      return { detected: "/Applications/Codex.app" }
    case "settings_model_unlock_status": {
      const models = activeMockProviderModels()
      return {
        appFound: true,
        appRunning: true,
        debugPort: 9222,
        injected: models.length > 0,
        modelCount: models.length,
        models,
      } satisfies ModelUnlockStatus
    }
    case "settings_preview_activation": {
      const provider = activeMockProvider()
      const model = provider?.availableModels?.[0]
      if (!provider || !model) {
        throw new Error("请先激活并同步一个包含可用模型的 API 服务。")
      }
      return {
        operationId: "mock-preview",
        targetPath: "/Users/demo/.codex/config.toml",
        baseHash: "mock",
        rendered: `model_provider = "custom"\nmodel = "${model}"`,
        changes: ["切换 model_provider", `使用服务 API 返回的模型 ${model}`],
        apiKeyMasked: "sk-••••••••",
      } satisfies ConfigPatchPreview
    }
    case "connections_test_provider":
      return {
        ok: true,
        status: 200,
        endpoint: "https://api.team.example/v1/models",
        message: "模型列表接口可以访问。",
        suggestV1: false,
      } satisfies ProviderTestResult
    case "connections_list_models": {
      const { id } = args as { id: string }
      const provider = mockProviders.find((candidate) => candidate.id === id)
      if (!provider) throw new Error(`API 服务不存在：${id}`)
      if (!provider.hasApiKey) throw new Error("此服务还没有 API Key。")
      return refreshMockProviderModels(provider)
    }
    case "connections_refresh_models": {
      const provider = activeMockProvider()
      if (!provider) throw new Error("当前不是第三方 API 服务。")
      if (!provider.hasApiKey) throw new Error("此服务还没有 API Key。")
      return refreshMockProviderModels(provider)
    }
    case "connections_refresh_quota": {
      const { accountId } = args as { accountId: string }
      const account = mockAccounts.find(
        (candidate) => candidate.id === accountId
      )
      const refreshedQuota = {
        ...mockQuota,
        fetchedAt: Math.floor(Date.now() / 1000),
      }
      if (account) account.quota = refreshedQuota
      return refreshedQuota
    }
    case "connections_get_reset_credits": {
      const { accountId } = args as { accountId: string }
      const account = mockAccounts.find(
        (candidate) => candidate.id === accountId
      )
      if (!account) throw new Error(`账号不存在：${accountId}`)
      const credits = [...(mockResetCredits[accountId] ?? [])]
      const summary = account.quota.resetCredits ?? {
        availableCount: undefined,
        detailsStatus: "unknown" as const,
      }
      return { accountId, summary, credits } satisfies ResetCreditDetails
    }
    case "connections_consume_reset_credit": {
      const { accountId, creditId } = args as {
        accountId: string
        creditId: string
        idempotencyKey: string
      }
      const account = mockAccounts.find(
        (candidate) => candidate.id === accountId
      )
      const credit = mockResetCredits[accountId]?.find(
        (candidate) => candidate.id === creditId
      )
      if (!account || !credit || credit.status !== "available") {
        throw new Error("重置卡不可用或不属于该账号。")
      }
      credit.status = "redeemed"
      const previous = account.quota.resetCredits?.availableCount
      const resetCredits = {
        availableCount:
          previous == null ? undefined : Math.max(0, previous - 1),
        detailsStatus: "partial" as const,
      }
      account.quota = {
        ...account.quota,
        estimates: [],
        resetCredits,
      }
      const details = {
        accountId,
        summary: resetCredits,
        credits: [...(mockResetCredits[accountId] ?? [])],
      } satisfies ResetCreditDetails
      return {
        outcome: "reset",
        details,
        quota: account.quota,
      } satisfies ResetCreditConsumeResult
    }
    case "connections_estimate_quota": {
      const { accountId } = args as { accountId: string }
      const account = mockAccounts.find(
        (candidate) => candidate.id === accountId
      )
      if (!account) throw new Error(`账号不存在：${accountId}`)
      // 与真实命令一致：估算操作自行取得当前额度快照，不依赖用户先点“刷新额度”。
      account.quota = {
        ...mockQuota,
        fetchedAt: Math.floor(Date.now() / 1000),
        estimates: [],
      }
      const windows: import("@/types").QuotaEstimateWindowResult[] = []
      for (const [slot, window] of Object.entries({
        primary: account.quota.data?.primary,
        secondary: account.quota.data?.secondary,
      })) {
        if (!window?.resetAt) continue
        const windowSeconds =
          window.windowSeconds ?? (slot === "primary" ? 18_000 : 604_800)
        if (window.usedPercent < 10) {
          windows.push({
            windowSeconds,
            resetAt: window.resetAt,
            success: false,
            reason: "额度已用比例低于 10%，样本不足，暂不估算。",
          })
          continue
        }
        const estimate = {
          windowSeconds,
          resetAt: window.resetAt,
          estimatedTotalMicrousd: 2_400_000,
          estimatedAt: Math.floor(Date.now() / 1000),
        }
        account.quota.estimates = [
          ...(account.quota.estimates ?? []).filter(
            (item) =>
              item.windowSeconds !== windowSeconds ||
              item.resetAt !== window.resetAt
          ),
          estimate,
        ]
        windows.push({
          windowSeconds,
          resetAt: window.resetAt,
          success: true,
          estimate,
        })
      }
      return { windows }
    }
    case "connections_refresh_login": {
      const { id } = args as { id: string }
      const account = mockAccounts.find((candidate) => candidate.id === id)
      if (!account) throw new Error(`账号不存在：${id}`)
      account.updatedAt = Date.now()
      account.credentialRefresh = {
        status: "healthy",
        lastAttemptAt: Math.floor(Date.now() / 1000),
        lastSuccessAt: Math.floor(Date.now() / 1000),
        lastRefreshAt: Math.floor(Date.now() / 1000),
        lastCheckAt: Math.floor(Date.now() / 1000),
        verification: "valid",
      }
      return { account, outcome: "refreshed" }
    }
    case "connections_update_account_remark": {
      const { id, remark } = args as { id: string; remark: string }
      const account = mockAccounts.find((candidate) => candidate.id === id)
      if (!account) throw new Error(`账号不存在：${id}`)
      account.remark = remark.trim()
      account.updatedAt = Date.now()
      return account
    }
    case "connections_update_account_remarks": {
      const { updates } = args as {
        updates: Array<{ id: string; remark: string }>
      }
      const accounts = updates.map(({ id, remark }) => {
        const account = mockAccounts.find((candidate) => candidate.id === id)
        if (!account) throw new Error(`账号不存在：${id}`)
        return { account, remark: remark.trim() }
      })
      const updatedAt = Date.now()
      accounts.forEach(({ account, remark }) => {
        account.remark = remark
        account.updatedAt = updatedAt
      })
      return accounts.map(({ account }) => account)
    }
    case "connections_delete_account": {
      const { id } = args as { id: string }
      const index = mockAccounts.findIndex((account) => account.id === id)
      if (index >= 0) mockAccounts.splice(index, 1)
      return undefined
    }
    case "connections_delete_accounts": {
      const { ids } = args as { ids: string[] }
      const uniqueIds = new Set(ids)
      const accounts = [...uniqueIds].map((id) => {
        const account = mockAccounts.find((candidate) => candidate.id === id)
        if (!account) throw new Error(`账号不存在：${id}`)
        return account
      })
      if (accounts.some((account) => account.active)) {
        throw new Error("正在使用所选账号，请先切换连接。")
      }
      for (let index = mockAccounts.length - 1; index >= 0; index -= 1) {
        const account = mockAccounts[index]
        if (account && uniqueIds.has(account.id)) mockAccounts.splice(index, 1)
      }
      return undefined
    }
    case "connections_delete_provider": {
      const { id } = args as { id: string }
      const index = mockProviders.findIndex((provider) => provider.id === id)
      if (index >= 0) mockProviders.splice(index, 1)
      return undefined
    }
    case "connections_refresh_all_quota":
      return mockAccounts.map((account) => ({
        accountId: account.id,
        quota: mockQuota,
      }))
    case "connections_save_provider": {
      const provider = (args as { provider: ProviderSaveInput }).provider
      const existingIndex = mockProviders.findIndex(
        (candidate) => candidate.id === provider.id
      )
      const existing =
        existingIndex >= 0 ? mockProviders[existingIndex] : undefined
      const hasApiKey =
        Boolean(provider.apiKey?.trim()) || Boolean(existing?.hasApiKey)
      const shouldRefreshModels =
        !existing ||
        existing.name !== provider.name ||
        existing.baseUrl !== provider.baseUrl ||
        existing.apiType !== provider.apiType ||
        !sameHeaders(existing.headers, provider.headers) ||
        Boolean(provider.apiKey?.trim())
      const selectedModels =
        provider.selectedModels === undefined &&
        existing &&
        !shouldRefreshModels
          ? existing.selectedModels
          : (provider.selectedModels ?? undefined)
      const saved: Provider = {
        ...existing,
        id: provider.id || `provider-${Date.now()}`,
        name: provider.name,
        baseUrl: provider.baseUrl,
        headers: { ...provider.headers },
        timeoutSecs: provider.timeoutSecs,
        enabled: provider.enabled,
        active: existing?.active ?? false,
        apiType: provider.apiType,
        apiKey: undefined,
        hasApiKey,
        availableModels: shouldRefreshModels
          ? []
          : [...(existing?.availableModels ?? [])],
        customModels: [
          ...(provider.customModels ?? existing?.customModels ?? []),
        ],
        selectedModels:
          selectedModels === undefined ? undefined : [...selectedModels],
        contextWindowOverride: provider.contextWindowOverride ?? undefined,
        createdAt: existing?.createdAt ?? Date.now(),
        updatedAt: Date.now(),
      }
      if (hasApiKey && shouldRefreshModels) {
        refreshMockProviderModels(saved)
      }
      if (existingIndex >= 0) mockProviders[existingIndex] = saved
      else mockProviders.push(saved)
      return structuredClone(saved)
    }
    case "connections_import_cookie": {
      const input = args as {
        name?: string
        accountId?: string
        content?: string
      }
      const id = `account-cookie-${Date.now()}`
      const account: OfficialAccountView = {
        id,
        name: input.name?.trim() || "Cookie 账号",
        remark: "",
        accountId: input.accountId?.trim() || id,
        email: "",
        source: "proxy_import",
        expiresAt: null,
        credentialRefresh: { status: "not_refreshable" },
        quota: { status: "never" },
        active: false,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      }
      mockAccounts.push(account)
      return { accounts: [account], detectedFormats: ["Cookie 登录数据"] }
    }
    case "connections_login_start":
      return {
        operationId: "mock-login",
        userCode: "ABCD-EFGH",
        verificationUri: "https://auth.openai.com/codex/device",
        expiresAt: Math.floor((now + 15 * 60_000) / 1000),
        intervalSecs: 5,
      } satisfies DeviceAuthorization
    case "connections_login_poll":
      return { status: "pending" } satisfies DeviceAuthPollResult
    case "usage_get_official_pricing":
    case "usage_refresh_official_pricing":
      return {
        status: "cached",
        sourceUrl: "https://openai.com/api/pricing",
        fetchedAtMs: now,
        modelCount: 1,
        models: ["gpt-5.6"],
        rates: [],
      } satisfies OfficialPricingCatalog
    case "usage_list_pricing_rules":
      return []
    case "usage_save_pricing_rule":
      return (args as { input: PricingRule }).input
    case "settings_save_codex_app_path":
    case "settings_apply_activation":
    case "connections_open_login_page":
    case "usage_delete_pricing_rule":
      return undefined
    case "usage_reprice":
      return {
        eventsRepriced: 2,
        estimatedCostMicrousd: 3_423_000,
        unpricedEvents: 0,
      }
    case "dashboard_launch":
    case "settings_unlock_models":
    case "settings_launch_codex_debug": {
      const models = activeMockProviderModels()
      return {
        port: 9222,
        injected: models.length > 0,
        modelCount: models.length,
        message: "Codex 已启动",
      }
    }
    default: {
      command satisfies never
      throw new Error(`未实现的 mock IPC 命令：${String(command)}`)
    }
  }
}
