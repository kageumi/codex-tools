export type Page = "dashboard" | "providers" | "usage" | "sessions" | "settings"

export type SettingsSection = "config" | "diagnostics" | "app" | "unlock"

export type Provider = {
  id: string
  name: string
  baseUrl: string
  headers: Record<string, string>
  timeoutSecs: number
  enabled: boolean
  active: boolean
  /** 服务接入方式：直连 Responses API，或经本机转换代理接入 Chat Completions API */
  apiType: "responses" | "chat"
  /** 从服务 /models 接口读取的模型上下文窗口（token），写入模型目录时优先使用 */
  modelContextWindows?: Record<string, number>
  /** 当前服务全部模型统一使用的手动上下文窗口；未设置时自动匹配。 */
  contextWindowOverride?: number | null
  /** 服务 /models 接口返回的可用模型列表（保存服务时静默获取） */
  availableModels?: string[]
  /** 用户手动添加的自定义模型 id；与 availableModels 一起构成有效模型 */
  customModels?: string[]
  /** 写入 Codex 的模型列表；未设置时默认使用全部可用模型 */
  selectedModels?: string[]
  /** models.dev（models.json）匹配的模型元数据（slug → 元数据） */
  modelsDevMeta?: Record<string, ProviderModelsDevMeta>
  apiKey?: string
  hasApiKey?: boolean
  createdAt: number
  updatedAt: number
}

export type ProviderSaveInput = Pick<
  Provider,
  | "id"
  | "name"
  | "baseUrl"
  | "headers"
  | "timeoutSecs"
  | "enabled"
  | "apiType"
  | "customModels"
  | "apiKey"
> & {
  /** null 表示清除旧筛选并默认使用全部有效模型。 */
  selectedModels: string[] | null
  /** null 表示清除手动覆盖并恢复自动上下文窗口匹配。 */
  contextWindowOverride?: number | null
}

export type Dashboard = {
  providerCount: number
  activeProvider?: string
  activeKind: "none" | "provider" | "official"
  activeAccountId?: string
  activeAccount?: string
  activeModel?: string
  activeQuota?: AccountQuota
  codexHome: string
  databaseCount: number
  sessionCount: number
  databaseHealth: string
  todayUsage: TokenBreakdown
  todayRequests: number
  todayEstimatedCostMicrousd: number
  todaySubscriptionTokens: number
  todayUnpricedTokens: number
  todayPartialTokens: number
  todayUnattributedTokens: number
}

export type UsageSourceKind = "official" | "provider" | "unattributed"

export type UsageGroupBy = "model" | "account"

export type CostStatus =
  | "estimated"
  | "subscription"
  | "unpriced"
  | "partial"
  | "unattributed"
  | "zero"

export type UsageRange = {
  startAtMs: number
  endAtMs: number
}

export type UsageQuery = {
  range: UsageRange
  groupBy: UsageGroupBy
}

export type TokenBreakdown = {
  inputTokens: number
  cachedInputTokens: number
  cacheWriteInputTokens: number
  outputTokens: number
  reasoningOutputTokens: number
  totalTokens: number
}

export type UsageRow = {
  key: string
  model: string
  sourceKind: UsageSourceKind
  providerId?: string
  accountId?: string
  sourceName: string
  tokens: TokenBreakdown
  requests: number
  estimatedCostMicrousd?: number
  costStatus: CostStatus
  pricingRuleName?: string
  pricingRuleVersion?: number
}

export type UsageOverview = {
  range: UsageRange
  totals: {
    tokens: TokenBreakdown
    requests: number
    estimatedCostMicrousd: number
    subscriptionTokens: number
    unpricedTokens: number
    partialTokens: number
    unattributedTokens: number
  }
  rows: UsageRow[]
  models: string[]
  lastRefreshedAtMs?: number
  collectionStartedAtMs?: number
  collectionStartedVersion?: string
  warnings: Array<{ path?: string; message: string }>
  /** 与 totals 同一趟查询产出的趋势点，避免对同一范围重复全量扫描。 */
  trendPoints: UsageTrendPoint[]
}

export type UsageTrend = {
  range: UsageRange
  points: UsageTrendPoint[]
}

export type UsageTrendPoint = {
  dayStartMs: number
  tokens: TokenBreakdown
  requests: number
  estimatedCostMicrousd: number
  unpricedTokens: number
  partialTokens: number
  unattributedTokens: number
}

export type OfficialPricingCatalog = {
  status: "waiting" | "cached"
  sourceUrl: string
  version?: number
  contentSha256?: string
  fetchedAtMs?: number
  etag?: string
  modelCount: number
  models: string[]
  rates: OfficialModelRate[]
}

export type OfficialModelRate = {
  model: string
  longContextThreshold?: number
  short: TokenRates
  long?: TokenRates
}

export type TokenRates = {
  input?: number
  cachedInput?: number
  cacheWrite?: number
  output?: number
}

export type RepriceResult = {
  eventsRepriced: number
  estimatedCostMicrousd: number
  unpricedEvents: number
}

export type PricingScopeKind =
  "account_model" | "provider_model" | "global_model" | "provider_default"

export type PricingMatchKind = "exact" | "prefix"

export type BillingMode = "token" | "subscription" | "unpriced"

export type PricingScope = {
  scopeKind: PricingScopeKind
  providerId?: string
  accountId?: string
}

export type PricingRule = {
  id: string
  version: number
  active: boolean
  scopeKind: PricingScopeKind
  providerId?: string
  accountId?: string
  modelPattern: string
  matchKind: PricingMatchKind
  billingMode: BillingMode
  inputUsdPerMillion?: string
  cachedReadUsdPerMillion?: string
  cacheWriteUsdPerMillion?: string
  outputUsdPerMillion?: string
  requestFeeUsd?: string
  cacheWriteIncludedInInput: boolean
  effectiveFromMs: number
  createdAtMs: number
  updatedAtMs: number
}

export type QuotaStatus =
  | "never"
  | "success"
  | "unsupported"
  | "unauthorized"
  | "rate_limited"
  | "error"

export type QuotaWindow = {
  usedPercent: number
  remainingPercent: number
  windowSeconds?: number
  resetAt?: number
}

export type QuotaData = {
  kind: "windowed"
  primary?: QuotaWindow
  secondary?: QuotaWindow
}

export type AccountQuota = {
  status: QuotaStatus
  data?: QuotaData
  planType?: string
  fetchedAt?: number
  lastAttemptAt?: number
  error?: string
  errorCode?: string
  estimates?: QuotaEstimate[]
  resetCredits?: ResetCreditSummary
}

export type ResetCreditDetailsStatus = "unknown" | "complete" | "partial"

export type ResetCreditSummary = {
  /** 服务端没有提供时为 undefined，绝不能按 0 张展示。 */
  availableCount?: number | null
  detailsStatus: ResetCreditDetailsStatus
}

export type ResetCredit = {
  id: string
  resetType?: string | null
  status?: string | null
  grantedAt?: number | null
  expiresAt?: number | null
  title?: string | null
  description?: string | null
}

export type ResetCreditDetails = {
  accountId: string
  summary: ResetCreditSummary
  credits: ResetCredit[]
}

export type ResetCreditConsumeOutcome =
  | "reset"
  | "already_redeemed"
  | "nothing_to_reset"
  | "no_credit"
  | "failed"
  | "unknown"

export type ResetCreditConsumeResult = {
  outcome: ResetCreditConsumeOutcome
  details: ResetCreditDetails
  quota?: AccountQuota
  refreshError?: string
}

export type QuotaEstimate = {
  windowSeconds: number
  resetAt: number
  estimatedTotalMicrousd: number
  estimatedAt: number
  /** 缺失字段表示旧计算结果，加载后会由后端清理。 */
  calculationVersion?: number
}

export type QuotaEstimateWindowResult = {
  windowSeconds: number
  resetAt: number
  success: boolean
  estimate?: QuotaEstimate
  reason?: string
}

export type QuotaEstimateResult = {
  windows: QuotaEstimateWindowResult[]
}

export type CredentialRefreshStatus =
  | "unknown"
  | "healthy"
  | "managed_by_codex"
  | "waiting_retry"
  | "reauthentication_required"
  | "not_refreshable"

export type LoginVerificationStatus =
  "unknown" | "valid" | "invalid" | "workspace_or_permission" | "check_failed"

export type CredentialRefreshState = {
  status: CredentialRefreshStatus
  lastAttemptAt?: number
  lastSuccessAt?: number
  nextRetryAt?: number
  retryCount?: number
  lastRefreshAt?: number
  lastSyncAt?: number
  lastCheckAt?: number
  verification?: LoginVerificationStatus
}

export type CredentialMaintenanceOutcome =
  | "refreshed"
  | "synced_from_codex"
  | "managed_by_codex"
  | "unchanged"
  | "waiting_retry"
  | "reauthentication_required"
  | "not_refreshable"

export type QuotaRefreshResult = {
  accountId: string
  quota: AccountQuota
}

export type Session = {
  identity: string
  id: string
  title: string
  provider: string
  cwd: string
  archived: boolean
  updatedAt: number
  sourceDb: string
  sourceRollout?: string
  originalProvider: string
  hasUserEvent: boolean
}

export type PageResult<T> = {
  items: T[]
  total: number
  page: number
  pageSize: number
}

export type RepairTarget = {
  id: string
  sources: string[]
  current: boolean
  count: number
}

export type RepairScan = {
  currentProvider: string
  targets: RepairTarget[]
  rolloutFiles: number
  sessionMetaCount: number
  databases: Array<{ path: string; schema: string; threadCount: number }>
  warnings: string[]
}

export type RepairResult = {
  targetProvider: string
  filesScanned: number
  filesCached: number
  filesOpened: number
  filesModified: number
  filesSkipped: number
  filesFailed: number
  sessionMetaUpdated: number
  rowsUpdated: number
  databasesScanned: number
  databasesUpdated: number
  warnings: string[]
  repairComplete: boolean
  verificationPassed: boolean
  elapsedMs: number
}

export type OfficialAccountView = {
  id: string
  name: string
  remark: string
  accountId: string
  email: string
  source: "open_ai_oauth" | "proxy_import"
  expiresAt: number | null
  credentialRefresh: CredentialRefreshState
  quota: AccountQuota
  active: boolean
  createdAt: number
  updatedAt: number
}

export type CredentialMaintenanceResult = {
  account: OfficialAccountView
  outcome: CredentialMaintenanceOutcome
}

export type ProxyImportResult = {
  accounts: OfficialAccountView[]
  detectedFormats: string[]
}

export type ProviderOverview = {
  providers: Provider[]
  officialAccounts: OfficialAccountView[]
}

export type DeviceAuthorization = {
  operationId: string
  userCode: string
  verificationUri: string
  expiresAt: number
  intervalSecs: number
}

export type DeviceAuthPollResult =
  | { status: "pending" }
  | { status: "expired" }
  | {
      status: "complete"
      account: OfficialAccountView
    }

export type ProviderTestResult = {
  ok: boolean
  status: number
  endpoint: string
  message: string
  suggestV1: boolean
}

export type ConfigPatchPreview = {
  operationId: string
  targetPath: string
  baseHash: string
  rendered: string
  changes: string[]
  apiKeyMasked: string
}

export type ConfigInspection = {
  path: string
  valid: boolean
  activeProvider?: string
  managedProviderPresent: boolean
  warnings: string[]
}

export type SettingsOverview = {
  inspection: ConfigInspection
  diagnostics: Record<string, unknown>
  canPreviewCustom: boolean
}

export type SupportDiagnostics = {
  schemaVersion: number
  generatedAt: string
  app: {
    name: string
    version: string
    buildProfile: string
  }
  system: {
    os: string
    architecture: string
    family: string
  }
  paths: {
    dataDirectory: string
    codexHome: string
    configFile: string
  }
  configuration: {
    valid: boolean
    activeProvider?: string
    managedProviderPresent: boolean
    warnings: string[]
  }
  connection: {
    activeKind: "none" | "provider" | "official"
    providerCount: number
    officialAccountCount: number
    activeModel?: string
  }
  storage: {
    files: Array<{
      name: string
      exists: boolean
      readable: boolean
      sizeBytes?: number
    }>
    usageDatabase: {
      exists: boolean
      sizeBytes?: number
      schemaVersion?: number
      quickCheck: string
      eventCount?: number
      cursorCount?: number
    }
    sessionDatabaseCount: number
    indexedSessionCount: number
  }
  network: {
    environmentProxyConfigured: boolean
    noProxyConfigured: boolean
    systemProxyConfigured: boolean
    tlsBackend: string
  }
  warnings: string[]
  privacy: {
    homePathsRedacted: boolean
    omitted: string[]
  }
}

export type CodexAppSetting = {
  /** 手动配置的 Codex 应用路径（.app 目录或可执行文件） */
  configured?: string
  /** 实际检测到的 Codex 应用路径 */
  detected?: string
}

export type ProviderModelsDevMeta = {
  name?: string
  contextWindow?: number
  description?: string
}

export type ModelUnlockStatus = {
  appFound: boolean
  appRunning: boolean
  debugPort?: number
  injected: boolean
  modelCount: number
  models: string[]
  warning?: string
}

export type ModelUnlockResult = {
  port: number
  injected: boolean
  modelCount: number
  message: string
}
