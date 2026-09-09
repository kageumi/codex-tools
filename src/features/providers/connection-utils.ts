import { formatDate } from "@/lib/format"
import { call } from "@/lib/ipc"
import type {
  CredentialMaintenanceResult,
  OfficialAccountView,
  Provider,
  ProviderSaveInput,
  RepairResult,
} from "@/types"

import { displayQuotaWindows } from "./quota-estimate"

export type ConnectionKind = "account" | "provider"

export const emptyProvider = (): Provider => ({
  id: "",
  name: "",
  baseUrl: "https://api.openai.com/v1",
  headers: {},
  timeoutSecs: 30,
  enabled: true,
  active: false,
  apiType: "responses",
  apiKey: "",
  hasApiKey: false,
  createdAt: 0,
  updatedAt: 0,
})

/** 将编辑器状态转换为保存 DTO；null 明确表示“全部有效模型”。 */
export function providerSaveInputOf(provider: Provider): ProviderSaveInput {
  return {
    id: provider.id,
    name: provider.name,
    baseUrl: provider.baseUrl,
    headers: { ...provider.headers },
    timeoutSecs: provider.timeoutSecs,
    enabled: provider.enabled,
    apiType: provider.apiType,
    selectedModels: provider.selectedModels ?? null,
    customModels: provider.customModels ?? [],
    contextWindowOverride: provider.contextWindowOverride ?? null,
    apiKey: provider.apiKey,
  }
}

/** 输入框只接受正整数 token；留空表示恢复当前服务的自动匹配。 */
export function isValidContextWindowOverride(value: string) {
  const trimmed = value.trim()
  return (
    !trimmed ||
    (/^[1-9]\d*$/.test(trimmed) && Number.isSafeInteger(Number(trimmed)))
  )
}

export function contextWindowOverrideInputOf(value: string): number | null {
  const trimmed = value.trim()
  return trimmed ? Number(trimmed) : null
}

/** 常驻编辑器每次打开或切换服务时用于重置未保存输入的会话标识。 */
export function contextWindowOverrideEditorSessionOf(
  provider: Provider,
  open: boolean
) {
  return [
    open ? "open" : "closed",
    provider.id,
    provider.updatedAt,
    provider.contextWindowOverride ?? "automatic",
  ].join("\u0000")
}

/** 有效模型 = /models 同步模型 ∪ 自定义模型（保序去重）。 */
export function effectiveModelsOf(provider: Provider): string[] {
  const available = provider.availableModels ?? []
  const availableSet = new Set(available)
  const models = [...available]
  for (const model of provider.customModels ?? []) {
    if (!availableSet.has(model)) models.push(model)
  }
  return models
}

/** 有效模型数 = /models 同步模型 ∪ 自定义模型（去重）。 */
export function effectiveModelCount(provider: Provider) {
  const available = provider.availableModels ?? []
  const availableSet = new Set(available)
  let count = available.length
  for (const model of provider.customModels ?? []) {
    if (!availableSet.has(model)) count += 1
  }
  return count
}

/** 将后端序列化的 null 统一转为 undefined，保持"未设置=全选"语义。 */
function selectedModelsOf(provider: Provider): string[] | undefined {
  return provider.selectedModels ?? undefined
}

/** 全选 = 未设置 selectedModels（默认写入全部），或所有有效模型均被选中。 */
export function allModelsSelected(provider: Provider): boolean {
  const selected = selectedModelsOf(provider)
  if (selected === undefined) return true
  const selectedSet = new Set(selected)
  return effectiveModelsOf(provider).every((model) => selectedSet.has(model))
}

/** 无选中 = 存在有效模型但 selectedModels 为空数组（此时禁止保存）。 */
export function noModelsSelected(provider: Provider): boolean {
  const selected = selectedModelsOf(provider)
  return effectiveModelCount(provider) > 0 && selected?.length === 0
}

export function accountIsExpired(account: OfficialAccountView) {
  return (
    account.expiresAt != null &&
    account.expiresAt <= Math.floor(Date.now() / 1000)
  )
}

/** 仅使用后端提供的脱敏维护状态；不得从前端推断或展示任何凭据内容。 */
export function credentialRefreshText(account: OfficialAccountView) {
  switch (account.credentialRefresh.status) {
    case "healthy":
      return account.credentialRefresh.lastRefreshAt
        ? "自动刷新正常"
        : "未到刷新时间"
    case "managed_by_codex":
      return "由 Codex 维护"
    case "waiting_retry":
      return "等待重试"
    case "reauthentication_required":
      return "需要重新登录"
    case "not_refreshable":
      return "不可自动刷新"
    default:
      return "未到刷新时间"
  }
}

/** 手动检查只报告实际 outcome，避免把 Codex 接管或无变更说成“已更新”。 */
export function credentialMaintenanceMessage(
  result: CredentialMaintenanceResult
) {
  if (result.account.credentialRefresh.status === "reauthentication_required") {
    return "需要重新登录"
  }
  if (result.account.credentialRefresh.status === "not_refreshable") {
    return "不可自动刷新：缺少可刷新凭据"
  }
  switch (result.outcome) {
    case "refreshed":
      return "登录凭据已刷新"
    case "synced_from_codex":
      return "已同步 Codex 最新凭据"
    case "managed_by_codex":
      return "由 Codex 维护"
    case "unchanged":
      return "未到自动刷新时间，已完成登录检查"
    case "waiting_retry":
      return "正在等待重试，未重复消耗登录凭据"
    case "reauthentication_required":
      return "需要重新登录"
    case "not_refreshable":
      return "不可自动刷新：缺少可刷新凭据"
  }
}

/** 登录状态只能来自在线验证，不能由本地过期时间或维护成功推断。 */
export function loginVerificationText(account: OfficialAccountView) {
  switch (account.credentialRefresh.verification) {
    case "valid":
      return "在线验证有效"
    case "invalid":
      return "登录无效，需要重新登录"
    case "workspace_or_permission":
      return "工作区或权限限制"
    case "check_failed":
      return "在线检查失败"
    default:
      return "尚未在线验证"
  }
}

export const DEACTIVATED_WORKSPACE_CODE = "deactivated_workspace"

export function accountWorkspaceIsDeactivated(account: OfficialAccountView) {
  return account.quota.errorCode === DEACTIVATED_WORKSPACE_CODE
}

export function quotaStatusText(
  account: OfficialAccountView,
  remainingPercent?: number
) {
  if (remainingPercent !== undefined) {
    return `剩余 ${remainingPercent.toFixed(1)}%`
  }
  if (account.quota.status !== "success" && account.quota.error) {
    return account.quota.error
  }
  switch (account.quota.status) {
    case "never":
      return "尚未刷新"
    case "unauthorized":
      return "登录已失效，请刷新登录"
    case "rate_limited":
      return "请求频繁，请稍后重试"
    case "unsupported":
      return "当前账号暂不支持额度查询"
    case "error":
      return account.quota.error || "额度刷新失败"
    default:
      return "暂无额度数据"
  }
}

export function accountDescription(account: OfficialAccountView) {
  const quotaWindows = displayQuotaWindows(account.quota).sort(
    (left, right) => left.windowSeconds - right.windowSeconds
  )
  const quotaText = quotaWindows.length
    ? quotaWindows
        .map(
          (quota) =>
            `${quota.label} 剩余 ${quota.remainingPercent.toFixed(1)}% · 重置 ${quota.resetAt ? formatDate(quota.resetAt, true) : "—"}`
        )
        .join("；")
    : quotaStatusText(account)
  const identityText =
    account.email.trim() || account.name.trim() || "OpenAI 账号"
  return `${accountPlanText(account)} · ${quotaText} · ${identityText}`
}

export function accountPlanText(account: OfficialAccountView) {
  const planType = account.quota.planType?.trim()
  if (!planType) return "OpenAI"
  switch (planType.toLowerCase()) {
    case "plus":
      return "Plus"
    case "pro":
      return "Pro"
    case "pro_5x":
    case "pro-5x":
      return "Pro 5x"
    case "pro_20x":
    case "pro-20x":
      return "Pro 20x"
    default:
      return planType
        .split(/[_-]+/)
        .filter(Boolean)
        .map((part) => part[0]?.toUpperCase() + part.slice(1).toLowerCase())
        .join(" ")
  }
}

export type FallbackCandidate = {
  id: string
  kind: ConnectionKind
}

export function repairWarning(result: RepairResult) {
  const details: string[] = []
  if (!result.repairComplete) {
    details.push("会话归属修复未完成，切换已回滚")
  }
  if (result.filesFailed > 0) {
    details.push(`${result.filesFailed} 个会话文件修复失败`)
  }
  if (result.warnings.length > 0) {
    details.push(result.warnings.slice(0, 2).join("；"))
  }
  if (details.length === 0) return undefined
  return result.repairComplete === false
    ? `切换已回滚：${details.join("；")}`
    : `连接已切换，但${details.join("；")}`
}

export function buildFallbackCandidates(
  accounts: OfficialAccountView[],
  providers: Provider[],
  excludedAccountIds: ReadonlySet<string> = new Set(),
  excludedProviderId?: string
): FallbackCandidate[] {
  const healthyAccounts: OfficialAccountView[] = []
  const otherAccounts: OfficialAccountView[] = []
  for (const account of accounts) {
    if (
      excludedAccountIds.has(account.id) ||
      accountWorkspaceIsDeactivated(account)
    ) {
      continue
    }
    const group =
      account.quota.status !== "unauthorized" && !accountIsExpired(account)
        ? healthyAccounts
        : otherAccounts
    group.push(account)
  }
  const enabledProviders = providers.filter(
    (provider) =>
      provider.id !== excludedProviderId &&
      provider.enabled &&
      provider.hasApiKey
  )

  return [
    ...healthyAccounts.map((account): FallbackCandidate => ({
      kind: "account",
      id: account.id,
    })),
    ...enabledProviders.map((provider): FallbackCandidate => ({
      kind: "provider",
      id: provider.id,
    })),
    ...otherAccounts.map((account): FallbackCandidate => ({
      kind: "account",
      id: account.id,
    })),
  ]
}

export async function switchActiveConnection(
  candidates: FallbackCandidate[]
): Promise<{ switchedId?: string; repair?: RepairResult; error?: unknown }> {
  let lastError: unknown
  for (const candidate of candidates) {
    try {
      const repair = await (candidate.kind === "account"
        ? call("connections_activate_account", { id: candidate.id })
        : call("connections_activate", { id: candidate.id }))
      if (repair?.repairComplete === false) {
        throw new Error(
          repairWarning(repair) ?? "会话归属修复未完成，连接已回滚，请稍后重试"
        )
      }
      return { switchedId: candidate.id, repair }
    } catch (reason) {
      lastError = reason
    }
  }
  return { error: lastError }
}

/** 在有效模型中切换某个模型的选中状态，返回新的 selectedModels。 */
export function toggleModelSelected(
  provider: Provider,
  model: string,
  checked: boolean
): string[] | undefined {
  const eff = effectiveModelsOf(provider)
  const current = selectedModelsOf(provider) ?? eff
  const next = checked
    ? [...new Set([...current, model])]
    : current.filter((value) => value !== model)
  return next.length === eff.length ? undefined : next
}

/** 添加自定义模型并保持“默认写入”语义（合并 customModels + selectedModels）。 */
export function addCustomModelTo(provider: Provider, model: string): Provider {
  const prevEffective = effectiveModelsOf(provider)
  const nextCustom = [...(provider.customModels ?? []), model]
  let nextSelected = selectedModelsOf(provider)
  // 新增的自定义模型默认写入 Codex（与“默认全选”一致）。
  if (nextSelected !== undefined) {
    const candidate = [...nextSelected, model]
    nextSelected =
      candidate.length === prevEffective.length + 1 ? undefined : candidate
  }
  return {
    ...provider,
    customModels: nextCustom,
    selectedModels: nextSelected,
  }
}

/** 移除自定义模型，并从 selectedModels 中同步剔除，必要时回退全选。 */
export function removeCustomModelFrom(
  provider: Provider,
  model: string
): Provider {
  const prevEffective = effectiveModelsOf(provider)
  const nextCustom = (provider.customModels ?? []).filter(
    (value) => value !== model
  )
  let nextSelected = selectedModelsOf(provider)
  if (nextSelected !== undefined) {
    const filtered = nextSelected.filter((value) => value !== model)
    nextSelected =
      filtered.length === prevEffective.length - 1 ? undefined : filtered
  }
  return {
    ...provider,
    customModels: nextCustom,
    selectedModels: nextSelected,
  }
}
