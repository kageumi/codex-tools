import { useCallback, useEffect, useMemo, useState } from "react"
import {
  Add01Icon,
  ArrowRight01Icon,
  CheckmarkCircle02Icon,
  Key01Icon,
  Login03Icon,
  Refresh01Icon,
  TestTube01Icon,
  Ticket01Icon,
} from "@hugeicons/core-free-icons"
import { HugeiconsIcon } from "@hugeicons/react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  Empty,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty"
import { Spinner } from "@/components/ui/spinner"
import { Skeleton } from "@/components/ui/skeleton"
import { Progress } from "@/components/ui/progress"
import { toast } from "@/components/ui/toast"
import { errorMessage, formatDate, formatUsd } from "@/lib/format"
import { useAsyncAction } from "@/hooks/use-async-action"
import { call } from "@/lib/ipc"
import type {
  DeviceAuthorization,
  OfficialAccountView,
  Provider,
  ProviderOverview,
} from "@/types"
import { emptyProvider } from "./connection-utils"

import {
  AccountLoginDialog,
  type AccountLoginMode,
} from "./account-login-dialog"
import {
  refreshAccountLogin,
  refreshAccountQuota,
  estimateAccountQuota,
  syncProviderModels,
  testProviderConnection,
} from "./connection-actions"
import {
  accountPlanText,
  credentialMaintenanceMessage,
  credentialRefreshText,
  loginVerificationText,
  effectiveModelCount,
  quotaStatusText,
} from "./connection-utils"
import { AccountRemarkDialog } from "./account-remark-dialog"
import {
  cloneProviderForEditing,
  ProviderEditorDialog,
} from "./provider-editor-dialog"
import { displayQuotaWindows, quotaWindowEstimate } from "./quota-estimate"
import { ResetCreditsDialog } from "./reset-credits-dialog"
import { resetCreditCountText } from "./reset-credits"

export function ProvidersPage({
  connections,
  selectedId,
  onSelectedIdChange,
  onRefresh,
}: {
  refreshRevision: number
  onRefresh: () => void
  connections?: ProviderOverview
  selectedId?: string
  onSelectedIdChange: (id: string) => void
}) {
  const [editorOpen, setEditorOpen] = useState(false)
  const [loginOpen, setLoginOpen] = useState(false)
  const [loginMode, setLoginMode] = useState<AccountLoginMode>("browser")
  const [loginError, setLoginError] = useState<string>()
  const [editor, setEditor] = useState<Provider>(emptyProvider())
  const [remarkAccount, setRemarkAccount] = useState<OfficialAccountView>()
  const [resetCreditAccountId, setResetCreditAccountId] = useState<string>()
  const resetCreditAccount = connections?.officialAccounts.find(
    (item) => item.id === resetCreditAccountId
  )
  const [authorization, setAuthorization] = useState<DeviceAuthorization>()
  const [estimating, setEstimating] = useState(false)
  const { busy, begin, end, run } = useAsyncAction<string>()

  const selected = useMemo(() => {
    const account = connections?.officialAccounts.find(
      (item) => item.id === selectedId
    )
    if (account) return { kind: "account" as const, value: account }
    const provider = connections?.providers.find(
      (item) => item.id === selectedId
    )
    if (provider) return { kind: "provider" as const, value: provider }
    const fallback =
      connections?.officialAccounts.find((item) => item.active) ??
      connections?.providers.find((item) => item.active) ??
      connections?.officialAccounts[0] ??
      connections?.providers[0]
    if (!fallback) return undefined
    return "email" in fallback
      ? { kind: "account" as const, value: fallback }
      : { kind: "provider" as const, value: fallback }
  }, [connections, selectedId])

  useEffect(() => {
    if (selected && selectedId !== selected.value.id) {
      onSelectedIdChange(selected.value.id)
    }
  }, [onSelectedIdChange, selected, selectedId])

  const startLogin = async () => {
    if (!begin("login")) return
    setLoginError(undefined)
    try {
      setAuthorization(await call("connections_login_start"))
    } catch (reason) {
      setLoginError(errorMessage(reason))
      toast.add({
        title: "无法获取 OpenAI 授权码",
        description: errorMessage(reason),
        type: "error",
      })
    } finally {
      end("login")
    }
  }

  const finishLoginPoll = useCallback(
    (result: Awaited<ReturnType<typeof call<"connections_login_poll">>>) => {
      if (result.status === "complete") {
        toast.add({
          title: "OpenAI 账号已保存",
          description: "当前连接未切换；需要使用时请在账号列表中手动切换。",
          type: "success",
        })
        setAuthorization(undefined)
        setLoginOpen(false)
        onSelectedIdChange(result.account.id)
        onRefresh()
        return true
      }
      if (result.status === "expired") {
        setAuthorization(undefined)
        setLoginError("授权码已过期，请重新获取授权码。")
        return true
      }
      return false
    },
    [onRefresh, onSelectedIdChange]
  )

  const checkLogin = async () => {
    if (!authorization) return
    if (!begin("poll")) return
    try {
      const result = await call("connections_login_poll", {
        operationId: authorization.operationId,
      })
      if (!finishLoginPoll(result)) {
        toast.add({
          title: "OpenAI 尚未确认授权",
        })
      }
    } catch (reason) {
      toast.add({
        title: "无法检查授权结果",
        description: errorMessage(reason),
        type: "error",
      })
    } finally {
      end("poll")
    }
  }

  useEffect(() => {
    if (!authorization || !loginOpen || loginMode !== "browser") return
    let cancelled = false
    let timer: number | undefined

    const schedule = () => {
      timer = window.setTimeout(
        () => {
          if (!begin("poll")) {
            schedule()
            return
          }
          void call("connections_login_poll", {
            operationId: authorization.operationId,
          })
            .then((result) => {
              if (cancelled || finishLoginPoll(result)) return
              schedule()
            })
            .catch((reason) => {
              if (cancelled) return
              setLoginError(
                `暂时无法连接 OpenAI，本应用将自动重试：${errorMessage(reason)}`
              )
              schedule()
            })
            .finally(() => end("poll"))
        },
        Math.max(1, authorization.intervalSecs) * 1000
      )
    }

    schedule()
    return () => {
      cancelled = true
      if (timer !== undefined) window.clearTimeout(timer)
    }
  }, [authorization, begin, end, finishLoginPoll, loginMode, loginOpen])

  const openAccountLogin = (mode: AccountLoginMode) => {
    if (busy) return
    if (mode !== "browser") setAuthorization(undefined)
    setLoginMode(mode)
    setLoginError(undefined)
    setLoginOpen(true)
  }

  const importCookie = async (
    name: string | undefined,
    accountId: string | undefined,
    content: string
  ) => {
    if (!begin("import")) return
    try {
      const imported = await call("connections_import_cookie", {
        name,
        accountId,
        content,
      })
      const firstAccount = imported.accounts[0]
      const formatLabel =
        imported.detectedFormats.join("、") || "Cookie 登录数据"
      toast.add({
        title:
          imported.accounts.length > 1
            ? `已导入 ${imported.accounts.length} 个 Cookie 账号`
            : "Cookie 登录数据已导入",
        description: `${formatLabel}${firstAccount ? ` · ${firstAccount.name}` : ""}`,
        type: "success",
      })
      setAuthorization(undefined)
      setLoginOpen(false)
      if (firstAccount) onSelectedIdChange(firstAccount.id)
      onRefresh()
    } catch (reason) {
      setLoginError(errorMessage(reason))
      toast.add({
        title: "Cookie 登录数据导入失败",
        description: errorMessage(reason),
        type: "error",
      })
    } finally {
      end("import")
    }
  }

  const connectionEditors = (
    <>
      <ProviderEditorDialog
        open={editorOpen}
        onOpenChange={setEditorOpen}
        provider={editor}
        onProviderChange={setEditor}
        onSaved={(saved) => {
          setEditor(saved)
          setEditorOpen(false)
          onRefresh()
        }}
      />
      <AccountRemarkDialog
        key={remarkAccount?.id ?? "closed"}
        account={remarkAccount}
        open={Boolean(remarkAccount)}
        onOpenChange={(open) => {
          if (!open) setRemarkAccount(undefined)
        }}
        onSaved={onRefresh}
      />
      <AccountLoginDialog
        key={loginOpen ? "login-open" : "login-closed"}
        open={loginOpen}
        mode={loginMode}
        onModeChange={(mode) => {
          if (mode !== "browser") setAuthorization(undefined)
          setLoginMode(mode)
        }}
        onOpenChange={(open) => {
          if (!open && busy) return
          setLoginOpen(open)
          if (!open && !authorization) setLoginError(undefined)
        }}
        authorization={authorization}
        starting={busy === "login"}
        polling={busy === "poll"}
        importing={busy === "import"}
        error={loginError}
        onStart={() => void startLogin()}
        onOpenPage={() =>
          void call("connections_open_login_page").catch((reason) =>
            toast.add({
              title: "无法打开登录页面",
              description: errorMessage(reason),
              type: "error",
            })
          )
        }
        onCheck={() => void checkLogin()}
        onImport={(name, accountId, content) =>
          void importCookie(name, accountId, content)
        }
      />
      <ResetCreditsDialog
        key={resetCreditAccountId ?? "closed"}
        account={resetCreditAccount}
        open={Boolean(resetCreditAccountId && resetCreditAccount)}
        onOpenChange={(nextOpen) => {
          if (!nextOpen) setResetCreditAccountId(undefined)
        }}
        onChanged={onRefresh}
      />
    </>
  )

  if (!connections) {
    return <ProvidersLoading />
  }

  if (!selected) {
    return (
      <div className="min-h-full px-3 pt-1 pb-3">
        <Card size="sm" className="min-h-full justify-center">
          <Empty>
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <HugeiconsIcon icon={Key01Icon} />
              </EmptyMedia>
              <EmptyTitle>还没有连接</EmptyTitle>
            </EmptyHeader>
            <div className="grid w-full grid-cols-3 gap-2">
              <Button
                type="button"
                variant="outline"
                className="min-w-0 px-2 text-xs"
                disabled={Boolean(busy)}
                onClick={() => openAccountLogin("browser")}
              >
                <HugeiconsIcon icon={Login03Icon} data-icon="inline-start" />
                OpenAI 授权
              </Button>
              <Button
                type="button"
                variant="outline"
                className="min-w-0 px-2 text-xs"
                disabled={Boolean(busy)}
                onClick={() => openAccountLogin("cookie")}
              >
                <HugeiconsIcon icon={Key01Icon} data-icon="inline-start" />
                导入 Cookie
              </Button>
              <Button
                type="button"
                variant="outline"
                className="min-w-0 px-2 text-xs"
                disabled={Boolean(busy)}
                onClick={() => {
                  setEditor(emptyProvider())
                  setEditorOpen(true)
                }}
              >
                <HugeiconsIcon icon={Add01Icon} data-icon="inline-start" />
                添加 API 服务
              </Button>
            </div>
          </Empty>
        </Card>
        {connectionEditors}
      </div>
    )
  }

  const isAccount = selected.kind === "account"
  const item = selected.value
  const account = isAccount ? (item as OfficialAccountView) : undefined
  const provider = !isAccount ? (item as Provider) : undefined
  const displayName = account?.remark || item.name
  const actionBusy = Boolean(busy) || estimating
  const canEstimate = Boolean(account)

  const estimateQuota = async () => {
    if (!account || estimating) return
    setEstimating(true)
    try {
      const result = await estimateAccountQuota(account.id)
      const failed = result.windows.filter((window) => !window.success)
      toast.add({
        title: failed.length ? "部分额度窗口未能估算" : "额度估算已更新",
        description:
          failed
            .map((window) => window.reason)
            .filter(Boolean)
            .join("；") || undefined,
        type: failed.length ? "error" : "success",
      })
    } catch (reason) {
      toast.add({
        title: "额度估算失败",
        description: errorMessage(reason),
        type: "error",
      })
    } finally {
      setEstimating(false)
      onRefresh()
    }
  }

  return (
    <div className="flex min-h-full flex-col gap-3 px-3 pt-1 pb-3">
      <Card size="sm" className="shrink-0">
        <CardContent className="flex flex-col gap-2">
          <div className="text-sm font-medium">添加连接</div>
          <div className="grid w-full grid-cols-3 gap-2">
            <Button
              type="button"
              size="sm"
              variant="outline"
              className="min-w-0 px-2 text-xs"
              disabled={actionBusy}
              onClick={() => openAccountLogin("browser")}
            >
              <HugeiconsIcon icon={Login03Icon} data-icon="inline-start" />
              OpenAI 授权
            </Button>
            <Button
              type="button"
              size="sm"
              variant="outline"
              className="min-w-0 px-2 text-xs"
              disabled={actionBusy}
              onClick={() => openAccountLogin("cookie")}
            >
              <HugeiconsIcon icon={Key01Icon} data-icon="inline-start" />
              导入 Cookie
            </Button>
            <Button
              type="button"
              size="sm"
              variant="outline"
              className="min-w-0 px-2 text-xs"
              disabled={actionBusy}
              onClick={() => {
                setEditor(emptyProvider())
                setEditorOpen(true)
              }}
            >
              <HugeiconsIcon icon={Add01Icon} data-icon="inline-start" />
              添加 API 服务
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card key={item.id} size="sm" className="shrink-0">
        {isAccount ? (
          <AccountCardHeader
            account={account!}
            displayName={displayName}
            onOpenResetCredits={() => setResetCreditAccountId(account!.id)}
          />
        ) : (
          <CardHeader className="border-b">
            <div className="flex items-center gap-2">
              <CardTitle>{displayName}</CardTitle>
              {item.active && (
                <Badge>
                  <HugeiconsIcon icon={CheckmarkCircle02Icon} />
                  当前连接
                </Badge>
              )}
              <Badge variant="outline">API 服务</Badge>
            </div>
            <div className="text-sm text-muted-foreground">
              {provider?.baseUrl}
            </div>
          </CardHeader>
        )}
        {isAccount ? (
          <AccountDetailContent account={account!} />
        ) : (
          <CardContent className="grid grid-cols-2 gap-x-6 gap-y-4">
            <Detail
              label="接入方式"
              value={
                provider?.apiType === "chat"
                  ? "Chat Completions"
                  : "Responses API"
              }
            />
            <Detail
              label="凭据状态"
              value={
                provider?.hasApiKey ? "API Key 已保存" : "等待填写 API Key"
              }
            />
            <Detail label="最近更新" value={formatDate(item.updatedAt, true)} />
            <Detail
              label="可用模型"
              value={`${provider ? effectiveModelCount(provider) : 0} 个`}
            />
          </CardContent>
        )}
        <CardFooter
          className={isAccount ? "flex-wrap gap-2 py-2" : "flex-wrap gap-2"}
        >
          {isAccount ? (
            <>
              <Button
                size="sm"
                disabled={actionBusy}
                aria-busy={busy === "quota"}
                onClick={() =>
                  void run("quota", () => refreshAccountQuota(item.id), {
                    success: "额度已刷新",
                    onSuccess: onRefresh,
                  })
                }
              >
                {busy === "quota" ? <Spinner data-icon="inline-start" /> : null}
                刷新额度
              </Button>
              <Button
                size="sm"
                variant="outline"
                disabled={actionBusy || !canEstimate}
                aria-busy={estimating}
                onClick={() => void estimateQuota()}
              >
                {estimating ? <Spinner data-icon="inline-start" /> : null}
                {estimating ? "刷新并估算中" : "额度估算"}
              </Button>
              <Button
                size="sm"
                variant="outline"
                disabled={actionBusy}
                aria-busy={busy === "login-refresh"}
                onClick={() =>
                  void run(
                    "login-refresh",
                    () => refreshAccountLogin(item.id),
                    {
                      success: "登录维护已完成",
                      successDescription: credentialMaintenanceMessage,
                      onSuccess: onRefresh,
                    }
                  )
                }
              >
                {busy === "login-refresh" ? (
                  <Spinner data-icon="inline-start" />
                ) : null}
                立即刷新登录
              </Button>
              <Button
                size="sm"
                variant="outline"
                disabled={actionBusy}
                onClick={() => setRemarkAccount(account)}
              >
                编辑备注
              </Button>
            </>
          ) : (
            <>
              <Button
                disabled={actionBusy}
                aria-busy={busy === "test"}
                onClick={() =>
                  void run("test", () => testProviderConnection(item.id), {
                    success: "连接测试通过",
                    onSuccess: onRefresh,
                  })
                }
              >
                {busy === "test" ? (
                  <Spinner data-icon="inline-start" />
                ) : (
                  <HugeiconsIcon
                    icon={TestTube01Icon}
                    data-icon="inline-start"
                  />
                )}
                测试连接
              </Button>
              <Button
                variant="outline"
                disabled={actionBusy}
                aria-busy={busy === "models"}
                onClick={() =>
                  void run("models", () => syncProviderModels(item.id), {
                    success: "模型已同步",
                    onSuccess: onRefresh,
                  })
                }
              >
                {busy === "models" ? (
                  <Spinner data-icon="inline-start" />
                ) : (
                  <HugeiconsIcon
                    icon={Refresh01Icon}
                    data-icon="inline-start"
                  />
                )}
                同步模型
              </Button>
              <Button
                variant="outline"
                disabled={actionBusy}
                onClick={() => {
                  if (!provider) return
                  setEditor(cloneProviderForEditing(provider))
                  setEditorOpen(true)
                }}
              >
                编辑 API 服务
              </Button>
            </>
          )}
        </CardFooter>
      </Card>

      {connectionEditors}
    </div>
  )
}

function ProvidersLoading() {
  return (
    <div
      className="flex min-h-full flex-col gap-3 px-3 pt-1 pb-3"
      aria-label="正在读取账号与服务"
      aria-busy="true"
    >
      <Card size="sm" className="shrink-0">
        <CardContent className="flex items-center gap-3">
          <div className="flex flex-1 flex-col gap-2">
            <Skeleton className="h-4 w-24" />
            <Skeleton className="h-3 w-52" />
          </div>
          <Skeleton className="h-8 w-24" />
          <Skeleton className="h-8 w-28" />
        </CardContent>
      </Card>
      <Card size="sm" className="shrink-0">
        <CardHeader className="border-b">
          <Skeleton className="h-5 w-36" />
          <Skeleton className="h-3.5 w-56" />
          <div className="flex flex-wrap gap-1.5">
            <Skeleton className="h-5 w-24" />
            <Skeleton className="h-5 w-28" />
          </div>
        </CardHeader>
        <CardContent className="flex flex-col gap-2">
          <div className="flex items-center gap-1.5">
            <Skeleton className="h-3.5 w-8" />
          </div>
          <div className="grid grid-cols-2 gap-2">
            {Array.from({ length: 2 }, (_, index) => (
              <div key={index} className="flex min-w-0 flex-col gap-1.5">
                <div className="flex items-center gap-1.5">
                  <Skeleton className="h-5 w-12" />
                  <Skeleton className="h-3.5 w-16" />
                </div>
                <Skeleton className="h-1 w-full" />
                <Skeleton className="h-3.5 w-32 max-w-full" />
              </div>
            ))}
          </div>
          <Skeleton className="h-3.5 w-52 max-w-full" />
          <div className="grid min-w-0 grid-cols-3 gap-2">
            {Array.from({ length: 3 }, (_, index) => (
              <div key={index} className="flex min-w-0 items-center gap-1">
                <Skeleton className="h-3.5 w-14" />
                <Skeleton className="h-3.5 w-20 max-w-full" />
              </div>
            ))}
          </div>
        </CardContent>
        <CardFooter className="gap-2 py-2">
          <Skeleton className="h-8 w-24" />
          <Skeleton className="h-8 w-24" />
          <Skeleton className="h-8 w-28" />
        </CardFooter>
      </Card>
    </div>
  )
}

type StatusBadgeVariant = "default" | "secondary" | "destructive" | "outline"

function AccountCardHeader({
  account,
  displayName,
  onOpenResetCredits,
}: {
  account: OfficialAccountView
  displayName: string
  onOpenResetCredits: () => void
}) {
  const loginStatus = loginVerificationText(account)
  const maintenanceStatus = credentialRefreshText(account)
  const shortLoginStatus = shortLoginStatusText(account)
  const shortMaintenanceStatus = shortMaintenanceStatusText(account)

  return (
    <CardHeader className="grid-cols-1 gap-4 border-b min-[480px]:grid-cols-[minmax(0,1fr)_auto]">
      <div className="flex min-w-0 flex-col gap-1.5">
        <div className="flex flex-wrap items-center gap-2">
          <CardTitle className="min-w-0 break-words">{displayName}</CardTitle>
          {account.active && (
            <Badge>
              <HugeiconsIcon icon={CheckmarkCircle02Icon} />
              当前连接
            </Badge>
          )}
          <Badge variant="secondary">{accountPlanText(account)}</Badge>
        </div>
        <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-sm text-muted-foreground">
          <span className="break-all">{account.email || "未提供邮箱"}</span>
          <span aria-hidden="true">·</span>
          <span>
            {account.source === "proxy_import"
              ? "Cookie 登录数据"
              : "OpenAI 官方授权"}
          </span>
        </div>
        <div className="flex min-w-0 flex-wrap items-center gap-1.5">
          <Badge
            variant={loginVerificationVariant(account)}
            className="h-auto max-w-full justify-start px-1.5 py-0.5 text-left leading-tight break-words whitespace-normal"
            aria-label={`登录状态：${loginStatus}`}
          >
            <span>{shortLoginStatus}</span>
          </Badge>
          <Badge
            variant={credentialRefreshVariant(account)}
            className="h-auto max-w-full justify-start px-1.5 py-0.5 text-left leading-tight break-words whitespace-normal"
            aria-label={`自动维护：${maintenanceStatus}`}
          >
            <span>{shortMaintenanceStatus}</span>
          </Badge>
        </div>
      </div>
      <Button
        type="button"
        variant="outline"
        className="h-auto justify-between gap-5 rounded-xl border-foreground/15 bg-muted/50 px-4 py-3 text-left hover:border-foreground/30 hover:bg-muted min-[480px]:min-w-36"
        onClick={onOpenResetCredits}
        aria-label={`${resetCreditCountText(account)}，打开重置卡列表`}
        aria-haspopup="dialog"
      >
        <span className="flex flex-col gap-1.5">
          <span className="flex items-center gap-1.5 text-sm">
            <HugeiconsIcon icon={Ticket01Icon} aria-hidden="true" />
            重置卡
          </span>
          <span className="flex items-baseline gap-1">
            <span className="text-2xl leading-none font-semibold tabular-nums">
              {account.quota.resetCredits?.availableCount ?? "—"}
            </span>
            <span className="text-xs font-normal text-muted-foreground">
              {account.quota.resetCredits?.availableCount == null
                ? "待查询"
                : "张"}
            </span>
          </span>
        </span>
        <span className="flex items-center gap-1 self-end text-xs text-muted-foreground">
          查看
          <HugeiconsIcon
            icon={ArrowRight01Icon}
            className="size-3.5"
            aria-hidden="true"
          />
        </span>
      </Button>
    </CardHeader>
  )
}

function AccountDetailContent({ account }: { account: OfficialAccountView }) {
  const quotaWindows = displayQuotaWindows(account.quota)

  return (
    <CardContent className="flex flex-col gap-1.5">
      <section className="flex flex-col gap-1 border-t pt-1.5">
        <div className="text-xs font-medium text-muted-foreground">额度</div>
        {quotaWindows.length ? (
          <div className="flex flex-col gap-1.5">
            <div
              className={
                quotaWindows.length > 1
                  ? "grid grid-cols-2 gap-2"
                  : "grid gap-2"
              }
            >
              {quotaWindows.map((quota) => {
                const estimate = quotaWindowEstimate(
                  account.quota.estimates ?? [],
                  quota
                )
                return (
                  <div
                    key={`${quota.windowSeconds}-${quota.resetAt}`}
                    className="flex min-w-0 flex-col gap-1.5"
                  >
                    <div className="flex min-w-0 flex-wrap items-center gap-1.5 text-xs font-medium">
                      <Badge variant="outline">{quota.label}</Badge>
                      <span className="text-muted-foreground tabular-nums">
                        {quota.remainingPercent.toFixed(1)}% 可用
                      </span>
                    </div>
                    <Progress
                      value={quota.remainingPercent}
                      className="gap-0 [&_[data-slot=progress-track]]:h-1"
                      aria-label={`${quota.label} Token 可用额度`}
                    />
                    <div className="flex min-w-0 flex-wrap items-baseline gap-x-1 text-xs break-words text-muted-foreground">
                      <span>{formatDate(quota.resetAt, true)} 重置</span>
                      <span aria-hidden="true">·</span>
                      <span>
                        {estimate
                          ? `估算 ${formatUsd(estimate.estimatedTotalMicrousd)}`
                          : "尚未估算"}
                      </span>
                    </div>
                  </div>
                )
              })}
            </div>
            <p className="text-xs leading-relaxed text-muted-foreground">
              本机估算，非官方账单。
            </p>
          </div>
        ) : (
          <Badge
            variant={quotaBadgeVariant(account)}
            className="h-auto max-w-full justify-start py-1 text-left leading-tight break-words whitespace-normal"
          >
            {quotaStatusText(account)}
          </Badge>
        )}
      </section>

      <section className="border-t pt-1.5">
        <div className="grid min-w-0 grid-cols-3 gap-2">
          <MaintenanceRecord
            label="上次刷新"
            value={formatDate(account.credentialRefresh.lastRefreshAt, true)}
          />
          <MaintenanceRecord
            label="上次检查"
            value={formatDate(account.credentialRefresh.lastCheckAt, true)}
          />
          <MaintenanceRecord
            label="资料更新"
            value={formatDate(account.updatedAt, true)}
          />
        </div>
      </section>
    </CardContent>
  )
}

function MaintenanceRecord({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex min-w-0 items-baseline gap-1">
      <span className="shrink-0 text-xs text-muted-foreground">{label}</span>
      <span className="min-w-0 font-medium break-words tabular-nums">
        {value}
      </span>
    </div>
  )
}

function shortLoginStatusText(account: OfficialAccountView) {
  switch (account.credentialRefresh.verification) {
    case "valid":
      return "登录有效"
    case "invalid":
      return "登录失效"
    case "workspace_or_permission":
      return "权限受限"
    case "check_failed":
      return "检查失败"
    default:
      return "未验证"
  }
}

function shortMaintenanceStatusText(account: OfficialAccountView) {
  switch (account.credentialRefresh.status) {
    case "healthy":
      return "维护正常"
    case "managed_by_codex":
      return "Codex 维护"
    case "waiting_retry":
      return "等待重试"
    case "reauthentication_required":
      return "需重新登录"
    case "not_refreshable":
      return "无法自动维护"
    default:
      return "等待维护"
  }
}

function loginVerificationVariant(
  account: OfficialAccountView
): StatusBadgeVariant {
  switch (account.credentialRefresh.verification) {
    case "valid":
      return "default"
    case "invalid":
    case "workspace_or_permission":
    case "check_failed":
      return "destructive"
    default:
      return "outline"
  }
}

function credentialRefreshVariant(
  account: OfficialAccountView
): StatusBadgeVariant {
  switch (account.credentialRefresh.status) {
    case "healthy":
    case "managed_by_codex":
      return "secondary"
    case "waiting_retry":
    case "unknown":
      return "outline"
    case "reauthentication_required":
    case "not_refreshable":
      return "destructive"
    default:
      return "outline"
  }
}

function quotaBadgeVariant(account: OfficialAccountView): StatusBadgeVariant {
  switch (account.quota.status) {
    case "unauthorized":
    case "error":
      return "destructive"
    case "never":
    case "rate_limited":
    case "unsupported":
    case "success":
    default:
      return "outline"
  }
}

function Detail({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="truncate font-medium">{value}</span>
    </div>
  )
}
