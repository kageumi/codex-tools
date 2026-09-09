import { useMemo, useRef, useState } from "react"
import {
  Login03Icon,
  Refresh01Icon,
  TestTube01Icon,
} from "@hugeicons/core-free-icons"

import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { ItemGroup } from "@/components/ui/item"
import {
  Sheet,
  SheetBody,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet"
import { Skeleton } from "@/components/ui/skeleton"
import { Spinner } from "@/components/ui/spinner"
import { hiddenOverlayStyles } from "@/components/ui/overlay-styles"
import { toast } from "@/components/ui/toast"
import { errorMessage, formatDate } from "@/lib/format"
import { useAsyncAction } from "@/hooks/use-async-action"
import { call } from "@/lib/ipc"
import type {
  OfficialAccountView,
  Provider,
  ProviderOverview,
  RepairResult,
} from "@/types"

import { AccountManagerDialog } from "./account-manager-dialog"
import { AccountRemarkDialog } from "./account-remark-dialog"
import {
  accountDescription,
  accountPlanText,
  accountIsExpired,
  accountWorkspaceIsDeactivated,
  buildFallbackCandidates,
  credentialMaintenanceMessage,
  emptyProvider,
  effectiveModelCount,
  quotaStatusText,
  repairWarning,
  switchActiveConnection,
  type ConnectionKind,
} from "./connection-utils"
import {
  ConnectionItem,
  EmptyConnectionItem,
  type PendingAction,
} from "./connection-item"
import {
  refreshAccountLogin,
  refreshAccountQuota,
  syncProviderModels,
  testProviderConnection,
} from "./connection-actions"
import {
  cloneProviderForEditing,
  ProviderEditorDialog,
} from "./provider-editor-dialog"
import { displayQuotaWindows } from "./quota-estimate"
import { resetCreditCountText } from "./reset-credits"

type ConnectionPage = "dashboard" | "providers"

type DeleteTarget = {
  active: boolean
  id: string
  kind: ConnectionKind
  name: string
}

const EMPTY_ACCOUNTS: OfficialAccountView[] = []
const EMPTY_PROVIDERS: Provider[] = []
const DELETE_NAME_MAX_LENGTH = 16

function truncateDeleteName(name: string) {
  const characters = Array.from(name)
  return characters.length > DELETE_NAME_MAX_LENGTH
    ? `${characters.slice(0, DELETE_NAME_MAX_LENGTH).join("")}…`
    : name
}

function accountTitle(account: OfficialAccountView) {
  return (
    account.remark.trim() ||
    account.name.trim() ||
    account.email.trim() ||
    "OpenAI 账号"
  )
}

function AccountConnectionDetails({
  account,
}: {
  account: OfficialAccountView
}) {
  const email = account.email.trim()
  const quotaWindows = displayQuotaWindows(account.quota).sort(
    (left, right) => left.windowSeconds - right.windowSeconds
  )

  return (
    <div className="flex min-w-0 flex-col gap-1.5">
      <div className="flex min-w-0 flex-wrap items-center gap-x-1.5 gap-y-0.5 text-xs text-muted-foreground">
        <span className="min-w-0 break-words whitespace-normal">
          {accountPlanText(account)}
        </span>
        {email && (
          <>
            <span aria-hidden="true">·</span>
            <span className="min-w-0 break-words whitespace-normal">
              {email}
            </span>
          </>
        )}
        <>
          <span aria-hidden="true">·</span>
          <span>{resetCreditCountText(account)}</span>
        </>
      </div>

      {quotaWindows.length ? (
        <div
          className={
            quotaWindows.length > 1
              ? "grid min-w-0 grid-cols-2 gap-1.5"
              : "grid min-w-0 gap-1.5"
          }
        >
          {quotaWindows.map((quota) => (
            <div
              key={`${quota.windowSeconds}-${quota.resetAt ?? "missing"}`}
              className="min-w-0 rounded-lg border border-border/70 bg-muted/30 px-2 py-1.5"
            >
              <div className="flex min-w-0 flex-wrap items-center gap-x-1.5 gap-y-0.5 text-xs">
                <Badge
                  variant="outline"
                  className="h-4 px-1.5 text-[11px] leading-none"
                >
                  {quota.label}
                </Badge>
                <span className="min-w-0 break-words whitespace-normal tabular-nums">
                  {quota.remainingPercent.toFixed(1)}% 可用
                </span>
              </div>
              <div className="mt-0.5 min-w-0 text-[11px] leading-relaxed break-words whitespace-normal text-muted-foreground">
                {quota.resetAt ? formatDate(quota.resetAt, true) : "—"} 重置
              </div>
            </div>
          ))}
        </div>
      ) : (
        <p className="min-w-0 text-xs leading-relaxed break-words whitespace-normal text-muted-foreground">
          {quotaStatusText(account)}
        </p>
      )}
    </div>
  )
}

export function ConnectionManagerSheet({
  open,
  onOpenChange,
  page,
  connections,
  selectedId,
  onSelectedIdChange,
  onChanged,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  page: ConnectionPage
  connections?: ProviderOverview
  selectedId?: string
  onSelectedIdChange: (id: string) => void
  onChanged: () => void
}) {
  const { busy: pending, begin, end } = useAsyncAction<PendingAction>()
  const [remarkAccount, setRemarkAccount] = useState<OfficialAccountView>()
  const [providerDraft, setProviderDraft] = useState<Provider>(emptyProvider())
  const [providerEditorOpen, setProviderEditorOpen] = useState(false)
  const latestSavedProviders = useRef(new Map<string, Provider>())
  const [deleteTarget, setDeleteTarget] = useState<DeleteTarget>()
  const [accountManagerOpen, setAccountManagerOpen] = useState(false)

  const accounts = connections?.officialAccounts ?? EMPTY_ACCOUNTS
  const providers = connections?.providers ?? EMPTY_PROVIDERS
  const frozen = Boolean(pending)
  const childOverlayOpen = Boolean(
    remarkAccount || providerEditorOpen || deleteTarget || accountManagerOpen
  )

  const runAction = async <T,>(
    action: PendingAction["action"],
    id: string,
    task: () => Promise<T>,
    success: string,
    successDescription?: (result: T) => string | undefined
  ) => {
    const key: PendingAction = { action, id }
    if (!begin(key)) return false
    try {
      const result = await task()
      onChanged()
      toast.add({
        title: success,
        description: successDescription?.(result),
        type: "success",
      })
      return true
    } catch (reason) {
      toast.add({
        title: "操作失败",
        description: errorMessage(reason),
        type: "error",
      })
      return false
    } finally {
      end(key)
    }
  }

  const activate = async (kind: ConnectionKind, id: string) => {
    const item =
      kind === "account"
        ? accounts.find((account) => account.id === id)
        : providers.find((provider) => provider.id === id)
    if (!item || item.active || pending) return
    const activated = await runAction(
      "activate",
      id,
      () =>
        kind === "account"
          ? call("connections_activate_account", { id })
          : call("connections_activate", { id }),
      "连接已切换",
      repairWarning
    )
    if (activated) onSelectedIdChange(id)
  }

  const editAccount = (account: OfficialAccountView) => {
    if (pending) return
    setRemarkAccount(account)
  }

  const editProvider = (provider: Provider) => {
    if (pending) return
    const cached = latestSavedProviders.current.get(provider.id)
    const latest =
      cached && cached.updatedAt >= provider.updatedAt ? cached : provider
    setProviderDraft(cloneProviderForEditing(latest))
    setProviderEditorOpen(true)
  }

  const excludedIdsFor = (target: DeleteTarget) =>
    target.kind === "account" ? new Set([target.id]) : new Set<string>()

  const requestDelete = (target: DeleteTarget) => {
    if (pending) return
    const excludedProviderId =
      target.kind === "provider" ? target.id : undefined
    const candidates = buildFallbackCandidates(
      accounts,
      providers,
      excludedIdsFor(target),
      excludedProviderId
    )
    if (target.active && candidates.length === 0) {
      toast.add({
        title: `无法删除“${target.name}”`,
        description: "至少保留一个可用连接，才能删除当前连接。",
        type: "error",
      })
      return
    }
    setDeleteTarget(target)
  }

  const deleteConnection = async () => {
    const target = deleteTarget
    if (!target) return
    const key: PendingAction = { action: "delete", id: target.id }
    if (!begin(key)) return

    let switchedId: string | undefined
    let switchRepair: RepairResult | undefined
    let lastSwitchError: unknown
    try {
      if (target.active) {
        const excludedProviderId =
          target.kind === "provider" ? target.id : undefined
        const switchResult = await switchActiveConnection(
          buildFallbackCandidates(
            accounts,
            providers,
            excludedIdsFor(target),
            excludedProviderId
          )
        )
        switchedId = switchResult.switchedId
        switchRepair = switchResult.repair
        lastSwitchError = switchResult.error

        if (!switchedId) {
          onChanged()
          toast.add({
            title: "无法切换连接，未执行删除",
            description: lastSwitchError
              ? `其余连接均无法启用：${errorMessage(lastSwitchError)}`
              : "至少保留一个可用连接，才能删除当前连接。",
            type: "error",
          })
          setDeleteTarget(undefined)
          return
        }
      }

      await (target.kind === "account"
        ? call("connections_delete_accounts", { ids: [target.id] })
        : call("connections_delete_provider", { id: target.id }))

      if (!selectedId || selectedId === target.id) {
        const remaining = [
          ...accounts.filter(
            (account) =>
              !(target.kind === "account" && account.id === target.id)
          ),
          ...providers.filter(
            (provider) =>
              !(target.kind === "provider" && provider.id === target.id)
          ),
        ]
        const nextId =
          switchedId ??
          remaining.find((connection) => connection.active)?.id ??
          remaining[0]?.id ??
          ""
        onSelectedIdChange(nextId)
      }

      onChanged()
      toast.add({
        title: `已删除“${target.name}”`,
        description: switchRepair ? repairWarning(switchRepair) : undefined,
        type: "success",
      })
      setDeleteTarget(undefined)
    } catch (reason) {
      onChanged()
      toast.add({
        title: switchedId ? "已切换连接，但删除失败" : "删除连接失败",
        description: errorMessage(reason),
        type: "error",
      })
      setDeleteTarget(undefined)
    } finally {
      end(key)
    }
  }

  const requestSheetOpenChange = (nextOpen: boolean) => {
    if (!nextOpen && pending) return
    onOpenChange(nextOpen)
  }

  const openBatchManager = () => {
    if (pending || accounts.length === 0) return
    onOpenChange(false)
    setAccountManagerOpen(true)
  }

  const accountRows = useMemo(
    () =>
      accounts.map((account) => ({
        account,
        expired: accountIsExpired(account),
        deactivated: accountWorkspaceIsDeactivated(account),
      })),
    [accounts]
  )
  const deleteTargetName = deleteTarget?.name ?? "这个连接"
  const deleteTargetLabel = truncateDeleteName(deleteTargetName)

  return (
    <>
      <Sheet open={open} onOpenChange={requestSheetOpenChange}>
        <SheetContent
          side="left"
          showCloseButton={!frozen}
          overlayClassName={childOverlayOpen ? hiddenOverlayStyles : undefined}
          className="data-[side=left]:w-96!"
          aria-busy={frozen}
        >
          <SheetHeader>
            <SheetTitle>账号与服务</SheetTitle>
          </SheetHeader>

          <SheetBody className="gap-4">
            {!connections && <ConnectionListSkeleton />}

            {connections && (
              <>
                <section
                  className="flex flex-col gap-1.5"
                  aria-labelledby="account-group-title"
                >
                  <div className="flex items-center justify-between gap-2 px-1">
                    <h3
                      id="account-group-title"
                      className="text-xs font-medium text-muted-foreground"
                    >
                      OpenAI 账号
                    </h3>
                    <Button
                      type="button"
                      size="xs"
                      variant="ghost"
                      disabled={frozen || accounts.length === 0}
                      onClick={openBatchManager}
                    >
                      批量管理
                    </Button>
                  </div>
                  <ItemGroup>
                    {accountRows.length === 0 && (
                      <EmptyConnectionItem label="暂无 OpenAI 账号" />
                    )}
                    {accountRows.map(({ account, expired, deactivated }) => (
                      <ConnectionItem
                        key={account.id}
                        kind="account"
                        id={account.id}
                        name={accountTitle(account)}
                        description={accountDescription(account)}
                        details={<AccountConnectionDetails account={account} />}
                        active={account.active}
                        canView={page === "providers"}
                        selected={
                          page === "providers" && selectedId === account.id
                        }
                        unavailable={
                          deactivated ||
                          expired ||
                          account.quota.status === "unauthorized"
                        }
                        unavailableLabel={
                          deactivated ? "工作区已停用" : "登录失效"
                        }
                        activateDisabled={deactivated}
                        frozen={frozen}
                        pending={pending}
                        onView={() => {
                          onSelectedIdChange(account.id)
                          onOpenChange(false)
                        }}
                        onActivate={() => void activate("account", account.id)}
                        onEdit={() => editAccount(account)}
                        onDelete={() =>
                          requestDelete({
                            active: account.active,
                            id: account.id,
                            kind: "account",
                            name: accountTitle(account),
                          })
                        }
                        moreActions={[
                          {
                            label: "刷新额度",
                            icon: Refresh01Icon,
                            onSelect: () =>
                              void runAction(
                                "quota",
                                account.id,
                                () => refreshAccountQuota(account.id),
                                "额度已刷新"
                              ),
                          },
                          {
                            label: "立即刷新登录",
                            icon: Login03Icon,
                            onSelect: () =>
                              void runAction(
                                "login",
                                account.id,
                                () => refreshAccountLogin(account.id),
                                "登录维护已完成",
                                credentialMaintenanceMessage
                              ),
                          },
                        ]}
                      />
                    ))}
                  </ItemGroup>
                </section>

                <section
                  className="flex flex-col gap-1.5"
                  aria-labelledby="provider-group-title"
                >
                  <h3
                    id="provider-group-title"
                    className="px-1 text-xs font-medium text-muted-foreground"
                  >
                    API 服务
                  </h3>
                  <ItemGroup>
                    {providers.length === 0 && (
                      <EmptyConnectionItem label="暂无 API 服务" />
                    )}
                    {providers.map((provider) => {
                      const modelCount = effectiveModelCount(provider)
                      return (
                        <ConnectionItem
                          key={provider.id}
                          kind="provider"
                          id={provider.id}
                          name={provider.name}
                          description={`${
                            modelCount ? `${modelCount} 个模型` : "模型尚未同步"
                          } · ${provider.baseUrl}`}
                          active={provider.active}
                          canView={page === "providers"}
                          selected={
                            page === "providers" && selectedId === provider.id
                          }
                          unavailable={!provider.enabled || !provider.hasApiKey}
                          unavailableLabel={
                            provider.enabled ? "缺少密钥" : "已停用"
                          }
                          activateDisabled={
                            !provider.enabled || !provider.hasApiKey
                          }
                          frozen={frozen}
                          pending={pending}
                          onView={() => {
                            onSelectedIdChange(provider.id)
                            onOpenChange(false)
                          }}
                          onActivate={() =>
                            void activate("provider", provider.id)
                          }
                          onEdit={() => editProvider(provider)}
                          onDelete={() =>
                            requestDelete({
                              active: provider.active,
                              id: provider.id,
                              kind: "provider",
                              name: provider.name,
                            })
                          }
                          moreActions={[
                            {
                              label: "测试连接",
                              icon: TestTube01Icon,
                              onSelect: () =>
                                void runAction(
                                  "test",
                                  provider.id,
                                  () => testProviderConnection(provider.id),
                                  "连接测试通过"
                                ),
                            },
                            {
                              label: "同步模型",
                              icon: Refresh01Icon,
                              onSelect: () =>
                                void runAction(
                                  "models",
                                  provider.id,
                                  () => syncProviderModels(provider.id),
                                  "模型已同步"
                                ),
                            },
                          ]}
                        />
                      )
                    })}
                  </ItemGroup>
                </section>
              </>
            )}
          </SheetBody>
        </SheetContent>
      </Sheet>

      <AccountRemarkDialog
        key={remarkAccount?.id ?? "closed"}
        account={remarkAccount}
        open={Boolean(remarkAccount)}
        onOpenChange={(nextOpen) => {
          if (!nextOpen) setRemarkAccount(undefined)
        }}
        onSaved={onChanged}
      />

      <ProviderEditorDialog
        open={providerEditorOpen}
        onOpenChange={setProviderEditorOpen}
        provider={providerDraft}
        onProviderChange={setProviderDraft}
        onSaved={(saved) => {
          latestSavedProviders.current.set(saved.id, saved)
          setProviderDraft(saved)
          setProviderEditorOpen(false)
          onChanged()
        }}
      />

      <AlertDialog
        open={Boolean(deleteTarget)}
        onOpenChange={(nextOpen) => {
          if (!nextOpen && pending) return
          if (!nextOpen) setDeleteTarget(undefined)
        }}
      >
        <AlertDialogContent aria-busy={pending?.action === "delete"}>
          <AlertDialogHeader>
            <AlertDialogTitle
              className="max-w-full min-w-0 truncate"
              title={`删除“${deleteTargetName}”？`}
            >
              删除“{deleteTargetLabel}”？
            </AlertDialogTitle>
            <AlertDialogDescription>
              {deleteTarget?.active
                ? "这是当前连接。程序会先切换到其他可用连接，再执行删除。"
                : "此操作会移除本机保存的连接与凭据信息，无法撤销。"}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={pending?.action === "delete"}>
              取消
            </AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              className="max-w-[min(16rem,100%)] min-w-0"
              disabled={pending?.action === "delete"}
              onClick={() => void deleteConnection()}
              title={`删除“${deleteTargetName}”`}
            >
              {pending?.action === "delete" && (
                <Spinner data-icon="inline-start" />
              )}
              <span className="min-w-0 truncate">
                删除“{deleteTargetLabel}”
              </span>
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AccountManagerDialog
        open={accountManagerOpen}
        onOpenChange={setAccountManagerOpen}
        accounts={accounts}
        providers={providers}
        initialSelectedIds={[]}
        selectedId={selectedId}
        onSelectedIdChange={onSelectedIdChange}
        onRefresh={onChanged}
      />
    </>
  )
}

function ConnectionListSkeleton() {
  return (
    <div className="flex flex-col gap-4" aria-label="正在读取连接">
      <div className="flex flex-col gap-2">
        <Skeleton className="h-3 w-20" />
        {[0, 1].map((item) => (
          <div key={item} className="flex items-start gap-2 px-1 py-1.5">
            <Skeleton className="size-6 shrink-0 rounded-full" />
            <div className="flex min-w-0 flex-1 flex-col gap-1.5">
              <Skeleton className="h-3.5 w-32" />
              <Skeleton className="h-3 w-40" />
              <div className="grid grid-cols-2 gap-1.5">
                {[0, 1].map((quota) => (
                  <div
                    key={quota}
                    className="flex min-w-0 flex-col gap-1 rounded-lg border border-border/70 p-1.5"
                  >
                    <Skeleton className="h-3 w-14" />
                    <Skeleton className="h-3 w-full" />
                  </div>
                ))}
              </div>
            </div>
          </div>
        ))}
      </div>
      <div className="flex flex-col gap-2">
        <Skeleton className="h-3 w-20" />
        {[0, 1].map((item) => (
          <div key={item} className="flex items-center gap-2 px-1 py-1.5">
            <Skeleton className="size-6 rounded-full" />
            <div className="flex min-w-0 flex-1 flex-col gap-1.5">
              <Skeleton className="h-3.5 w-24" />
              <Skeleton className="h-3 w-full" />
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
