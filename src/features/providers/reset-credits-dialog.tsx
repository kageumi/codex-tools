import { useEffect, useRef, useState } from "react"
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
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogBody,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Spinner } from "@/components/ui/spinner"
import { errorMessage, formatDate } from "@/lib/format"
import { call } from "@/lib/ipc"
import type {
  OfficialAccountView,
  ResetCredit,
  ResetCreditConsumeOutcome,
  ResetCreditDetails,
} from "@/types"

type CachedDetails = { revision: number | null; details: ResetCreditDetails }
const detailCache = new Map<string, CachedDetails>()
const detailInflight = new Map<string, Promise<ResetCreditDetails>>()
const operationKeys = new Map<string, string>()
const unknownOperations = new Set<string>()
const operationId = (accountId: string, creditId: string) =>
  `${accountId}:${creditId}`

function loadDetails(accountId: string, revision: number | null) {
  const cached = detailCache.get(accountId)
  if (cached?.revision === revision) return Promise.resolve(cached.details)
  const pending = detailInflight.get(accountId)
  if (pending) return pending
  const request = call("connections_get_reset_credits", { accountId }).then(
    (details) => {
      detailCache.set(accountId, { revision, details })
      return details
    }
  )
  detailInflight.set(accountId, request)
  return request.finally(() => detailInflight.delete(accountId))
}
const creditTitle = (credit: ResetCredit) =>
  credit.title?.trim() || credit.resetType?.trim() || "重置卡"
const creditIsUsable = (credit: ResetCredit) =>
  credit.status === "available" &&
  (credit.expiresAt == null || credit.expiresAt * 1000 > Date.now())
function statusText(credit: ResetCredit) {
  if (credit.expiresAt != null && credit.expiresAt * 1000 <= Date.now())
    return "已过期"
  return (
    (
      {
        available: "可使用",
        redeemed: "已使用",
        redeeming: "使用中",
        expired: "已过期",
      } as Record<string, string>
    )[credit.status ?? ""] ?? "状态未知"
  )
}
function outcomeText(outcome: ResetCreditConsumeOutcome) {
  return {
    reset: "重置已提交并由服务端确认。",
    already_redeemed: "该卡此前已使用；已刷新服务端状态。",
    nothing_to_reset: "服务端没有可重置的额度。",
    no_credit: "服务端未接受这张重置卡。",
    failed: "服务端未完成重置。",
    unknown: "服务端返回了未知结果；未自动重试。",
  }[outcome]
}
export function ResetCreditsDialog({
  account,
  open,
  onOpenChange,
  onChanged,
}: {
  account?: OfficialAccountView
  open: boolean
  onOpenChange: (open: boolean) => void
  onChanged: () => void
}) {
  const [details, setDetails] = useState<ResetCreditDetails>()
  const [loading, setLoading] = useState(false)
  const [loadError, setLoadError] = useState<string>()
  const [resultMessage, setResultMessage] = useState<string>()
  const [confirming, setConfirming] = useState<ResetCredit>()
  const [using, setUsing] = useState(false)
  const usingRef = useRef(false)
  const useButton = useRef<HTMLButtonElement | null>(null)
  const onChangedRef = useRef(onChanged)
  const accountId = account?.id
  const revision = account?.quota.fetchedAt ?? null
  useEffect(() => {
    onChangedRef.current = onChanged
  }, [onChanged])
  useEffect(() => {
    if (!open || !accountId) return
    let cancelled = false
    const cached = detailCache.get(accountId)
    void Promise.resolve().then(() => {
      if (cancelled) return
      setDetails(cached?.revision === revision ? cached.details : undefined)
      setLoadError(undefined)
      setLoading(cached?.revision !== revision)
    })
    void loadDetails(accountId, revision)
      .then(
        (next) => {
          if (cancelled) return
          setDetails(next)
          onChangedRef.current()
        },
        (reason) => !cancelled && setLoadError(errorMessage(reason))
      )
      .finally(() => !cancelled && setLoading(false))
    return () => {
      cancelled = true
    }
  }, [accountId, open, revision])
  const restoreFocus = () =>
    requestAnimationFrame(() => useButton.current?.focus())
  const closeConfirmation = () => {
    if (!usingRef.current) {
      setConfirming(undefined)
      restoreFocus()
    }
  }
  const consume = async () => {
    if (!account || !confirming || usingRef.current) return
    const key = operationId(account.id, confirming.id)
    if (unknownOperations.has(key)) return
    const idempotencyKey = operationKeys.get(key) ?? crypto.randomUUID()
    operationKeys.set(key, idempotencyKey)
    usingRef.current = true
    setUsing(true)
    setResultMessage(undefined)
    try {
      const result = await call("connections_consume_reset_credit", {
        accountId: account.id,
        creditId: confirming.id,
        idempotencyKey,
      })
      detailCache.set(account.id, {
        revision: result.quota?.fetchedAt ?? revision,
        details: result.details,
      })
      setDetails(result.details)
      setConfirming(undefined)
      if (result.outcome === "unknown") {
        unknownOperations.add(key)
      } else {
        operationKeys.delete(key)
      }
      setResultMessage(
        [outcomeText(result.outcome), result.refreshError]
          .filter(Boolean)
          .join(" ")
      )
      onChanged()
      restoreFocus()
    } catch (reason) {
      unknownOperations.add(key)
      setConfirming(undefined)
      setResultMessage(`${errorMessage(reason)} 结果可能未知，未自动重试。`)
      restoreFocus()
    } finally {
      usingRef.current = false
      setUsing(false)
    }
  }
  const visibleDetails =
    details?.accountId === account?.id ? details : undefined
  const credits = [...(visibleDetails?.credits ?? [])].sort(
    (a, b) =>
      (a.expiresAt ?? Number.MAX_SAFE_INTEGER) -
      (b.expiresAt ?? Number.MAX_SAFE_INTEGER)
  )
  const title =
    account?.remark.trim() || account?.name || account?.email || "OpenAI 账号"
  return (
    <>
      <Dialog
        open={open}
        onOpenChange={(next) => {
          if (next || (!confirming && !using)) onOpenChange(next)
        }}
      >
        <DialogContent
          aria-busy={loading || using}
          showCloseButton={!confirming && !using}
        >
          <DialogHeader>
            <DialogTitle>重置卡</DialogTitle>
            <DialogDescription>所属账号：{title}</DialogDescription>
          </DialogHeader>
          <DialogBody>
            {loading ? (
              <div className="flex items-center gap-2 py-6 text-sm text-muted-foreground">
                <Spinner /> 正在读取重置卡…
              </div>
            ) : loadError && !details ? (
              <p className="text-sm text-destructive">{loadError}</p>
            ) : (
              <div className="flex flex-col gap-3">
                <p className="text-sm text-muted-foreground">
                  {visibleDetails?.summary.availableCount == null
                    ? "服务端未提供可用重置卡数量。"
                    : `服务端可用数量：${visibleDetails.summary.availableCount} 张`}
                </p>
                {visibleDetails?.summary.detailsStatus === "partial" && (
                  <p className="rounded-lg border border-border/70 bg-muted/40 px-3 py-2 text-xs leading-relaxed text-muted-foreground">
                    服务端仅提供了部分卡片详情；数量以上方服务端摘要为准。
                  </p>
                )}
                {resultMessage && (
                  <p className="text-sm text-muted-foreground">
                    {resultMessage}
                  </p>
                )}
                {loadError && (
                  <p className="text-sm text-destructive">{loadError}</p>
                )}
                <div className="max-h-[min(50vh,26rem)] overflow-y-auto pr-1">
                  <div className="flex flex-col gap-2">
                    {credits.map((credit) => {
                      const key = account
                        ? operationId(account.id, credit.id)
                        : credit.id
                      return (
                        <div
                          key={credit.id}
                          className="flex min-w-0 items-center justify-between gap-3 rounded-lg border border-border/70 px-3 py-2.5"
                        >
                          <div className="min-w-0">
                            <div className="flex flex-wrap items-center gap-2">
                              <span className="font-medium break-words">
                                {creditTitle(credit)}
                              </span>
                              <Badge variant="outline">
                                {statusText(credit)}
                              </Badge>
                            </div>
                            <p className="mt-1 text-xs text-muted-foreground">
                              到期：
                              {credit.expiresAt == null
                                ? "未提供到期时间"
                                : formatDate(credit.expiresAt, true)}
                            </p>
                            {credit.description && (
                              <p className="mt-1 text-xs text-muted-foreground">
                                {credit.description}
                              </p>
                            )}
                          </div>
                          <Button
                            type="button"
                            size="sm"
                            disabled={
                              !creditIsUsable(credit) ||
                              unknownOperations.has(key) ||
                              using
                            }
                            onClick={(event) => {
                              useButton.current = event.currentTarget
                              setConfirming(credit)
                            }}
                          >
                            使用
                          </Button>
                        </div>
                      )
                    })}
                    {!credits.length && (
                      <p className="py-6 text-center text-sm text-muted-foreground">
                        暂无可展示的重置卡详情。
                      </p>
                    )}
                  </div>
                </div>
              </div>
            )}
          </DialogBody>
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              disabled={Boolean(confirming) || using}
              onClick={() => onOpenChange(false)}
            >
              关闭
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <AlertDialog
        open={Boolean(confirming)}
        onOpenChange={(next) => !next && closeConfirmation()}
      >
        <AlertDialogContent aria-busy={using}>
          <AlertDialogHeader>
            <AlertDialogTitle>确认使用重置卡？</AlertDialogTitle>
            <AlertDialogDescription>
              <span className="block">账号：{title}</span>
              <span className="block">
                卡片：{confirming ? creditTitle(confirming) : ""}
              </span>
              <span className="block">
                到期：
                {confirming?.expiresAt == null
                  ? "未提供到期时间"
                  : formatDate(confirming.expiresAt, true)}
              </span>
              <span className="mt-2 block">此操作不可撤销。</span>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={using} onClick={closeConfirmation}>
              取消
            </AlertDialogCancel>
            <AlertDialogAction disabled={using} onClick={() => void consume()}>
              {using && <Spinner data-icon="inline-start" />}确认使用
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}
