import { useCallback, useEffect, useRef, useState } from "react"
import { isTauri } from "@tauri-apps/api/core"
import {
  Database01Icon,
  Folder01Icon,
  InformationCircleIcon,
  Wrench01Icon,
} from "@hugeicons/core-free-icons"
import { HugeiconsIcon } from "@hugeicons/react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
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
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty"
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemGroup,
  ItemMedia,
  ItemTitle,
} from "@/components/ui/item"
import {
  Pagination,
  PaginationContent,
  PaginationItem,
  PaginationNext,
  PaginationPrevious,
} from "@/components/ui/pagination"
import { Skeleton } from "@/components/ui/skeleton"
import { Spinner } from "@/components/ui/spinner"
import { toast } from "@/components/ui/toast"
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { errorMessage, formatDate, formatInteger } from "@/lib/format"
import { useAsync } from "@/hooks/use-async"
import { call } from "@/lib/ipc"
import type {
  RepairTarget,
  Session,
  SessionAction,
  SessionDeletePreview,
  SessionSyncStatus,
} from "@/types"

const pageSize = 6

export function SessionsPage({
  refreshRevision,
  query,
  onRefresh,
}: {
  refreshRevision: number
  onRefresh: () => void
  query: string
}) {
  const [page, setPage] = useState(1)
  const [status, setStatus] = useState<"active" | "archived">("active")
  const [repairTarget, setRepairTarget] = useState<RepairTarget>()
  const [busy, setBusy] = useState(false)
  const [syncRevision, setSyncRevision] = useState(0)
  const [statusRevision, setStatusRevision] = useState(0)
  const [syncSnapshot, setSyncSnapshot] = useState<{
    key: string
    status?: SessionSyncStatus
    error?: string
  }>()
  const [listenerError, setListenerError] = useState<string>()
  const [operation, setOperation] = useState<{
    threadId: string
    action: SessionAction
  }>()
  const [deleteTarget, setDeleteTarget] = useState<Session>()
  const [deletePreview, setDeletePreview] = useState<SessionDeletePreview>()
  const [deleteError, setDeleteError] = useState<string>()
  const [preparingDelete, setPreparingDelete] = useState(false)
  const operationLock = useRef(false)
  const cancelDeleteRef = useRef<HTMLButtonElement>(null)
  const refreshTimer = useRef<ReturnType<typeof setTimeout> | undefined>(
    undefined
  )
  const notificationCount = useRef(0)
  const pendingRefreshKind = useRef<"catalog" | "status">("status")
  const loadedSyncRevision = useRef(syncRevision)
  const loadedRefreshRevision = useRef(refreshRevision)

  const queueRefresh = useCallback((kind: "catalog" | "status" = "catalog") => {
    if (kind === "catalog") pendingRefreshKind.current = "catalog"
    if (refreshTimer.current !== undefined) return
    refreshTimer.current = setTimeout(() => {
      refreshTimer.current = undefined
      const pending = pendingRefreshKind.current
      pendingRefreshKind.current = "status"
      if (pending === "catalog") setSyncRevision((value) => value + 1)
      else setStatusRevision((value) => value + 1)
    }, 120)
  }, [])

  useEffect(() => {
    let disposed = false
    let unlisten: (() => void) | undefined
    if (isTauri()) {
      void import("@tauri-apps/api/event")
        .then(({ listen }) =>
          listen<{ kind?: string }>("sessions-changed", (event) => {
            if (disposed) return
            if (
              event?.payload?.kind === "runtime" ||
              event?.payload?.kind === "connection"
            ) {
              queueRefresh("status")
            } else {
              notificationCount.current += 1
              queueRefresh("catalog")
            }
          })
        )
        .then((stop) => {
          if (disposed) stop()
          else unlisten = stop
        })
        .catch((reason) => {
          if (!disposed) setListenerError(errorMessage(reason))
        })
    }
    return () => {
      disposed = true
      unlisten?.()
      clearTimeout(refreshTimer.current)
      refreshTimer.current = undefined
      pendingRefreshKind.current = "status"
    }
  }, [queueRefresh])

  const fetchList = useCallback(() => {
    const forceRefresh =
      refreshRevision !== loadedRefreshRevision.current ||
      syncRevision !== loadedSyncRevision.current
    loadedRefreshRevision.current = refreshRevision
    loadedSyncRevision.current = syncRevision
    return call("sessions_list", {
      query,
      page,
      pageSize,
      refresh: forceRefresh,
      status,
    })
  }, [page, query, refreshRevision, status, syncRevision])
  const { data: result, error: listError } = useAsync(fetchList, {
    onError: (message) =>
      toast.add({
        title: "无法读取会话列表",
        description: message,
        type: "error",
      }),
    onSuccess: (nextResult) => {
      if (nextResult.page !== page) setPage(nextResult.page)
    },
  })

  const threadIdsKey = result
    ? JSON.stringify(
        [...new Set(result.items.map((session) => session.id))].sort()
      )
    : undefined
  const syncKey = `${threadIdsKey}:${syncRevision}:${statusRevision}:${refreshRevision}`
  const syncStatus =
    syncSnapshot?.key === syncKey ? syncSnapshot.status : undefined
  const syncError =
    listenerError ??
    (syncSnapshot?.key === syncKey ? syncSnapshot.error : undefined)
  useEffect(() => {
    if (threadIdsKey === undefined) return
    let cancelled = false
    void call("sessions_sync_status", {
      threadIds: JSON.parse(threadIdsKey) as string[],
    })
      .then((next) => {
        if (!cancelled) setSyncSnapshot({ key: syncKey, status: next })
      })
      .catch((reason) => {
        if (!cancelled)
          setSyncSnapshot({ key: syncKey, error: errorMessage(reason) })
      })
    return () => {
      cancelled = true
    }
  }, [threadIdsKey, syncKey])

  const disabledReason = (session: Session) => {
    if (operation || preparingDelete) return "正在处理会话操作，请稍候。"
    if (syncError) return syncError
    if (!syncStatus) return "正在检查 Codex 连接和会话运行状态。"
    if (!syncStatus.connected)
      return syncStatus.message || "请完全退出 Codex，再从 CodexTools 启动。"
    const thread = syncStatus.threads.find(
      (entry) => entry.threadId === session.id
    )
    if (thread?.blockedReason) return thread.blockedReason
    if (thread?.state === "running") return "会话正在运行，请等待完成后再操作。"
    if (thread?.state !== "idle")
      return "暂时无法确认会话运行状态，请刷新后再试。"
    return undefined
  }

  const mutateSession = async (session: Session, action: SessionAction) => {
    if (operationLock.current) return
    operationLock.current = true
    setOperation({ threadId: session.id, action })
    setDeleteError(undefined)
    const observedNotifications = notificationCount.current
    let failed = false
    try {
      const response = await call("sessions_mutate", {
        threadId: session.id,
        action,
        ...(action === "delete"
          ? {
              confirmedThreadIds: deletePreview?.affectedThreads.map(
                (thread) => thread.id
              ),
            }
          : {}),
      })
      toast.add({
        title:
          action === "delete"
            ? "会话已删除"
            : action === "archive"
              ? "会话已归档"
              : "已取消归档",
        description: response.message,
        type: "success",
      })
      if (action === "delete") setDeleteTarget(undefined)
      if (notificationCount.current === observedNotifications) queueRefresh()
    } catch (reason) {
      failed = true
      const message = errorMessage(reason)
      if (action === "delete") setDeleteError(message)
      toast.add({ title: "会话操作失败", description: message, type: "error" })
    } finally {
      operationLock.current = false
      setOperation(undefined)
      if (failed) queueRefresh("status")
    }
  }

  const prepareDelete = async (session: Session) => {
    if (operationLock.current) return
    operationLock.current = true
    setDeleteTarget(session)
    setDeletePreview(undefined)
    setDeleteError(undefined)
    setPreparingDelete(true)
    try {
      setDeletePreview(
        await call("sessions_prepare_delete", { threadId: session.id })
      )
    } catch (reason) {
      setDeleteError(errorMessage(reason))
    } finally {
      operationLock.current = false
      setPreparingDelete(false)
    }
  }

  const fetchScan = useCallback(() => call("sessions_scan"), [])
  const { data: scan, error: scanError } = useAsync(
    fetchScan,
    {
      onError: (message) =>
        toast.add({
          title: "无法扫描会话",
          description: message,
          type: "error",
        }),
    },
    `${refreshRevision}:${syncRevision}`
  )

  const repair = async () => {
    if (!repairTarget || !scan) return
    setBusy(true)
    try {
      const response = await call("sessions_repair", {
        targetProvider: scan.currentProvider,
      })
      const partial =
        response.repairComplete === false ||
        response.filesFailed > 0 ||
        response.warnings.length > 0
      toast.add({
        title: partial ? "会话修复已完成，但有部分警告" : "会话归属已修复",
        description:
          response.warnings[0] ?? `已更新 ${response.rowsUpdated} 条记录。`,
        type: partial ? "warning" : "success",
      })
      setRepairTarget(undefined)
      onRefresh()
    } catch (reason) {
      toast.add({
        title: "修复失败",
        description: errorMessage(reason),
        type: "error",
      })
    } finally {
      setBusy(false)
    }
  }

  if ((!result && listError) || (!scan && scanError))
    return (
      <div className="flex min-h-full flex-col gap-3 px-3 pt-1 pb-3">
        {listError && (
          <Alert variant="destructive">
            <HugeiconsIcon icon={InformationCircleIcon} />
            <AlertTitle>无法读取会话列表</AlertTitle>
            <AlertDescription>{listError}</AlertDescription>
          </Alert>
        )}
        {scanError && (
          <Alert variant="destructive">
            <HugeiconsIcon icon={InformationCircleIcon} />
            <AlertTitle>无法扫描会话</AlertTitle>
            <AlertDescription>{scanError}</AlertDescription>
          </Alert>
        )}
      </div>
    )

  if (!result || !scan)
    return (
      <div
        className="grid min-h-full grid-rows-[minmax(72px,auto)_minmax(256px,1fr)] gap-3 px-3 pt-1 pb-3"
        role="status"
        aria-busy="true"
      >
        <span className="sr-only">正在读取会话</span>
        <Skeleton className="rounded-2xl" />
        <Skeleton className="rounded-2xl" />
      </div>
    )
  const pageCount = Math.max(1, Math.ceil(result.total / pageSize))
  const currentPage = result.page
  const repairTargets = scan.targets.filter(
    (target) => !target.current && target.count > 0
  )
  const repairCount = repairTargets.reduce(
    (total, target) => total + target.count,
    0
  )
  const combinedRepairTarget: RepairTarget = {
    id: "all",
    current: false,
    count: repairCount,
    sources: [...new Set(repairTargets.flatMap((target) => target.sources))],
  }

  return (
    <div className="flex min-h-full flex-col gap-3 px-3 pt-1 pb-2">
      {listError && (
        <Alert variant="destructive">
          <HugeiconsIcon icon={InformationCircleIcon} />
          <AlertTitle>会话列表刷新失败</AlertTitle>
          <AlertDescription>{listError}</AlertDescription>
        </Alert>
      )}
      {scanError && (
        <Alert variant="destructive">
          <HugeiconsIcon icon={InformationCircleIcon} />
          <AlertTitle>会话扫描失败</AlertTitle>
          <AlertDescription>{scanError}</AlertDescription>
        </Alert>
      )}
      {scan.warnings.length > 0 && (
        <Alert>
          <HugeiconsIcon icon={InformationCircleIcon} />
          <AlertTitle>部分会话数据未能完整扫描</AlertTitle>
          <AlertDescription>
            {scan.warnings[0] ?? "扫描返回了未分类警告。"}
            {scan.warnings.length > 1
              ? `（另有 ${scan.warnings.length - 1} 条警告）`
              : ""}
          </AlertDescription>
        </Alert>
      )}
      <Card
        size="sm"
        className="min-h-64 shrink-0 gap-3 data-[size=sm]:[--card-spacing:--spacing(3)]"
      >
        <CardHeader className="border-b pb-2!">
          <div className="flex items-center gap-2">
            <CardTitle>会话</CardTitle>
            <ToggleGroup
              variant="outline"
              spacing={0}
              value={[status]}
              onValueChange={(value) => {
                if (!value[0]) return
                setStatus(value[0] as "active" | "archived")
                setPage(1)
              }}
            >
              <ToggleGroupItem value="active">活跃</ToggleGroupItem>
              <ToggleGroupItem value="archived">已归档</ToggleGroupItem>
            </ToggleGroup>
          </div>
          <CardDescription className="col-span-full flex min-w-0 items-center justify-end gap-3 text-xs">
            <span
              className="truncate text-right"
              role="status"
              title={syncError ?? syncStatus?.message}
            >
              {syncError ?? syncStatus?.message ?? "正在检查 Codex 同步连接…"}
            </span>
          </CardDescription>
          <CardAction className="flex items-center gap-1.5">
            <Badge variant="outline" title="已索引会话">
              索引 {formatInteger(scan.sessionMetaCount)}
            </Badge>
            <Badge variant="outline" title="Rollout 文件">
              Rollout {formatInteger(scan.rolloutFiles)}
            </Badge>
            <Badge variant="outline">{result.total} 条</Badge>
            {repairCount > 0 && (
              <Button
                size="xs"
                variant="outline"
                onClick={() => setRepairTarget(combinedRepairTarget)}
              >
                <HugeiconsIcon icon={Wrench01Icon} data-icon="inline-start" />
                修复 {repairCount} 条
              </Button>
            )}
          </CardAction>
        </CardHeader>
        <CardContent>
          {result.items.length ? (
            <ItemGroup className="gap-1!">
              {result.items.map((session) => (
                <Item
                  key={session.identity}
                  size="xs"
                  variant="outline"
                  className="flex-nowrap py-1"
                >
                  <ItemMedia variant="icon">
                    <HugeiconsIcon
                      icon={session.archived ? Database01Icon : Folder01Icon}
                    />
                  </ItemMedia>
                  <ItemContent className="min-w-0">
                    <ItemTitle className="w-full min-w-0">
                      <span
                        className="min-w-0 truncate"
                        title={session.title || "未命名会话"}
                      >
                        {session.title || "未命名会话"}
                      </span>
                      <Badge variant="secondary" className="shrink-0">
                        {session.provider}
                      </Badge>
                    </ItemTitle>
                    <ItemDescription className="truncate text-[11px] leading-4">
                      {session.cwd}
                    </ItemDescription>
                  </ItemContent>
                  <ItemActions className="shrink-0 gap-3">
                    <div className="text-right text-xs text-muted-foreground">
                      <div>{formatDate(session.updatedAt, true)}</div>
                      <div>
                        {syncStatus?.threads.find(
                          (thread) => thread.threadId === session.id
                        )?.state === "running"
                          ? "运行中"
                          : session.archived
                            ? "已归档"
                            : "活跃"}
                      </div>
                    </div>
                    <div className="flex w-[105px] shrink-0 justify-end gap-1.5">
                      <SessionActionButton
                        label={session.archived ? "取消归档" : "归档"}
                        title={session.title}
                        reason={disabledReason(session)}
                        loading={
                          operation?.threadId === session.id &&
                          operation.action !== "delete"
                        }
                        onClick={() =>
                          void mutateSession(
                            session,
                            session.archived ? "unarchive" : "archive"
                          )
                        }
                      />
                      {!session.archived && (
                        <SessionActionButton
                          label="删除"
                          title={session.title}
                          destructive
                          reason={disabledReason(session)}
                          loading={
                            operation?.threadId === session.id &&
                            operation.action === "delete"
                          }
                          onClick={() => void prepareDelete(session)}
                        />
                      )}
                    </div>
                  </ItemActions>
                </Item>
              ))}
            </ItemGroup>
          ) : (
            <Empty>
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <HugeiconsIcon icon={InformationCircleIcon} />
                </EmptyMedia>
                <EmptyTitle>没有匹配的会话</EmptyTitle>
                <EmptyDescription>调整搜索词或刷新本地索引。</EmptyDescription>
              </EmptyHeader>
            </Empty>
          )}
        </CardContent>
        {pageCount > 1 && (
          <CardFooter className="py-1.5">
            <Pagination>
              <PaginationContent>
                <PaginationItem>
                  <PaginationPrevious
                    size="xs"
                    text="上一页"
                    disabled={currentPage === 1}
                    onClick={() => setPage(currentPage - 1)}
                  />
                </PaginationItem>
                <PaginationItem>
                  <span className="px-2 text-xs text-muted-foreground">
                    {currentPage} / {pageCount}
                  </span>
                </PaginationItem>
                <PaginationItem>
                  <PaginationNext
                    size="xs"
                    text="下一页"
                    disabled={currentPage === pageCount}
                    onClick={() => setPage(currentPage + 1)}
                  />
                </PaginationItem>
              </PaginationContent>
            </Pagination>
          </CardFooter>
        )}
      </Card>

      <AlertDialog
        open={Boolean(deleteTarget)}
        onOpenChange={(open) => {
          if (!open && !operation) setDeleteTarget(undefined)
        }}
      >
        <AlertDialogContent initialFocus={cancelDeleteRef}>
          <AlertDialogHeader>
            <AlertDialogTitle>永久删除会话？</AlertDialogTitle>
            <AlertDialogDescription>
              将永久删除“{deleteTarget?.title || "未命名会话"}
              ”及下列派生会话，无法恢复。已记录的历史 Token 用量会保留。
            </AlertDialogDescription>
          </AlertDialogHeader>
          {preparingDelete ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Spinner />
              正在核对删除范围和运行状态…
            </div>
          ) : (
            deletePreview && (
              <ul
                className="max-h-40 space-y-1 overflow-y-auto rounded-xl bg-muted p-3 text-sm"
                aria-label="将删除的会话"
              >
                {deletePreview.affectedThreads.map((thread) => (
                  <li
                    key={thread.id}
                    className="truncate"
                    title={thread.title || thread.id}
                  >
                    {thread.title || "未命名会话"}
                  </li>
                ))}
              </ul>
            )
          )}
          {(deleteError || deletePreview?.blockedReason) && (
            <Alert variant="destructive">
              <AlertDescription>
                {deleteError ?? deletePreview?.blockedReason}
              </AlertDescription>
            </Alert>
          )}
          <AlertDialogFooter>
            <AlertDialogCancel
              ref={cancelDeleteRef}
              disabled={Boolean(operation)}
            >
              取消
            </AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              disabled={
                preparingDelete ||
                Boolean(operation) ||
                !deletePreview ||
                Boolean(deletePreview.blockedReason) ||
                Boolean(deleteTarget && disabledReason(deleteTarget))
              }
              onClick={() =>
                deleteTarget && void mutateSession(deleteTarget, "delete")
              }
            >
              {operation?.action === "delete" && (
                <Spinner data-icon="inline-start" />
              )}
              永久删除
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog
        open={Boolean(repairTarget)}
        onOpenChange={(open) => !open && setRepairTarget(undefined)}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>修复会话归属？</AlertDialogTitle>
            <AlertDialogDescription>
              将 {repairTarget?.count ?? 0} 条来自{" "}
              {repairTarget?.sources.join("、")} 的会话更新为当前提供方“
              {scan.currentProvider}”。操作会修改本地会话元数据。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>取消</AlertDialogCancel>
            <AlertDialogAction disabled={busy} onClick={() => void repair()}>
              {busy && <Spinner data-icon="inline-start" />}确认修复
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}

function SessionActionButton({
  label,
  title,
  reason,
  loading,
  destructive,
  onClick,
}: {
  label: string
  title: string
  reason?: string
  loading?: boolean
  destructive?: boolean
  onClick: () => void
}) {
  const button = (
    <Button
      size="xs"
      variant={destructive ? "destructive" : "outline"}
      disabled={Boolean(reason)}
      aria-label={`${label}：${title || "未命名会话"}`}
      onClick={onClick}
    >
      {loading && <Spinner data-icon="inline-start" />}
      {label}
    </Button>
  )
  return reason ? (
    <Tooltip>
      <TooltipTrigger
        render={
          <span
            className="inline-flex rounded-lg focus-visible:ring-2 focus-visible:ring-ring"
            tabIndex={0}
            aria-label={`${label}不可用：${reason}`}
          />
        }
      >
        {button}
      </TooltipTrigger>
      <TooltipContent>{reason}</TooltipContent>
    </Tooltip>
  ) : (
    button
  )
}
