import { useCallback, useRef, useState } from "react"
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
import { errorMessage, formatDate, formatInteger } from "@/lib/format"
import { useAsync } from "@/hooks/use-async"
import { call } from "@/lib/ipc"
import type { RepairTarget } from "@/types"

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
  const loadedRefreshRevision = useRef(refreshRevision)

  const fetchList = useCallback(() => {
    const forceRefresh = refreshRevision !== loadedRefreshRevision.current
    loadedRefreshRevision.current = refreshRevision
    return call("sessions_list", {
      query,
      page,
      pageSize,
      refresh: forceRefresh,
      status,
    })
  }, [page, query, refreshRevision, status])
  const { data: result, error: listError } = useAsync(fetchList, {
    clearOnLoad: true,
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
    refreshRevision
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
    <div className="flex min-h-full flex-col gap-3 px-3 pt-1 pb-3">
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
      <Card size="sm" className="shrink-0">
        <CardContent className="grid grid-cols-[1fr_1fr_auto] items-center gap-4">
          <Summary
            icon={Database01Icon}
            label="已索引会话"
            value={formatInteger(scan.sessionMetaCount)}
          />
          <Summary
            icon={Folder01Icon}
            label="Rollout 文件"
            value={formatInteger(scan.rolloutFiles)}
          />
          <div className="flex gap-2">
            {repairCount > 0 && (
              <Button
                size="sm"
                variant="outline"
                onClick={() => setRepairTarget(combinedRepairTarget)}
              >
                <HugeiconsIcon icon={Wrench01Icon} data-icon="inline-start" />
                修复 {repairCount} 条
              </Button>
            )}
          </div>
        </CardContent>
      </Card>
      <Card size="sm" className="min-h-64 shrink-0">
        <CardHeader className="border-b">
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
          <CardDescription>
            {query
              ? `“${query}” 的${status === "archived" ? "已归档" : "活跃"}结果`
              : `全部${status === "archived" ? "已归档" : "活跃"}本地会话`}
          </CardDescription>
          <CardAction>
            <Badge variant="outline">{result.total} 条</Badge>
          </CardAction>
        </CardHeader>
        <CardContent>
          {result.items.length ? (
            <ItemGroup>
              {result.items.map((session) => (
                <Item
                  key={session.identity}
                  size="xs"
                  variant="outline"
                  className="flex-nowrap"
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
                    <ItemDescription className="truncate">
                      {session.cwd}
                    </ItemDescription>
                  </ItemContent>
                  <ItemActions className="shrink-0">
                    <div className="text-right text-xs text-muted-foreground">
                      <div>{formatDate(session.updatedAt, true)}</div>
                      <div>{session.archived ? "已归档" : "活跃"}</div>
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
          <CardFooter>
            <Pagination>
              <PaginationContent>
                <PaginationItem>
                  <PaginationPrevious
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

function Summary({
  icon,
  label,
  value,
}: {
  icon: typeof Database01Icon
  label: string
  value: string
}) {
  return (
    <div className="flex items-center gap-2">
      <div className="flex size-8 items-center justify-center rounded-xl bg-muted">
        <HugeiconsIcon icon={icon} />
      </div>
      <div>
        <div className="text-xs text-muted-foreground">{label}</div>
        <div className="text-base font-medium tabular-nums">{value}</div>
      </div>
    </div>
  )
}
