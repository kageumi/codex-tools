import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import {
  Add01Icon,
  Database02Icon,
  Delete02Icon,
  Edit02Icon,
  InformationCircleIcon,
  Refresh01Icon,
} from "@hugeicons/core-free-icons"
import { HugeiconsIcon } from "@hugeicons/react"
import { CartesianGrid, Line, LineChart, XAxis, YAxis } from "recharts"

import {
  Alert,
  AlertAction,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart"
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
  ItemTitle,
} from "@/components/ui/item"
import { Skeleton } from "@/components/ui/skeleton"
import { Spinner } from "@/components/ui/spinner"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { toast } from "@/components/ui/toast"
import {
  cacheHitRate,
  errorMessage,
  formatDate,
  formatInteger,
  formatPercent,
  formatRange,
  formatTokens,
  formatUsd,
  todayRange,
} from "@/lib/format"
import {
  tokenTickFormatter,
  trendPointsToSeries,
  usageChartConfig,
} from "@/lib/chart"
import { useAsync } from "@/hooks/use-async"
import { useToday } from "@/hooks/use-today"
import { call } from "@/lib/ipc"
import { createRequestGate } from "@/lib/request-gate"
import type {
  OfficialPricingCatalog,
  Provider,
  PricingRule,
  UsageGroupBy,
  UsageQuery,
  UsageRow,
} from "@/types"

import { PricingEditor } from "./pricing-editor-dialog"
import { billingModeLabel, pricingSourceLabel, pricingSummary } from "./pricing"
import { UsageDetail } from "./usage-detail-sheet"

let automaticOfficialPricingSync: Promise<OfficialPricingCatalog> | undefined

function refreshOfficialPricingOnce() {
  if (!automaticOfficialPricingSync) {
    const shared = call("usage_refresh_official_pricing").finally(() => {
      if (automaticOfficialPricingSync === shared) {
        automaticOfficialPricingSync = undefined
      }
    })
    automaticOfficialPricingSync = shared
  }
  return automaticOfficialPricingSync
}

function officialPricingCatalogChanged(
  previous: OfficialPricingCatalog | undefined,
  next: OfficialPricingCatalog
) {
  return !previous || previous.contentSha256 !== next.contentSha256
}

export function UsagePage({
  refreshRevision,
  days,
  groupBy,
  onRefresh,
  providers,
}: {
  refreshRevision: number
  onRefresh: () => void
  days: number
  groupBy: UsageGroupBy
  providers: Provider[]
}) {
  const [tab, setTab] = useState("details")
  const [selection, setSelection] = useState<{
    row: UsageRow
    query: UsageQuery
    refreshRevision: number
  }>()
  const [ruleOpen, setRuleOpen] = useState(false)
  const [editingRule, setEditingRule] = useState<PricingRule>()
  const [busy, setBusy] = useState(false)
  const [pricingMutation, setPricingMutation] = useState<string>()
  const pricingMutationActive = useRef(false)
  const [officialCatalog, setOfficialCatalog] =
    useState<OfficialPricingCatalog>()
  const [officialPricingError, setOfficialPricingError] = useState<string>()
  const [syncing, setSyncing] = useState(false)
  const [overviewRequestGate] = useState(createRequestGate)
  const mounted = useRef(true)
  const currentScan = useRef<
    ReturnType<typeof overviewRequestGate.begin> | undefined
  >(undefined)

  useEffect(() => {
    mounted.current = true
    return () => {
      mounted.current = false
      overviewRequestGate.invalidate()
    }
  }, [overviewRequestGate])

  const today = useToday()
  const query = useMemo(
    () => ({ range: todayRange(days, today), groupBy }),
    [days, groupBy, today]
  )
  const activeQuery = useRef(query)
  useEffect(() => {
    activeQuery.current = query
    overviewRequestGate.invalidate()
  }, [overviewRequestGate, query])

  const fetchOverview = useCallback(
    () => call("usage_get_overview", { query }),
    [query]
  )
  const {
    data: overview,
    error: overviewError,
    mutate: setOverview,
    clear: clearOverview,
  } = useAsync(
    fetchOverview,
    { requestGate: overviewRequestGate },
    refreshRevision
  )

  useEffect(() => {
    clearOverview()
  }, [clearOverview, query])

  const selected =
    selection?.query === query && selection.refreshRevision === refreshRevision
      ? selection.row
      : undefined

  const fetchRules = useCallback(() => call("usage_list_pricing_rules", {}), [])
  const {
    data: rules,
    error: rulesError,
    mutate: setRules,
  } = useAsync(fetchRules, undefined, refreshRevision)

  const reloadOverview = useCallback(
    async (requestedQuery: UsageQuery) => {
      while (mounted.current && activeQuery.current === requestedQuery) {
        const request = overviewRequestGate.begin("background")
        if (!overviewRequestGate.isCurrent(request)) {
          await overviewRequestGate.waitForChange()
          continue
        }

        try {
          const next = await call("usage_get_overview", {
            query: requestedQuery,
          })
          if (
            mounted.current &&
            overviewRequestGate.isCurrent(request) &&
            activeQuery.current === requestedQuery
          ) {
            setSelection(undefined)
            setOverview(next)
            return
          }
        } catch (reason) {
          if (overviewRequestGate.isCurrent(request)) throw reason
        } finally {
          overviewRequestGate.finish(request)
        }
      }
    },
    [overviewRequestGate, setOverview]
  )

  const syncOfficialPricing = useCallback(async () => {
    setSyncing(true)
    setOfficialPricingError(undefined)
    try {
      const catalog = await refreshOfficialPricingOnce()
      if (!mounted.current) return undefined
      setOfficialCatalog(catalog)
      toast.add({ title: "官方价格已同步", type: "success" })
      if (officialPricingCatalogChanged(officialCatalog, catalog)) {
        try {
          await reloadOverview(activeQuery.current)
        } catch (reason) {
          if (!mounted.current) return catalog
          toast.add({
            title: "无法更新费用概览",
            description: errorMessage(reason),
            type: "error",
          })
        }
      }
      return catalog
    } catch (reason) {
      if (!mounted.current) return undefined
      const message = errorMessage(reason)
      setOfficialPricingError(message)
      toast.add({
        title: "官方价格同步失败",
        description: message,
        type: "error",
      })
      return undefined
    } finally {
      if (mounted.current) setSyncing(false)
    }
  }, [officialCatalog, reloadOverview])

  useEffect(() => {
    let cancelled = false
    void (async () => {
      let cachedCatalog: OfficialPricingCatalog | undefined
      try {
        const cached = await call("usage_get_official_pricing")
        if (!cancelled) {
          setOfficialCatalog(cached)
        }
        cachedCatalog = cached
      } catch {
        // The network refresh below can still recover when no cache is present.
      }

      try {
        const catalog = await refreshOfficialPricingOnce()
        if (cancelled) return
        setOfficialCatalog(catalog)
        setOfficialPricingError(undefined)
        if (officialPricingCatalogChanged(cachedCatalog, catalog)) {
          try {
            await reloadOverview(activeQuery.current)
          } catch {
            // The catalog is still valid; the normal overview request can recover.
          }
        }
      } catch (reason) {
        if (!cancelled) setOfficialPricingError(errorMessage(reason))
      }
    })()
    return () => {
      cancelled = true
    }
  }, [reloadOverview])

  const refreshUsage = async () => {
    setSelection(undefined)
    const requestedQuery = query
    const request = overviewRequestGate.begin("scan")
    currentScan.current = request
    setBusy(true)
    try {
      const next = await call("usage_refresh", { query: requestedQuery })
      if (
        !mounted.current ||
        !overviewRequestGate.isCurrent(request) ||
        activeQuery.current !== requestedQuery
      )
        return
      setOverview(next)
      toast.add({
        title: next.warnings.length ? "用量已刷新，但有部分警告" : "用量已刷新",
        description: next.warnings[0]?.message,
        type: next.warnings.length ? "warning" : "success",
      })
    } catch (reason) {
      if (!mounted.current || !overviewRequestGate.isCurrent(request)) return
      toast.add({
        title: "刷新失败",
        description: errorMessage(reason),
        type: "error",
      })
    } finally {
      overviewRequestGate.finish(request)
      if (mounted.current && currentScan.current === request) {
        currentScan.current = undefined
        setBusy(false)
      }
    }
  }

  const deleteRule = async (id: string) => {
    if (pricingMutationActive.current) return
    pricingMutationActive.current = true
    setPricingMutation(id)
    try {
      await call("usage_delete_pricing_rule", { id })
      setRules((current) => (current ?? []).filter((rule) => rule.id !== id))
      try {
        await call("usage_reprice", { range: query.range })
        await reloadOverview(query)
        toast.add({ title: "价格规则已删除", type: "success" })
      } catch (reason) {
        toast.add({
          title: "价格规则已删除，但重新计价失败",
          description: errorMessage(reason),
          type: "warning",
        })
      }
    } catch (reason) {
      toast.add({
        title: "删除失败",
        description: errorMessage(reason),
        type: "error",
      })
    } finally {
      pricingMutationActive.current = false
      if (mounted.current) setPricingMutation(undefined)
    }
  }

  const openNewRule = () => {
    setEditingRule(undefined)
    setRuleOpen(true)
  }

  const openEditRule = (rule: PricingRule) => {
    setEditingRule(rule)
    setRuleOpen(true)
  }

  const points = useMemo(
    () => trendPointsToSeries(overview?.trendPoints ?? [], days === 1),
    [days, overview]
  )
  const cacheRows = useMemo(
    () =>
      overview?.rows.filter(
        (row) =>
          row.tokens.cachedInputTokens > 0 ||
          row.tokens.cacheWriteInputTokens > 0
      ) ?? [],
    [overview]
  )
  const modelOptions = useMemo(
    () =>
      [...new Set(overview?.models ?? [])]
        .filter(Boolean)
        .sort((left, right) => left.localeCompare(right)),
    [overview]
  )

  if (!overview && overviewError)
    return (
      <div className="min-h-full px-3 pt-1 pb-3">
        <Alert variant="destructive">
          <HugeiconsIcon icon={InformationCircleIcon} />
          <AlertTitle>无法读取用量</AlertTitle>
          <AlertDescription>{overviewError}</AlertDescription>
        </Alert>
      </div>
    )

  if (!overview)
    return (
      <div
        className="grid min-h-full grid-rows-[164px_minmax(208px,1fr)] gap-3 px-3 pt-1 pb-3"
        role="status"
        aria-busy="true"
      >
        <span className="sr-only">正在读取用量</span>
        <Skeleton className="rounded-2xl" />
        <Skeleton className="rounded-2xl" />
      </div>
    )

  return (
    <div className="flex min-h-full flex-col gap-3 px-3 pt-1 pb-3">
      {overviewError && (
        <Alert variant="destructive">
          <HugeiconsIcon icon={InformationCircleIcon} />
          <AlertTitle>用量刷新失败</AlertTitle>
          <AlertDescription>{overviewError}</AlertDescription>
        </Alert>
      )}
      {overview.warnings.length > 0 && (
        <Alert>
          <HugeiconsIcon icon={InformationCircleIcon} />
          <AlertTitle>部分用量日志未能完整读取</AlertTitle>
          <AlertDescription>
            {overview.warnings[0]?.message}
            {overview.warnings.length > 1
              ? `（另有 ${overview.warnings.length - 1} 条警告）`
              : ""}
          </AlertDescription>
        </Alert>
      )}
      {rulesError && (
        <Alert variant="destructive">
          <HugeiconsIcon icon={InformationCircleIcon} />
          <AlertTitle>无法读取价格规则</AlertTitle>
          <AlertDescription>{rulesError}</AlertDescription>
        </Alert>
      )}
      <Card size="sm" className="shrink-0">
        <CardHeader className="border-b">
          <CardTitle>用量趋势</CardTitle>
          <CardDescription>{formatRange(overview.range)}</CardDescription>
          <CardAction>
            <Button
              size="sm"
              variant="outline"
              disabled={busy}
              onClick={() => void refreshUsage()}
            >
              {busy ? (
                <Spinner data-icon="inline-start" />
              ) : (
                <HugeiconsIcon icon={Refresh01Icon} data-icon="inline-start" />
              )}
              扫描
            </Button>
          </CardAction>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <ChartContainer
            config={usageChartConfig}
            className="aspect-auto h-28 w-full"
            initialDimension={{ width: 600, height: 112 }}
          >
            <LineChart
              data={points}
              margin={{ left: 4, right: 20, top: 4, bottom: 0 }}
            >
              <CartesianGrid vertical={false} strokeDasharray="4 4" />
              <XAxis
                dataKey="date"
                tickLine={false}
                axisLine={false}
                interval={days === 1 ? "preserveStartEnd" : 0}
                minTickGap={days === 1 ? 24 : 0}
              />
              <YAxis
                width={56}
                tickLine={false}
                axisLine={false}
                tickMargin={8}
                tickFormatter={tokenTickFormatter}
              />
              <ChartTooltip
                cursor={false}
                content={<ChartTooltipContent indicator="line" />}
              />
              <ChartLegend content={<ChartLegendContent />} />
              <Line
                dataKey="input"
                type="linear"
                stroke="var(--color-input)"
                strokeWidth={2}
                dot={false}
              />
              <Line
                dataKey="output"
                type="linear"
                stroke="var(--color-output)"
                strokeWidth={2}
                dot={false}
              />
              <Line
                dataKey="cache"
                type="linear"
                stroke="var(--color-cache)"
                strokeWidth={1.75}
                strokeDasharray="5 4"
                dot={false}
              />
            </LineChart>
          </ChartContainer>
          <div className="grid grid-cols-4 divide-x divide-border">
            <Metric
              label="总 Token"
              value={formatTokens(overview.totals.tokens.totalTokens)}
            />
            <Metric
              label="请求"
              value={formatInteger(overview.totals.requests)}
            />
            <Metric
              label="缓存命中"
              value={formatPercent(cacheHitRate(overview.totals.tokens))}
            />
            <Metric
              label="估算费用"
              value={formatUsd(overview.totals.estimatedCostMicrousd)}
            />
          </div>
        </CardContent>
      </Card>

      <Card size="sm" className="min-h-52 flex-1">
        <Tabs
          value={tab}
          onValueChange={setTab}
          className="flex min-h-0 flex-1 flex-col"
        >
          <CardHeader className="grid grid-cols-[1fr_auto] items-center border-b">
            <TabsList>
              <TabsTrigger value="details">明细</TabsTrigger>
              <TabsTrigger value="cache">缓存</TabsTrigger>
              <TabsTrigger value="pricing">价格规则</TabsTrigger>
            </TabsList>
            {tab === "pricing" && (
              <Button
                size="sm"
                disabled={Boolean(pricingMutation)}
                onClick={openNewRule}
              >
                <HugeiconsIcon icon={Add01Icon} data-icon="inline-start" />
                添加规则
              </Button>
            )}
          </CardHeader>
          <CardContent className="min-h-0 flex-1 overflow-y-auto overscroll-contain">
            <TabsContent value="details" className="mt-0">
              {overview.rows.length ? (
                <ItemGroup>
                  {overview.rows.map((row) => (
                    <Item
                      key={row.key}
                      size="xs"
                      variant="outline"
                      className="flex-nowrap"
                      render={<button type="button" />}
                      onClick={() =>
                        setSelection({ row, query, refreshRevision })
                      }
                    >
                      <ItemContent>
                        <ItemTitle>
                          {groupBy === "model" ? row.model : row.sourceName}
                        </ItemTitle>
                        <ItemDescription>
                          {formatInteger(row.requests)} 次请求 ·{" "}
                          {row.pricingRuleName ?? "未匹配价格规则"}
                        </ItemDescription>
                      </ItemContent>
                      <ItemActions className="text-right">
                        <div>
                          <div className="font-medium tabular-nums">
                            {formatTokens(row.tokens.totalTokens)}
                          </div>
                          <div className="text-xs text-muted-foreground">
                            {formatUsd(row.estimatedCostMicrousd)}
                          </div>
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
                    <EmptyTitle>暂无用量</EmptyTitle>
                    <EmptyDescription>
                      扫描 Codex 会话后会显示详细数据。
                    </EmptyDescription>
                  </EmptyHeader>
                </Empty>
              )}
            </TabsContent>
            <TabsContent value="pricing" className="mt-0">
              <Alert className="mb-3">
                <HugeiconsIcon icon={InformationCircleIcon} />
                <AlertTitle>
                  OpenAI 官方参考价格
                  <Badge variant="secondary">
                    {officialPricingError
                      ? "同步失败"
                      : officialCatalog
                        ? officialCatalog.status === "waiting"
                          ? "待同步"
                          : `${officialCatalog.modelCount} 个模型`
                        : "读取中"}
                  </Badge>
                </AlertTitle>
                <AlertDescription className="truncate">
                  {officialPricingError
                    ? `同步失败：${officialPricingError}`
                    : officialCatalog?.fetchedAtMs
                      ? `上次同步 ${formatDate(officialCatalog.fetchedAtMs, true)}`
                      : "进入用量页会自动同步，也可手动刷新。"}
                </AlertDescription>
                <AlertAction>
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={syncing}
                    onClick={() => void syncOfficialPricing()}
                  >
                    {syncing ? (
                      <Spinner data-icon="inline-start" />
                    ) : (
                      <HugeiconsIcon
                        icon={Refresh01Icon}
                        data-icon="inline-start"
                      />
                    )}
                    {officialPricingError ? "重试" : "同步"}
                  </Button>
                </AlertAction>
              </Alert>
              {!rules ? (
                rulesError ? null : (
                  <Skeleton className="h-24 rounded-xl" />
                )
              ) : rules.length ? (
                <ItemGroup>
                  {rules.map((rule) => (
                    <Item
                      key={rule.id}
                      size="xs"
                      variant="outline"
                      className="flex-nowrap"
                    >
                      <ItemContent>
                        <ItemTitle>
                          {rule.modelPattern}
                          <Badge variant="secondary">
                            {billingModeLabel(rule.billingMode)}
                          </Badge>
                        </ItemTitle>
                        <ItemDescription>
                          {pricingSourceLabel(rule, providers)} ·{" "}
                          {pricingSummary(rule)}
                        </ItemDescription>
                      </ItemContent>
                      <ItemActions>
                        <Button
                          size="icon-sm"
                          variant="ghost"
                          aria-label="编辑规则"
                          disabled={Boolean(pricingMutation)}
                          onClick={() => openEditRule(rule)}
                        >
                          <HugeiconsIcon icon={Edit02Icon} />
                        </Button>
                        <Button
                          size="icon-sm"
                          variant="ghost"
                          aria-label="删除规则"
                          disabled={Boolean(pricingMutation)}
                          onClick={() => void deleteRule(rule.id)}
                        >
                          {pricingMutation === rule.id ? (
                            <Spinner />
                          ) : (
                            <HugeiconsIcon icon={Delete02Icon} />
                          )}
                        </Button>
                      </ItemActions>
                    </Item>
                  ))}
                </ItemGroup>
              ) : (
                <Empty>
                  <EmptyHeader>
                    <EmptyTitle>使用官方参考价格</EmptyTitle>
                    <EmptyDescription>
                      还没有自定义价格规则；官方账号会使用缓存的参考价格。
                    </EmptyDescription>
                  </EmptyHeader>
                </Empty>
              )}
            </TabsContent>
            <TabsContent value="cache" className="mt-0">
              {cacheRows.length ? (
                <ItemGroup>
                  {cacheRows.map((row) => (
                    <Item
                      key={row.key}
                      size="xs"
                      variant="outline"
                      className="flex-nowrap"
                      render={<button type="button" />}
                      onClick={() =>
                        setSelection({ row, query, refreshRevision })
                      }
                    >
                      <ItemContent>
                        <ItemTitle>
                          {groupBy === "model" ? row.model : row.sourceName}
                        </ItemTitle>
                        <ItemDescription>
                          读取 {formatTokens(row.tokens.cachedInputTokens)} ·
                          写入 {formatTokens(row.tokens.cacheWriteInputTokens)}
                        </ItemDescription>
                      </ItemContent>
                      <ItemActions className="text-right">
                        <div>
                          <div className="font-medium tabular-nums">
                            {formatPercent(cacheHitRate(row.tokens))}
                          </div>
                          <div className="text-xs text-muted-foreground">
                            命中率
                          </div>
                        </div>
                      </ItemActions>
                    </Item>
                  ))}
                </ItemGroup>
              ) : (
                <Empty>
                  <EmptyHeader>
                    <EmptyMedia variant="icon">
                      <HugeiconsIcon icon={Database02Icon} />
                    </EmptyMedia>
                    <EmptyTitle>暂无缓存数据</EmptyTitle>
                    <EmptyDescription>
                      当前范围内没有检测到缓存读取或写入 Token。
                    </EmptyDescription>
                  </EmptyHeader>
                </Empty>
              )}
            </TabsContent>
          </CardContent>
        </Tabs>
      </Card>

      <UsageDetail
        row={selected}
        groupBy={groupBy}
        onOpenChange={(open) => !open && setSelection(undefined)}
      />
      <PricingEditor
        key={`${ruleOpen ? "open" : "closed"}-${editingRule?.id ?? "new"}`}
        open={ruleOpen}
        range={query.range}
        modelOptions={modelOptions}
        providers={providers}
        rules={rules ?? []}
        editingRule={editingRule}
        onOpenChange={(open) => {
          setRuleOpen(open)
          if (!open) setEditingRule(undefined)
        }}
        onSaved={() => {
          setRuleOpen(false)
          setEditingRule(undefined)
          onRefresh()
        }}
      />
    </div>
  )
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex min-w-0 flex-col gap-1 px-3 first:pl-0 last:pr-0">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="text-base font-medium tabular-nums">{value}</span>
    </div>
  )
}
