import { useCallback, useMemo } from "react"
import { InformationCircleIcon } from "@hugeicons/core-free-icons"
import { HugeiconsIcon } from "@hugeicons/react"
import { CartesianGrid, Line, LineChart, XAxis, YAxis } from "recharts"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart"
import { Progress } from "@/components/ui/progress"
import { Skeleton } from "@/components/ui/skeleton"
import {
  formatDate,
  formatInteger,
  formatTokens,
  formatUsd,
  todayRange,
  tokenInput,
} from "@/lib/format"
import {
  tokenTickFormatter,
  trendPointsToSeries,
  usageChartConfig,
} from "@/lib/chart"
import { useAsync } from "@/hooks/use-async"
import { call } from "@/lib/ipc"
import type { Dashboard } from "@/types"
import {
  displayQuotaWindows,
  type DisplayQuotaWindow,
} from "@/features/providers/quota-estimate"

export function DashboardPage({
  dashboard,
  refreshRevision,
}: {
  dashboard?: Dashboard
  refreshRevision: number
}) {
  const fetchUsage = useCallback(
    () =>
      call("usage_get_overview", {
        query: { range: todayRange(7), groupBy: "model" },
      }),
    []
  )
  const { data: usage, error } = useAsync(
    fetchUsage,
    undefined,
    refreshRevision
  )

  const points = useMemo(
    () => trendPointsToSeries(usage?.trendPoints ?? []),
    [usage]
  )
  const quotaWindows = displayQuotaWindows(dashboard?.activeQuota).sort(
    (left, right) => left.windowSeconds - right.windowSeconds
  )

  if (!dashboard || (!usage && !error)) {
    return (
      <div
        className="grid min-h-full grid-rows-[minmax(240px,1fr)_80px_80px] gap-3 px-3 pt-1 pb-3"
        role="status"
        aria-busy="true"
      >
        <span className="sr-only">正在读取概览</span>
        <Skeleton className="rounded-2xl" />
        <Skeleton className="rounded-2xl" />
        <Skeleton className="rounded-2xl" />
      </div>
    )
  }

  if (!usage) {
    return (
      <div className="min-h-full px-3 pt-1 pb-3">
        <Alert variant="destructive">
          <HugeiconsIcon icon={InformationCircleIcon} />
          <AlertTitle>无法读取概览用量</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      </div>
    )
  }

  return (
    <div className="flex min-h-full flex-col gap-3 px-3 pt-1 pb-3">
      {error && (
        <Alert variant="destructive">
          <HugeiconsIcon icon={InformationCircleIcon} />
          <AlertTitle>数据刷新失败</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      <Card size="sm" className="min-h-60 flex-1">
        <CardHeader className="border-b">
          <CardTitle>Token 趋势（最近 7 天）</CardTitle>
        </CardHeader>
        <CardContent className="flex min-h-0 flex-1">
          <ChartContainer
            config={usageChartConfig}
            className="aspect-auto min-h-0 w-full flex-1"
            initialDimension={{ width: 620, height: 220 }}
          >
            <LineChart
              data={points}
              margin={{ left: 8, right: 10, top: 4, bottom: 0 }}
            >
              <CartesianGrid vertical={false} strokeDasharray="4 4" />
              <XAxis
                dataKey="date"
                tickLine={false}
                axisLine={false}
                tickMargin={8}
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
                strokeWidth={2.5}
                dot={{ r: 3, fill: "var(--color-input)" }}
                activeDot={{ r: 4 }}
              />
              <Line
                dataKey="output"
                type="linear"
                stroke="var(--color-output)"
                strokeWidth={2.5}
                dot={{ r: 3, fill: "var(--color-output)" }}
                activeDot={{ r: 4 }}
              />
              <Line
                dataKey="cache"
                type="linear"
                stroke="var(--color-cache)"
                strokeWidth={2}
                strokeDasharray="5 4"
                dot={{ r: 2.5, fill: "var(--color-cache)" }}
                activeDot={{ r: 4 }}
              />
            </LineChart>
          </ChartContainer>
        </CardContent>
      </Card>

      <Card size="sm" className="shrink-0">
        <CardContent className="grid grid-cols-3 divide-x divide-border">
          <Metric
            label="总 Token"
            value={formatTokens(usage.totals.tokens.totalTokens)}
            detail={`输入 ${formatTokens(tokenInput(usage.totals.tokens))} · 输出 ${formatTokens(usage.totals.tokens.outputTokens)}`}
          />
          <Metric label="请求" value={formatInteger(usage.totals.requests)} />
          <Metric
            label="估算费用"
            value={formatUsd(usage.totals.estimatedCostMicrousd)}
          />
        </CardContent>
      </Card>

      {dashboard.activeKind === "official" && (
        <Card size="sm" className="shrink-0">
          <CardContent
            className={
              quotaWindows.length > 1
                ? "grid min-w-0 grid-cols-2 gap-3"
                : "grid min-w-0 gap-3"
            }
          >
            {quotaWindows.length ? (
              quotaWindows.map((quota) => (
                <QuotaWindowCard
                  key={`${quota.windowSeconds}-${quota.resetAt ?? "missing"}`}
                  quota={quota}
                />
              ))
            ) : (
              <p className="text-xs text-muted-foreground">
                {dashboard.activeQuota?.error || "额度暂不可用"}
              </p>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}

function QuotaWindowCard({ quota }: { quota: DisplayQuotaWindow }) {
  return (
    <div className="flex min-w-0 flex-col gap-1.5">
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
      <div className="text-xs text-muted-foreground">
        {quota.resetAt ? formatDate(quota.resetAt, true) : "—"} 重置
      </div>
    </div>
  )
}

function Metric({
  label,
  value,
  detail,
}: {
  label: string
  value: string
  detail?: string
}) {
  return (
    <div className="flex min-w-0 flex-col gap-1 px-3 first:pl-0 last:pr-0">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="text-base font-medium tabular-nums">{value}</span>
      {detail && (
        <span className="truncate text-xs text-muted-foreground">{detail}</span>
      )}
    </div>
  )
}
