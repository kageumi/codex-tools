import type { TokenBreakdown, UsageRange } from "@/types"

const compactNumber = new Intl.NumberFormat("zh-CN", {
  notation: "compact",
  maximumFractionDigits: 1,
})

const integerFormatter = new Intl.NumberFormat("zh-CN")

const usdFormatterPrecise = new Intl.NumberFormat("zh-CN", {
  style: "currency",
  currency: "USD",
  currencyDisplay: "narrowSymbol",
  minimumFractionDigits: 4,
  maximumFractionDigits: 4,
})

const usdFormatterCompact = new Intl.NumberFormat("zh-CN", {
  style: "currency",
  currency: "USD",
  currencyDisplay: "narrowSymbol",
  minimumFractionDigits: 2,
  maximumFractionDigits: 4,
})

const dateFormatter = new Intl.DateTimeFormat("zh-CN", {
  month: "2-digit",
  day: "2-digit",
})

const dateTimeFormatter = new Intl.DateTimeFormat("zh-CN", {
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hourCycle: "h23",
})

const percentFormatter = new Intl.NumberFormat("zh-CN", {
  style: "percent",
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
})

export function formatTokens(value = 0) {
  return compactNumber.format(value)
}

export function formatInteger(value = 0) {
  return integerFormatter.format(value)
}

export function formatUsd(microusd = 0) {
  const formatter =
    microusd >= 100_000 ? usdFormatterCompact : usdFormatterPrecise
  return formatter.format(microusd / 1_000_000)
}

export function formatDate(value?: number, includeTime = false) {
  if (!value) return "—"
  const formatter = includeTime ? dateTimeFormatter : dateFormatter
  const date = new Date(value > 10_000_000_000 ? value : value * 1000)
  return Number.isFinite(date.getTime()) ? formatter.format(date) : "—"
}

export function formatRange(range: UsageRange) {
  return `${formatDate(range.startAtMs)} – ${formatDate(range.endAtMs - 1)}`
}

export function todayRange(days = 1, now: Date = new Date()): UsageRange {
  const end = new Date(now)
  end.setHours(24, 0, 0, 0)
  const start = new Date(end)
  start.setDate(start.getDate() - days)
  return { startAtMs: start.getTime(), endAtMs: end.getTime() }
}

export function tokenInput(tokens?: TokenBreakdown) {
  return tokens?.inputTokens ?? 0
}

export function cacheHitRate(tokens?: TokenBreakdown) {
  const input = tokens?.inputTokens ?? 0
  if (input <= 0) return undefined
  const cached = Math.min(Math.max(tokens?.cachedInputTokens ?? 0, 0), input)
  return (cached / input) * 100
}

export function formatPercent(value?: number) {
  return value === undefined ? "—" : percentFormatter.format(value / 100)
}

export function errorMessage(reason: unknown) {
  return reason instanceof Error ? reason.message : String(reason)
}
