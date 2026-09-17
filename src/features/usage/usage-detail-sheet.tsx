import { useState } from "react"
import {
  Item,
  ItemActions,
  ItemContent,
  ItemGroup,
  ItemTitle,
} from "@/components/ui/item"
import {
  Progress,
  ProgressLabel,
  ProgressValue,
} from "@/components/ui/progress"
import { Separator } from "@/components/ui/separator"
import {
  Sheet,
  SheetBody,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet"
import { regularOutputTokens } from "@/lib/chart"
import { cacheHitRate, formatInteger, formatPercent } from "@/lib/format"
import type { UsageGroupBy, UsageRow } from "@/types"

export function UsageDetail({
  row,
  groupBy,
  onOpenChange,
}: {
  row?: UsageRow
  groupBy: UsageGroupBy
  onOpenChange: (open: boolean) => void
}) {
  const [lastRow, setLastRow] = useState<UsageRow>()
  if (row && row !== lastRow) setLastRow(row)
  const display = row ?? lastRow

  if (!display) return null
  const hitRate = cacheHitRate(display.tokens)
  const details = [
    ["普通输入", display.tokens.inputTokens],
    ["缓存输入", display.tokens.cachedInputTokens],
    ["缓存写入", display.tokens.cacheWriteInputTokens],
    [
      "普通输出",
      regularOutputTokens(
        display.tokens.outputTokens,
        display.tokens.reasoningOutputTokens
      ),
    ],
    ["推理输出", display.tokens.reasoningOutputTokens],
  ] as const
  return (
    <Sheet open={Boolean(row)} onOpenChange={onOpenChange}>
      <SheetContent>
        <SheetHeader>
          <SheetTitle>
            {groupBy === "model" ? display.model : display.sourceName}
          </SheetTitle>
        </SheetHeader>
        <SheetBody className="gap-2">
          <Progress
            value={hitRate ?? 0}
            className="rounded-2xl bg-muted/40 p-3"
          >
            <ProgressLabel>缓存命中率</ProgressLabel>
            <ProgressValue>{() => formatPercent(hitRate)}</ProgressValue>
          </Progress>
          <ItemGroup>
            {details.map(([label, value]) => (
              <Item key={label} size="xs" variant="muted">
                <ItemContent>
                  <ItemTitle>{label}</ItemTitle>
                </ItemContent>
                <ItemActions className="font-medium tabular-nums">
                  {formatInteger(value)}
                </ItemActions>
              </Item>
            ))}
          </ItemGroup>
          <Separator />
          <div className="flex items-center justify-between">
            <span className="font-medium">合计</span>
            <span className="text-base font-medium tabular-nums">
              {formatInteger(display.tokens.totalTokens)}
            </span>
          </div>
        </SheetBody>
      </SheetContent>
    </Sheet>
  )
}
