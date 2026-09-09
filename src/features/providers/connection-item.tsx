import { memo, type ReactNode } from "react"
import {
  ApiIcon,
  Delete02Icon,
  Edit02Icon,
  MoreHorizontalIcon,
} from "@hugeicons/core-free-icons"
import { HugeiconsIcon } from "@hugeicons/react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemFooter,
  ItemHeader,
  ItemMedia,
  ItemTitle,
} from "@/components/ui/item"
import { Spinner } from "@/components/ui/spinner"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"

import { type ConnectionKind } from "./connection-utils"

export type PendingAction = {
  action:
    "activate" | "delete" | "quota" | "login" | "test" | "models" | "remark"
  id: string
}

export type MoreAction = {
  icon: Parameters<typeof HugeiconsIcon>[0]["icon"]
  label: string
  onSelect: () => void
}

export const ConnectionItem = memo(function ConnectionItem({
  kind,
  id,
  name,
  description,
  details,
  active,
  canView,
  selected,
  unavailable,
  unavailableLabel,
  activateDisabled = false,
  frozen,
  pending,
  onView,
  onActivate,
  onEdit,
  onDelete,
  moreActions,
}: {
  kind: ConnectionKind
  id: string
  name: string
  description: string
  details?: ReactNode
  active: boolean
  canView: boolean
  selected: boolean
  unavailable: boolean
  unavailableLabel: string
  activateDisabled?: boolean
  frozen: boolean
  pending?: PendingAction
  onView: () => void
  onActivate: () => void
  onEdit: () => void
  onDelete: () => void
  moreActions: MoreAction[]
}) {
  const activating = pending?.action === "activate" && pending.id === id
  const rowPending = pending?.id === id

  return (
    <Item
      size="sm"
      variant={active || selected ? "muted" : "outline"}
      className="items-start"
      aria-label={`${kind === "account" ? "账号" : "API 服务"} ${name}`}
    >
      <ItemHeader className="min-h-5 items-start">
        {kind === "provider" && (
          <ItemMedia variant="icon">
            <HugeiconsIcon icon={ApiIcon} />
          </ItemMedia>
        )}
        <ItemTitle
          className={
            kind === "account"
              ? "line-clamp-none min-w-0 flex-1 break-words whitespace-normal"
              : "min-w-0 flex-1"
          }
        >
          {name}
        </ItemTitle>
        <ItemActions className="max-w-[45%] flex-wrap justify-end gap-1 self-start">
          {active && <Badge>当前</Badge>}
          {selected && <Badge variant="secondary">已选</Badge>}
          {unavailable && (
            <Badge variant="destructive">{unavailableLabel}</Badge>
          )}
        </ItemActions>
      </ItemHeader>
      <ItemContent title={description} className="basis-full">
        {details ? (
          <>
            <div
              data-slot="item-description"
              className="min-w-0 text-left text-sm font-normal text-muted-foreground"
            >
              {details}
            </div>
            <span className="sr-only">{description}</span>
          </>
        ) : (
          <ItemDescription>{description}</ItemDescription>
        )}
      </ItemContent>
      <ItemFooter className="justify-end gap-1.5">
        {canView && !selected ? (
          <Button
            type="button"
            size="xs"
            variant="ghost"
            className="mr-auto"
            disabled={frozen}
            onClick={onView}
          >
            查看
          </Button>
        ) : (
          <span
            aria-hidden="true"
            className="mr-auto inline-flex h-6 min-w-8"
          />
        )}
        <Button
          type="button"
          size="xs"
          variant="outline"
          disabled={frozen || active || activateDisabled}
          aria-busy={activating}
          onClick={onActivate}
        >
          {activating && <Spinner data-icon="inline-start" />}
          设为当前
        </Button>
        <Tooltip>
          <TooltipTrigger
            render={
              <Button
                type="button"
                size="icon-xs"
                variant="outline"
                aria-label={`编辑${kind === "account" ? "账号" : "服务"}：${name}`}
                disabled={frozen}
                onClick={onEdit}
              />
            }
          >
            <HugeiconsIcon icon={Edit02Icon} />
          </TooltipTrigger>
          <TooltipContent>编辑</TooltipContent>
        </Tooltip>
        <Tooltip>
          <TooltipTrigger
            render={
              <Button
                type="button"
                size="icon-xs"
                variant="destructive"
                aria-label={`删除${kind === "account" ? "账号" : "服务"}：${name}`}
                disabled={frozen}
                onClick={onDelete}
              />
            }
          >
            <HugeiconsIcon icon={Delete02Icon} />
          </TooltipTrigger>
          <TooltipContent>删除</TooltipContent>
        </Tooltip>
        <DropdownMenu>
          <DropdownMenuTrigger
            render={
              <Button
                type="button"
                size="icon-xs"
                variant="ghost"
                aria-label={`更多管理操作：${name}`}
                title={`更多管理操作：${name}`}
                disabled={frozen}
              />
            }
          >
            {rowPending && !activating && pending?.action !== "delete" ? (
              <Spinner />
            ) : (
              <HugeiconsIcon icon={MoreHorizontalIcon} />
            )}
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuGroup>
              {moreActions.map((action) => (
                <DropdownMenuItem
                  key={action.label}
                  disabled={frozen}
                  onClick={action.onSelect}
                >
                  <HugeiconsIcon icon={action.icon} />
                  {action.label}
                </DropdownMenuItem>
              ))}
            </DropdownMenuGroup>
          </DropdownMenuContent>
        </DropdownMenu>
      </ItemFooter>
    </Item>
  )
})

export const EmptyConnectionItem = memo(function EmptyConnectionItem({
  label,
}: {
  label: string
}) {
  return (
    <Item size="xs" variant="outline">
      <ItemContent>
        <ItemDescription>{label}</ItemDescription>
      </ItemContent>
    </Item>
  )
})
