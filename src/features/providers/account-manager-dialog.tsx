import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Delete02Icon } from "@hugeicons/core-free-icons"
import { HugeiconsIcon } from "@hugeicons/react"

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
import { Checkbox } from "@/components/ui/checkbox"
import {
  Dialog,
  DialogBody,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { hiddenOverlayStyles } from "@/components/ui/overlay-styles"
import { Spinner } from "@/components/ui/spinner"
import {
  Table,
  TableBody,
  TableCaption,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { toast } from "@/components/ui/toast"
import { useAsyncAction } from "@/hooks/use-async-action"
import { errorMessage } from "@/lib/format"
import { call } from "@/lib/ipc"
import type { OfficialAccountView, Provider, RepairResult } from "@/types"

import {
  buildFallbackCandidates,
  repairWarning,
  switchActiveConnection,
} from "./connection-utils"

const MAX_REMARK_LENGTH = 200

type AccountManagerDialogProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  accounts: OfficialAccountView[]
  providers: Provider[]
  initialSelectedIds: readonly string[]
  selectedId?: string
  onSelectedIdChange: (id: string) => void
  onRefresh: () => void
}

const AccountRow = memo(function AccountRow({
  account,
  checked,
  remark,
  frozen,
  onToggle,
  onRemarkChange,
}: {
  account: OfficialAccountView
  checked: boolean
  remark: string
  frozen: boolean
  onToggle: (id: string, checked: boolean) => void
  onRemarkChange: (id: string, value: string) => void
}) {
  const displayName = account.remark || account.name
  return (
    <TableRow data-state={checked ? "selected" : undefined}>
      <TableCell>
        <Checkbox
          aria-label={`选择账号 ${displayName}`}
          checked={checked}
          disabled={frozen}
          onCheckedChange={(nextChecked) => onToggle(account.id, nextChecked)}
        />
      </TableCell>
      <TableCell>
        <div className="max-w-52">
          <div className="truncate font-medium">{account.name}</div>
          <div className="truncate text-xs text-muted-foreground">
            {account.email || account.accountId}
          </div>
        </div>
      </TableCell>
      <TableCell>
        {account.active ? (
          <Badge>当前账号</Badge>
        ) : (
          <Badge variant="outline">已保存</Badge>
        )}
      </TableCell>
      <TableCell>
        <Input
          aria-label={`${account.name}的账号备注`}
          disabled={frozen}
          maxLength={MAX_REMARK_LENGTH}
          placeholder="未设置备注"
          value={remark}
          onChange={(event) => onRemarkChange(account.id, event.target.value)}
        />
      </TableCell>
    </TableRow>
  )
})

export function AccountManagerDialog({
  open,
  onOpenChange,
  accounts,
  providers,
  initialSelectedIds,
  selectedId,
  onSelectedIdChange,
  onRefresh,
}: AccountManagerDialogProps) {
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set())
  const [drafts, setDrafts] = useState<Record<string, string>>({})
  const [deleteOpen, setDeleteOpen] = useState(false)
  const wasOpenRef = useRef(false)
  const {
    busy,
    begin: beginBusy,
    end: endBusy,
  } = useAsyncAction<"saving" | "deleting">()

  useEffect(() => {
    if (!open) {
      wasOpenRef.current = false
      return
    }

    const accountIds = new Set(accounts.map((account) => account.id))
    if (!wasOpenRef.current) {
      wasOpenRef.current = true
      setSelectedIds(
        new Set(initialSelectedIds.filter((id) => accountIds.has(id)))
      )
      setDrafts(
        Object.fromEntries(
          accounts.map((account) => [account.id, account.remark])
        )
      )
      return
    }

    setSelectedIds(
      (current) => new Set([...current].filter((id) => accountIds.has(id)))
    )
    setDrafts((current) =>
      Object.fromEntries(
        accounts.map((account) => [
          account.id,
          current[account.id] ?? account.remark,
        ])
      )
    )
  }, [accounts, initialSelectedIds, open])

  const selectedAccounts = useMemo(
    () => accounts.filter((account) => selectedIds.has(account.id)),
    [accounts, selectedIds]
  )
  const selectedCount = selectedAccounts.length
  const allSelected = accounts.length > 0 && selectedCount === accounts.length
  const someSelected = selectedCount > 0 && !allSelected
  const activeSelected = selectedAccounts.some((account) => account.active)
  const changedUpdates = useMemo(
    () =>
      accounts.flatMap((account) => {
        const remark = (drafts[account.id] ?? account.remark).trim()
        return remark === account.remark ? [] : [{ id: account.id, remark }]
      }),
    [accounts, drafts]
  )
  const frozen = Boolean(busy)

  const close = () => {
    setDeleteOpen(false)
    setSelectedIds(new Set())
    wasOpenRef.current = false
    onOpenChange(false)
  }

  const requestOpenChange = (nextOpen: boolean) => {
    if (!nextOpen && frozen) return
    if (!nextOpen) {
      setDeleteOpen(false)
      setSelectedIds(new Set())
      wasOpenRef.current = false
    }
    onOpenChange(nextOpen)
  }

  const toggleAccount = useCallback(
    (id: string, checked: boolean) => {
      if (busy) return
      setSelectedIds((current) => {
        const next = new Set(current)
        if (checked) next.add(id)
        else next.delete(id)
        return next
      })
    },
    [busy]
  )

  const updateDraft = useCallback(
    (id: string, value: string) => {
      if (busy) return
      setDrafts((current) => ({ ...current, [id]: value }))
    },
    [busy]
  )

  const fallbackCandidates = () =>
    buildFallbackCandidates(accounts, providers, selectedIds)

  const requestDelete = () => {
    if (!selectedCount || busy) return
    if (changedUpdates.length > 0) {
      toast.add({
        title: "请先保存账号备注",
        description: "保存备注后即可继续删除所选账号。",
        type: "error",
      })
      return
    }
    if (activeSelected && fallbackCandidates().length === 0) {
      toast.add({
        title: "无法删除所选账号",
        description: "至少保留一个可用连接后，才能删除这些账号。",
        type: "error",
      })
      return
    }
    setDeleteOpen(true)
  }

  const saveRemarks = async () => {
    if (!changedUpdates.length || !beginBusy("saving")) return
    try {
      await call("connections_update_account_remarks", {
        updates: changedUpdates,
      })
      toast.add({ title: "账号备注已保存", type: "success" })
      onRefresh()
    } catch (reason) {
      toast.add({
        title: "保存账号备注失败",
        description: errorMessage(reason),
        type: "error",
      })
    } finally {
      endBusy("saving")
    }
  }

  const deleteAccounts = async () => {
    if (!selectedCount || !beginBusy("deleting")) return
    const ids = selectedAccounts.map((account) => account.id)
    const deletingIds = new Set(ids)
    let switchedId: string | undefined
    let switchRepair: RepairResult | undefined
    let lastSwitchError: unknown

    try {
      if (activeSelected) {
        const switchResult = await switchActiveConnection(fallbackCandidates())
        switchedId = switchResult.switchedId
        switchRepair = switchResult.repair
        lastSwitchError = switchResult.error

        if (!switchedId) {
          onRefresh()
          toast.add({
            title: "无法切换连接，未删除账号",
            description: lastSwitchError
              ? `其余连接均无法启用：${errorMessage(lastSwitchError)}`
              : "至少保留一个可用连接后，才能删除当前账号。",
            type: "error",
          })
          return
        }
      }

      await call("connections_delete_accounts", { ids })

      if (!selectedId || deletingIds.has(selectedId)) {
        const remainingAccounts = accounts.filter(
          (account) => !deletingIds.has(account.id)
        )
        const nextId =
          switchedId ??
          remainingAccounts.find((account) => account.active)?.id ??
          providers.find((provider) => provider.active)?.id ??
          remainingAccounts[0]?.id ??
          providers[0]?.id ??
          ""
        onSelectedIdChange(nextId)
      }

      toast.add({
        title: `已删除 ${ids.length} 个账号`,
        description: switchRepair ? repairWarning(switchRepair) : undefined,
        type: "success",
      })
      onRefresh()
      close()
    } catch (reason) {
      onRefresh()
      if (switchedId) {
        toast.add({
          title: "已切换连接，但删除账号失败",
          description: errorMessage(reason),
          type: "error",
        })
      } else {
        toast.add({
          title: "批量删除账号失败",
          description: errorMessage(reason),
          type: "error",
        })
      }
      setDeleteOpen(false)
    } finally {
      endBusy("deleting")
    }
  }

  return (
    <>
      <Dialog open={open} onOpenChange={requestOpenChange}>
        <DialogContent
          size="wide"
          showCloseButton={!frozen}
          overlayClassName={deleteOpen ? hiddenOverlayStyles : undefined}
          aria-busy={frozen}
        >
          <DialogHeader>
            <DialogTitle>批量管理账号</DialogTitle>
          </DialogHeader>

          <DialogBody>
            <div className="rounded-2xl border">
              <Table>
                <TableCaption className="sr-only">
                  OpenAI 账号、当前连接状态与本机备注
                </TableCaption>
                <TableHeader className="sticky top-0 z-10 bg-popover">
                  <TableRow>
                    <TableHead className="w-10">
                      <Checkbox
                        aria-label="选择全部账号"
                        checked={allSelected}
                        indeterminate={someSelected}
                        disabled={frozen || accounts.length === 0}
                        onCheckedChange={(checked) => {
                          if (busy) return
                          setSelectedIds(
                            checked
                              ? new Set(accounts.map((account) => account.id))
                              : new Set()
                          )
                        }}
                      />
                    </TableHead>
                    <TableHead>账号</TableHead>
                    <TableHead className="w-28">状态</TableHead>
                    <TableHead>备注</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {accounts.map((account) => (
                    <AccountRow
                      key={account.id}
                      account={account}
                      checked={selectedIds.has(account.id)}
                      remark={drafts[account.id] ?? account.remark}
                      frozen={frozen}
                      onToggle={toggleAccount}
                      onRemarkChange={updateDraft}
                    />
                  ))}
                </TableBody>
              </Table>
            </div>
          </DialogBody>

          <DialogFooter className="items-center">
            <span className="mr-auto text-xs text-muted-foreground">
              已选择 {selectedCount} 个账号
            </span>
            <Button
              type="button"
              variant="destructive"
              disabled={frozen || selectedCount === 0}
              onClick={requestDelete}
            >
              <HugeiconsIcon icon={Delete02Icon} data-icon="inline-start" />
              删除所选
            </Button>
            <Button
              type="button"
              variant="outline"
              disabled={frozen}
              onClick={() => requestOpenChange(false)}
            >
              取消
            </Button>
            <Button
              type="button"
              disabled={frozen || changedUpdates.length === 0}
              onClick={() => void saveRemarks()}
            >
              {busy === "saving" && <Spinner data-icon="inline-start" />}
              保存备注
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <AlertDialog
        open={deleteOpen}
        onOpenChange={(nextOpen) => {
          if (!nextOpen && busy) return
          setDeleteOpen(nextOpen)
        }}
      >
        <AlertDialogContent aria-busy={busy === "deleting"}>
          <AlertDialogHeader>
            <AlertDialogTitle>删除 {selectedCount} 个账号？</AlertDialogTitle>
            <AlertDialogDescription>
              {activeSelected
                ? "所选包含当前账号。程序会先依次尝试切换到其他可用连接，再原子删除所选账号。"
                : "此操作会移除所选账号在本机保存的登录信息，无法撤销。"}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={frozen}>取消</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              disabled={frozen}
              aria-busy={busy === "deleting"}
              onClick={() => void deleteAccounts()}
            >
              {busy === "deleting" && <Spinner data-icon="inline-start" />}
              确认删除
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}
