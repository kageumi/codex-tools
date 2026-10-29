import { useRef, useState } from "react"

import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogBody,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
} from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Spinner } from "@/components/ui/spinner"
import { toast } from "@/components/ui/toast"
import { errorMessage } from "@/lib/format"
import { call } from "@/lib/ipc"
import type { OfficialAccountView } from "@/types"

export function AccountRemarkDialog({
  account,
  open,
  onOpenChange,
  onSaved,
}: {
  account?: OfficialAccountView
  open: boolean
  onOpenChange: (open: boolean) => void
  onSaved: () => void
}) {
  const [draft, setDraft] = useState(account?.remark ?? "")
  const [saving, setSaving] = useState(false)
  const savingRef = useRef(false)

  const close = () => {
    if (!savingRef.current) onOpenChange(false)
  }

  const save = async () => {
    if (!account || savingRef.current || draft.trim() === account.remark) return
    savingRef.current = true
    setSaving(true)
    try {
      await call("connections_update_account_remark", {
        id: account.id,
        remark: draft,
      })
      onSaved()
      toast.add({ title: "账号备注已保存", type: "success" })
      onOpenChange(false)
    } catch (reason) {
      toast.add({
        title: "保存失败",
        description: errorMessage(reason),
        type: "error",
      })
    } finally {
      savingRef.current = false
      setSaving(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={close}>
      <DialogContent showCloseButton={!saving} aria-busy={saving}>
        <DialogHeader>
          <DialogTitle>编辑账号备注</DialogTitle>
        </DialogHeader>
        <DialogBody>
          <FieldGroup>
            <Field data-disabled={saving}>
              <FieldLabel htmlFor="account-remark">账号备注</FieldLabel>
              <Input
                id="account-remark"
                autoFocus
                disabled={saving}
                maxLength={200}
                placeholder={account?.name || "例如：工作账号"}
                value={draft}
                onChange={(event) => setDraft(event.target.value)}
              />
              <FieldDescription>留空可恢复显示账号原名称。</FieldDescription>
            </Field>
          </FieldGroup>
        </DialogBody>
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={saving}
            onClick={close}
          >
            取消
          </Button>
          <Button
            type="button"
            disabled={saving || !account || draft.trim() === account.remark}
            onClick={() => void save()}
          >
            {saving && <Spinner data-icon="inline-start" />}
            保存
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
