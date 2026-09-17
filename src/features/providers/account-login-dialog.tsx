import { useRef, useState } from "react"
import {
  Copy01Icon,
  ExternalLinkIcon,
  InformationCircleIcon,
  Key01Icon,
  Login03Icon,
} from "@hugeicons/core-free-icons"
import { HugeiconsIcon } from "@hugeicons/react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
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
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty"
import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
} from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Spinner } from "@/components/ui/spinner"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Textarea } from "@/components/ui/textarea"
import { toast } from "@/components/ui/toast"
import { writeClipboardText } from "@/lib/clipboard"
import { errorMessage } from "@/lib/format"
import type { DeviceAuthorization } from "@/types"

const MAX_DISPLAY_NAME_LENGTH = 128
const MAX_ACCOUNT_ID_LENGTH = 512
const MAX_COOKIE_CREDENTIAL_LENGTH = 262_144

export type AccountLoginMode = "browser" | "cookie"

export function AccountLoginDialog({
  open,
  mode,
  onModeChange,
  onOpenChange,
  authorization,
  starting,
  polling,
  importing,
  error,
  onStart,
  onOpenPage,
  onCheck,
  onImport,
}: {
  open: boolean
  mode: AccountLoginMode
  onModeChange: (mode: AccountLoginMode) => void
  onOpenChange: (open: boolean) => void
  authorization?: DeviceAuthorization
  starting: boolean
  polling: boolean
  importing: boolean
  error?: string
  onStart: () => void
  onOpenPage: () => void
  onCheck: () => void
  onImport: (
    name: string | undefined,
    accountId: string | undefined,
    content: string
  ) => void
}) {
  const [name, setName] = useState("")
  const [accountId, setAccountId] = useState("")
  const [hasContent, setHasContent] = useState(false)
  const contentRef = useRef<HTMLTextAreaElement>(null)
  const busy = starting || polling || importing

  const copyAuthorizationCode = async () => {
    if (!authorization) return
    try {
      await writeClipboardText(authorization.userCode)
    } catch (reason) {
      toast.add({
        title: "复制失败",
        description: errorMessage(reason),
        type: "error",
      })
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent showCloseButton={!busy} aria-busy={busy}>
        <DialogHeader>
          <DialogTitle>登录 OpenAI 账号</DialogTitle>
        </DialogHeader>

        <DialogBody>
          {error && (
            <Alert variant="destructive">
              <HugeiconsIcon icon={InformationCircleIcon} />
              <AlertTitle>无法完成登录</AlertTitle>
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          <Tabs
            value={mode}
            onValueChange={(value) => onModeChange(value as AccountLoginMode)}
          >
            <TabsList className="grid w-full grid-cols-2">
              <TabsTrigger value="browser" disabled={busy}>
                <HugeiconsIcon icon={Login03Icon} data-icon="inline-start" />
                官方授权
              </TabsTrigger>
              <TabsTrigger value="cookie" disabled={busy}>
                <HugeiconsIcon icon={Key01Icon} data-icon="inline-start" />
                Cookie 导入
              </TabsTrigger>
            </TabsList>

            <TabsContent value="browser" className="flex flex-col gap-3 pt-1">
              {authorization ? (
                <Alert>
                  <HugeiconsIcon icon={Login03Icon} />
                  <AlertTitle className="flex flex-wrap items-center gap-2">
                    授权码：
                    <code className="font-mono font-semibold">
                      {authorization.userCode}
                    </code>
                    <Badge variant="outline">
                      <Spinner data-icon="inline-start" />
                      等待授权
                    </Badge>
                  </AlertTitle>
                  <AlertDescription>
                    请在 OpenAI
                    授权页面输入此代码。授权完成后，本应用会自动保存账号并更新登录状态。
                  </AlertDescription>
                </Alert>
              ) : (
                <Empty className="min-h-24 bg-muted">
                  <EmptyHeader>
                    {starting ? (
                      <>
                        <EmptyMedia variant="icon">
                          <Spinner />
                        </EmptyMedia>
                        <EmptyTitle>正在获取授权码…</EmptyTitle>
                      </>
                    ) : (
                      <>
                        <EmptyMedia variant="icon">
                          <HugeiconsIcon icon={Login03Icon} />
                        </EmptyMedia>
                        <EmptyTitle>使用 OpenAI 官方设备授权</EmptyTitle>
                        <EmptyDescription>
                          获取一次性授权码后，在 OpenAI
                          页面确认登录；本应用不会读取浏览器 Cookie。
                        </EmptyDescription>
                      </>
                    )}
                  </EmptyHeader>
                  {!starting && (
                    <EmptyContent>
                      <Button type="button" disabled={busy} onClick={onStart}>
                        <HugeiconsIcon
                          icon={Login03Icon}
                          data-icon="inline-start"
                        />
                        获取授权码
                      </Button>
                    </EmptyContent>
                  )}
                </Empty>
              )}

              {authorization && (
                <div className="flex flex-wrap gap-2">
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={() => void copyAuthorizationCode()}
                  >
                    <HugeiconsIcon icon={Copy01Icon} data-icon="inline-start" />
                    复制授权码
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={onOpenPage}
                  >
                    <HugeiconsIcon
                      icon={ExternalLinkIcon}
                      data-icon="inline-start"
                    />
                    打开授权页面
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    disabled={busy}
                    onClick={onCheck}
                  >
                    {polling && <Spinner data-icon="inline-start" />}
                    检查授权结果
                  </Button>
                </div>
              )}
            </TabsContent>

            <TabsContent value="cookie" className="pt-1">
              <FieldGroup>
                <Field data-disabled={busy}>
                  <FieldLabel htmlFor="cookie-account-name">
                    显示名称（可选）
                  </FieldLabel>
                  <Input
                    id="cookie-account-name"
                    disabled={busy}
                    maxLength={MAX_DISPLAY_NAME_LENGTH}
                    placeholder="例如：工作账号"
                    value={name}
                    onChange={(event) => setName(event.target.value)}
                  />
                </Field>
                <Field data-disabled={busy}>
                  <FieldLabel htmlFor="cookie-account-id">
                    ChatGPT Account ID（可选）
                  </FieldLabel>
                  <Input
                    id="cookie-account-id"
                    autoComplete="off"
                    disabled={busy}
                    maxLength={MAX_ACCOUNT_ID_LENGTH}
                    placeholder="团队账号可填写；个人账号通常留空"
                    value={accountId}
                    onChange={(event) => setAccountId(event.target.value)}
                  />
                  <FieldDescription>
                    用于识别团队空间和查询额度。导入内容已包含 Account ID
                    时无需重复填写。
                  </FieldDescription>
                </Field>
                <Field data-disabled={busy}>
                  <FieldLabel htmlFor="cookie-account-content">
                    Cookie 登录数据
                  </FieldLabel>
                  <Textarea
                    ref={contentRef}
                    id="cookie-account-content"
                    autoCapitalize="none"
                    className="field-sizing-fixed h-28 max-h-28 min-h-28 max-w-full resize-none overflow-y-auto font-mono text-xs break-all"
                    autoComplete="off"
                    autoCorrect="off"
                    data-1p-ignore
                    data-lpignore="true"
                    disabled={busy}
                    spellCheck={false}
                    wrap="soft"
                    maxLength={MAX_COOKIE_CREDENTIAL_LENGTH}
                    placeholder={
                      "粘贴 Refresh Token、Access Token，或账号工具导出的 JSON"
                    }
                    onInput={(event) =>
                      setHasContent(/\S/.test(event.currentTarget.value))
                    }
                  />
                  <FieldDescription>
                    支持单个 Token、单账号 JSON 和账号数组，并可识别
                    CPA、Sub2API、Cockpit、9router
                    的导出格式。只会提取登录所需字段，原始内容不会保存。
                  </FieldDescription>
                </Field>
              </FieldGroup>
            </TabsContent>
          </Tabs>
        </DialogBody>

        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={busy}
            onClick={() => onOpenChange(false)}
          >
            取消
          </Button>
          {mode === "cookie" && (
            <Button
              type="button"
              disabled={busy || !hasContent}
              onClick={() => {
                const content = contentRef.current?.value ?? ""
                if (!content.trim()) return
                onImport(
                  name.trim() || undefined,
                  accountId.trim() || undefined,
                  content
                )
              }}
            >
              {importing ? (
                <Spinner data-icon="inline-start" />
              ) : (
                <HugeiconsIcon icon={Key01Icon} data-icon="inline-start" />
              )}
              {importing ? "正在验证并导入…" : "导入 Cookie 数据"}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
