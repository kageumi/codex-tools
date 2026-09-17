import { useEffect, useState } from "react"
import { open } from "@tauri-apps/plugin-dialog"
import {
  CheckmarkCircle02Icon,
  CodeIcon,
  Copy01Icon,
  FolderOpenIcon,
  InformationCircleIcon,
  PlayIcon,
  Refresh01Icon,
  Wrench01Icon,
} from "@hugeicons/core-free-icons"
import { HugeiconsIcon } from "@hugeicons/react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
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
  Dialog,
  DialogBody,
  DialogContent,
  DialogDescription,
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
import {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupInput,
} from "@/components/ui/input-group"
import {
  Item,
  ItemContent,
  ItemDescription,
  ItemGroup,
  ItemMedia,
  ItemTitle,
} from "@/components/ui/item"
import { Skeleton } from "@/components/ui/skeleton"
import { Spinner } from "@/components/ui/spinner"
import { toast } from "@/components/ui/toast"
import { repairWarning } from "@/features/providers/connection-utils"
import { errorMessage } from "@/lib/format"
import { writeClipboardText } from "@/lib/clipboard"
import { useAsyncAction } from "@/hooks/use-async-action"
import { call } from "@/lib/ipc"
import { createRequestGate } from "@/lib/request-gate"
import type { RequestGate } from "@/lib/request-gate"
import type {
  CodexAppSetting,
  ConfigPatchPreview,
  ModelUnlockStatus,
  SettingsOverview,
  SettingsSection,
  SupportDiagnostics,
} from "@/types"

export function SettingsPage({
  refreshRevision,
  section,
  onRefresh,
}: {
  refreshRevision: number
  onRefresh: () => void
  section: SettingsSection
}) {
  const [overview, setOverview] = useState<SettingsOverview>()
  const [app, setApp] = useState<CodexAppSetting>()
  const [unlock, setUnlock] = useState<ModelUnlockStatus>()
  const [diagnostics, setDiagnostics] = useState<SupportDiagnostics>()
  const [preview, setPreview] = useState<{
    value: ConfigPatchPreview
    request: ReturnType<RequestGate["begin"]>
  }>()
  const [previewRequests] = useState(createRequestGate)
  const { busy, begin, end, run } = useAsyncAction()
  const [loadError, setLoadError] = useState<{
    section: SettingsSection
    message: string
  }>()

  useEffect(() => {
    previewRequests.invalidate()
    let cancelled = false
    const request =
      section === "diagnostics"
        ? call("settings_get_diagnostics").then((next) => {
            if (!cancelled) {
              setDiagnostics(next)
              setLoadError(undefined)
            }
          })
        : section === "app"
          ? call("settings_get_codex_app").then((next) => {
              if (!cancelled) {
                setApp(next)
                setLoadError(undefined)
              }
            })
          : section === "unlock"
            ? call("settings_model_unlock_status").then((next) => {
                if (!cancelled) {
                  setUnlock(next)
                  setLoadError(undefined)
                }
              })
            : call("settings_get_overview").then((next) => {
                if (!cancelled) {
                  setOverview(next)
                  setLoadError(undefined)
                }
              })
    void request.catch((reason) => {
      if (cancelled) return
      const message = errorMessage(reason)
      setLoadError({ section, message })
      toast.add({
        title: "无法读取设置",
        description: message,
        type: "error",
      })
    })
    return () => {
      cancelled = true
    }
  }, [previewRequests, refreshRevision, section])

  const onPreview = async () => {
    if (!begin("preview")) return
    const request = previewRequests.begin()
    let retained = false
    try {
      const next = await call("settings_preview_activation")
      if (previewRequests.isCurrent(request)) {
        setPreview({ value: next, request })
        retained = true
      }
    } catch (reason) {
      if (!previewRequests.isCurrent(request)) return
      toast.add({
        title: "无法生成预览",
        description: errorMessage(reason),
        type: "error",
      })
    } finally {
      if (!retained) previewRequests.finish(request)
      end("preview")
    }
  }

  const loading =
    (section === "config" && !overview) ||
    (section === "diagnostics" && !diagnostics) ||
    (section === "app" && !app) ||
    (section === "unlock" && !unlock)

  const sectionError = loadError?.section === section ? loadError.message : null
  const activePreview =
    section === "config" &&
    preview &&
    previewRequests.isCurrent(preview.request)
      ? preview.value
      : undefined
  const closePreview = () => {
    if (preview) previewRequests.finish(preview.request)
    setPreview(undefined)
  }

  if (loading && sectionError)
    return (
      <div className="min-h-full px-3 pt-1 pb-3">
        <Alert variant="destructive">
          <HugeiconsIcon icon={InformationCircleIcon} />
          <AlertTitle>无法读取设置</AlertTitle>
          <AlertDescription>{sectionError}</AlertDescription>
        </Alert>
      </div>
    )

  if (loading)
    return (
      <div className="min-h-full px-3 pt-1 pb-3" role="status" aria-busy="true">
        <span className="sr-only">正在读取设置</span>
        <Skeleton className="min-h-full rounded-2xl" />
      </div>
    )

  return (
    <div className="flex min-h-full flex-col gap-3 px-3 pt-1 pb-3">
      {sectionError && (
        <Alert variant="destructive">
          <HugeiconsIcon icon={InformationCircleIcon} />
          <AlertTitle>设置刷新失败</AlertTitle>
          <AlertDescription>{sectionError}</AlertDescription>
        </Alert>
      )}
      {section === "config" && (
        <ConfigSection
          overview={overview!}
          busy={busy}
          onPreview={onPreview}
          onOfficial={() =>
            void run("official", () => call("connections_activate_official"), {
              success: "已切换为 OpenAI 官方配置",
              successDescription: repairWarning,
              onSuccess: onRefresh,
            })
          }
        />
      )}
      {section === "diagnostics" && (
        <DiagnosticsSection diagnostics={diagnostics!} />
      )}
      {section === "app" && (
        <AppSection
          key={`${app!.configured ?? "auto"}:${app!.detected ?? "missing"}`}
          app={app!}
          busy={busy}
          onSave={(path) =>
            void run(
              "app",
              () => call("settings_save_codex_app_path", { path }),
              { success: "桌面应用路径已保存", onSuccess: onRefresh }
            )
          }
        />
      )}
      {section === "unlock" && (
        <UnlockSection
          status={unlock!}
          busy={busy}
          onRefresh={onRefresh}
          onUnlock={() =>
            void run("unlock", () => call("settings_unlock_models"), {
              success: "模型已注入 Codex",
              onSuccess: onRefresh,
            })
          }
          onDebug={() =>
            void run("debug", () => call("settings_launch_codex_debug"), {
              success: "Codex 调试实例已启动",
              onSuccess: onRefresh,
            })
          }
        />
      )}

      <Dialog
        open={Boolean(activePreview)}
        onOpenChange={(open) => {
          if (!open && busy === "apply") return
          closePreview()
        }}
      >
        <DialogContent
          showCloseButton={busy !== "apply"}
          aria-busy={busy === "apply"}
        >
          <DialogHeader>
            <DialogTitle>配置变更预览</DialogTitle>
            <DialogDescription>{activePreview?.targetPath}</DialogDescription>
          </DialogHeader>
          <DialogBody>
            {activePreview?.changes.map((change) => (
              <div key={change} className="rounded-2xl bg-muted px-3 py-2">
                {change}
              </div>
            ))}
            <pre className="max-h-56 overflow-auto overscroll-contain rounded-2xl bg-muted p-3 text-xs whitespace-pre-wrap">
              {activePreview?.rendered}
            </pre>
          </DialogBody>
          <DialogFooter>
            <Button
              variant="outline"
              disabled={busy === "apply"}
              onClick={closePreview}
            >
              取消
            </Button>
            <Button
              disabled={Boolean(busy)}
              onClick={() =>
                activePreview &&
                void run(
                  "apply",
                  () =>
                    call("settings_apply_activation", {
                      operationId: activePreview.operationId,
                    }),
                  {
                    success: "配置已应用",
                    onSuccess: () => {
                      onRefresh()
                      closePreview()
                    },
                  }
                )
              }
            >
              {busy === "apply" && <Spinner data-icon="inline-start" />}应用配置
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

function ConfigSection({
  overview,
  busy,
  onPreview,
  onOfficial,
}: {
  overview: SettingsOverview
  busy?: string
  onPreview: () => Promise<void>
  onOfficial: () => void
}) {
  const inspection = overview.inspection
  return (
    <Card size="sm" className="flex-1">
      <CardHeader className="border-b">
        <div className="flex items-center gap-2">
          <CardTitle>当前配置</CardTitle>
          <Badge variant={inspection.valid ? "secondary" : "destructive"}>
            {inspection.valid ? "配置有效" : "需要处理"}
          </Badge>
        </div>
        <CardDescription>{inspection.path}</CardDescription>
      </CardHeader>
      <CardContent className="grid gap-3">
        <div className="grid grid-cols-2 gap-4">
          <Detail
            label="当前 Provider"
            value={inspection.activeProvider ?? "OpenAI 官方"}
          />
          <Detail
            label="托管配置"
            value={inspection.managedProviderPresent ? "已写入" : "未写入"}
          />
        </div>
        {inspection.warnings.length > 0 && (
          <Alert variant="destructive">
            <HugeiconsIcon icon={InformationCircleIcon} />
            <AlertTitle>配置警告</AlertTitle>
            <AlertDescription>
              {inspection.warnings.join("；")}
            </AlertDescription>
          </Alert>
        )}
        <div className="flex gap-2">
          <Button
            disabled={!overview.canPreviewCustom || Boolean(busy)}
            onClick={() => void onPreview()}
          >
            {busy === "preview" ? (
              <Spinner data-icon="inline-start" />
            ) : (
              <HugeiconsIcon icon={CodeIcon} data-icon="inline-start" />
            )}
            预览自定义配置
          </Button>
          <Button
            variant="outline"
            disabled={Boolean(busy)}
            onClick={onOfficial}
          >
            恢复 OpenAI 官方
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}

function DiagnosticsSection({
  diagnostics,
}: {
  diagnostics: SupportDiagnostics
}) {
  const content = JSON.stringify(diagnostics, null, 2)
  const report = [
    "## Codex Tools 支持报告",
    "",
    "> 报告已自动隐藏密钥、Token、账号标识、邮箱、代理地址，并将用户目录替换为 ~。",
    "",
    "```json",
    content,
    "```",
  ].join("\n")
  const copyReport = async () => {
    try {
      await writeClipboardText(report)
      toast.add({ title: "支持报告已复制", type: "success" })
    } catch (reason) {
      toast.add({
        title: "复制失败",
        description: errorMessage(reason),
        type: "error",
      })
    }
  }
  const proxyConfigured =
    diagnostics.network.environmentProxyConfigured ||
    diagnostics.network.systemProxyConfigured
  return (
    <Card size="sm" className="flex-1">
      <CardHeader className="border-b">
        <CardTitle>诊断信息</CardTitle>
        <CardDescription>
          可直接提交给开发者 · {formatDiagnosticTime(diagnostics.generatedAt)}
        </CardDescription>
        <CardAction>
          <Button size="sm" variant="outline" onClick={() => void copyReport()}>
            <HugeiconsIcon icon={Copy01Icon} data-icon="inline-start" />
            复制支持报告
          </Button>
        </CardAction>
      </CardHeader>
      <CardContent className="grid gap-3">
        <Alert>
          <HugeiconsIcon icon={CheckmarkCircle02Icon} />
          <AlertTitle>已自动脱敏</AlertTitle>
          <AlertDescription>
            不包含 API Key、Cookie、OAuth
            Token、账号标识、邮箱、自定义请求头值或代理地址。
          </AlertDescription>
        </Alert>

        <div className="grid grid-cols-2 gap-4">
          <Detail
            label="应用与系统"
            value={`${diagnostics.app.version} · ${diagnostics.system.os}/${diagnostics.system.architecture}`}
          />
          <Detail
            label="配置"
            value={diagnostics.configuration.valid ? "有效" : "需要处理"}
          />
          <Detail
            label="用量数据库"
            value={
              diagnostics.storage.usageDatabase.quickCheck === "ok"
                ? `正常 · ${diagnostics.storage.usageDatabase.eventCount ?? 0} 条事件`
                : diagnostics.storage.usageDatabase.quickCheck
            }
          />
          <Detail
            label="会话与网络"
            value={`${diagnostics.storage.indexedSessionCount} 个会话 · ${proxyConfigured ? "已配置代理" : "直连"}`}
          />
        </div>

        {diagnostics.warnings.length > 0 && (
          <Alert variant="destructive">
            <HugeiconsIcon icon={InformationCircleIcon} />
            <AlertTitle>检测到 {diagnostics.warnings.length} 项问题</AlertTitle>
            <AlertDescription>
              {diagnostics.warnings.join("；")}
            </AlertDescription>
          </Alert>
        )}

        <pre className="max-h-64 overflow-auto overscroll-contain rounded-2xl bg-muted p-3 text-xs whitespace-pre-wrap">
          {content}
        </pre>
      </CardContent>
    </Card>
  )
}

function formatDiagnosticTime(value: string) {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

function AppSection({
  app,
  busy,
  onSave,
}: {
  app: CodexAppSetting
  busy?: string
  onSave: (path: string | null) => void
}) {
  const [path, setPath] = useState(app.configured ?? app.detected ?? "")
  const choose = async () => {
    try {
      const isWindows = /Windows/i.test(navigator.userAgent)
      const result = await open({
        // macOS 可选择 .app 应用包或 Contents/MacOS 内的可执行文件；
        // Windows 选择实际的 .exe。
        directory: false,
        multiple: false,
        filters: isWindows
          ? [
              {
                name: "ChatGPT / Codex 可执行文件",
                extensions: ["exe"],
              },
            ]
          : undefined,
        title: "选择 ChatGPT 或 Codex 启动程序",
      })
      if (typeof result === "string") {
        setPath(result)
        onSave(result)
      }
    } catch (reason) {
      toast.add({
        title: "无法打开应用选择器",
        description: `${errorMessage(reason)}；也可以直接在输入框中填写路径。`,
        type: "error",
      })
    }
  }
  return (
    <Card size="sm" className="flex-1">
      <CardHeader className="border-b">
        <CardTitle>ChatGPT / Codex 启动程序</CardTitle>
        <CardDescription>
          手动指定用于启动和模型解锁的应用或可执行文件。
        </CardDescription>
      </CardHeader>
      <CardContent>
        <FieldGroup>
          <Field>
            <FieldLabel htmlFor="codex-path">
              ChatGPT / Codex 执行程序地址
            </FieldLabel>
            <InputGroup>
              <InputGroupInput
                id="codex-path"
                value={path}
                disabled={Boolean(busy)}
                placeholder="/Applications/Codex.app 或 .../Contents/MacOS/Codex"
                onChange={(event) => setPath(event.target.value)}
              />
              <InputGroupAddon align="inline-end">
                <InputGroupButton
                  size="sm"
                  disabled={Boolean(busy)}
                  aria-label="手动选择 ChatGPT 或 Codex 执行程序"
                  onClick={() => void choose()}
                >
                  <HugeiconsIcon
                    icon={FolderOpenIcon}
                    data-icon="inline-start"
                  />
                  选择程序
                </InputGroupButton>
              </InputGroupAddon>
            </InputGroup>
            <FieldDescription>
              {app.configured ? "当前为手动路径" : "当前使用自动检测"}
              ；检测结果：
              {app.detected ?? "未找到 ChatGPT 或 Codex"}
              。支持 macOS .app、应用内部可执行文件和 Windows .exe。
            </FieldDescription>
          </Field>
          <div className="flex gap-2">
            <Button
              disabled={Boolean(busy)}
              onClick={() => onSave(path || null)}
            >
              {busy === "app" && <Spinner data-icon="inline-start" />}保存路径
            </Button>
            <Button
              variant="outline"
              disabled={Boolean(busy)}
              onClick={() => {
                setPath("")
                onSave(null)
              }}
            >
              恢复自动检测
            </Button>
          </div>
        </FieldGroup>
      </CardContent>
    </Card>
  )
}

function UnlockSection({
  status,
  busy,
  onRefresh,
  onUnlock,
  onDebug,
}: {
  status: ModelUnlockStatus
  busy?: string
  onRefresh: () => void
  onUnlock: () => void
  onDebug: () => void
}) {
  return (
    <Card size="sm" className="flex-1">
      <CardHeader className="border-b">
        <div className="flex items-center gap-2">
          <CardTitle>模型解锁</CardTitle>
          <Badge variant={status.injected ? "secondary" : "outline"}>
            {status.injected ? "已注入" : "未注入"}
          </Badge>
        </div>
        <CardDescription>
          向 Codex 桌面端注入当前服务的可用模型。
        </CardDescription>
      </CardHeader>
      <CardContent className="grid gap-3">
        <ItemGroup>
          <StateItem label="找到应用" value={status.appFound ? "是" : "否"} />
          <StateItem
            label="应用运行中"
            value={status.appRunning ? "是" : "否"}
          />
          <StateItem
            label="调试端口"
            value={status.debugPort ? String(status.debugPort) : "未连接"}
          />
          <StateItem label="模型数量" value={`${status.modelCount} 个`} />
        </ItemGroup>
        {status.warning && (
          <Alert>
            <HugeiconsIcon icon={InformationCircleIcon} />
            <AlertTitle>提示</AlertTitle>
            <AlertDescription>{status.warning}</AlertDescription>
          </Alert>
        )}
        <div className="flex flex-wrap gap-2">
          <Button
            disabled={Boolean(busy) || !status.appFound}
            onClick={onUnlock}
          >
            {busy === "unlock" ? (
              <Spinner data-icon="inline-start" />
            ) : (
              <HugeiconsIcon icon={Wrench01Icon} data-icon="inline-start" />
            )}
            解锁模型
          </Button>
          <Button variant="outline" disabled={Boolean(busy)} onClick={onDebug}>
            {busy === "debug" ? (
              <Spinner data-icon="inline-start" />
            ) : (
              <HugeiconsIcon icon={PlayIcon} data-icon="inline-start" />
            )}
            启动调试实例
          </Button>
          <Button
            size="icon"
            variant="ghost"
            disabled={Boolean(busy)}
            aria-label="刷新状态"
            onClick={onRefresh}
          >
            <HugeiconsIcon icon={Refresh01Icon} />
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}

function StateItem({ label, value }: { label: string; value: string }) {
  return (
    <Item size="xs" variant="outline">
      <ItemMedia variant="icon">
        <HugeiconsIcon icon={CheckmarkCircle02Icon} />
      </ItemMedia>
      <ItemContent>
        <ItemTitle>{label}</ItemTitle>
        <ItemDescription>{value}</ItemDescription>
      </ItemContent>
    </Item>
  )
}
function Detail({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="mt-1 font-medium">{value}</div>
    </div>
  )
}
