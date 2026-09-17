import {
  lazy,
  memo,
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react"
import {
  ArrowDown01Icon,
  ChartHistogramIcon,
  Clock01Icon,
  ComputerIcon,
  FilterIcon,
  Home01Icon,
  InformationCircleIcon,
  Link01Icon,
  Moon01Icon,
  Refresh01Icon,
  Rocket01Icon,
  Search01Icon,
  Settings01Icon,
  Sun01Icon,
} from "@hugeicons/core-free-icons"
import { HugeiconsIcon, type IconSvgElement } from "@hugeicons/react"
import { useTheme } from "next-themes"

import { Badge } from "@/components/ui/badge"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { ErrorBoundary } from "@/components/error-boundary"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field"
import {
  InputGroup,
  InputGroupAddon,
  InputGroupInput,
} from "@/components/ui/input-group"
import {
  Sheet,
  SheetBody,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet"
import { Spinner } from "@/components/ui/spinner"
import { Toaster, toast } from "@/components/ui/toast"
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { errorMessage } from "@/lib/format"
import { call } from "@/lib/ipc"
import type {
  Dashboard,
  Page,
  ProviderOverview,
  SettingsSection,
  UsageGroupBy,
} from "@/types"

const DashboardPage = memo(
  lazy(() =>
    import("@/features/dashboard/dashboard-page").then((module) => ({
      default: module.DashboardPage,
    }))
  )
)
const ProvidersPage = memo(
  lazy(() =>
    import("@/features/providers/providers-page").then((module) => ({
      default: module.ProvidersPage,
    }))
  )
)
const UsagePage = memo(
  lazy(() =>
    import("@/features/usage/usage-page").then((module) => ({
      default: module.UsagePage,
    }))
  )
)
const SessionsPage = memo(
  lazy(() =>
    import("@/features/sessions/sessions-page").then((module) => ({
      default: module.SessionsPage,
    }))
  )
)
const SettingsPage = memo(
  lazy(() =>
    import("@/features/settings/settings-page").then((module) => ({
      default: module.SettingsPage,
    }))
  )
)
const ConnectionManagerSheet = memo(
  lazy(() =>
    import("@/features/providers/connection-manager-sheet").then((module) => ({
      default: module.ConnectionManagerSheet,
    }))
  )
)

const navigation: Array<{
  id: Page
  label: string
  icon: IconSvgElement
}> = [
  { id: "dashboard", label: "概览", icon: Home01Icon },
  { id: "providers", label: "连接", icon: Link01Icon },
  { id: "usage", label: "用量", icon: ChartHistogramIcon },
  { id: "sessions", label: "会话", icon: Clock01Icon },
  { id: "settings", label: "设置", icon: Settings01Icon },
]

const contextMetadata: Record<Page, { icon: IconSvgElement; action: string }> =
  {
    dashboard: { icon: Link01Icon, action: "切换或管理连接" },
    providers: { icon: Link01Icon, action: "切换或管理连接" },
    usage: { icon: FilterIcon, action: "调整用量筛选" },
    sessions: { icon: Search01Icon, action: "搜索会话" },
    settings: { icon: Settings01Icon, action: "选择设置章节" },
  }

const settingsSectionLabels: Record<SettingsSection, string> = {
  config: "当前配置",
  diagnostics: "诊断信息",
  app: "启动程序",
  unlock: "模型解锁",
}

const themeOptions = [
  { id: "system", label: "跟随系统", icon: ComputerIcon },
  { id: "light", label: "浅色", icon: Sun01Icon },
  { id: "dark", label: "深色", icon: Moon01Icon },
] as const

type ThemeOptionId = (typeof themeOptions)[number]["id"]

function isThemeOptionId(value: unknown): value is ThemeOptionId {
  return value === "system" || value === "light" || value === "dark"
}

function usesConnectionManager(page: Page): page is "dashboard" | "providers" {
  return page === "dashboard" || page === "providers"
}

function usesSharedConnectionState(page: Page) {
  return usesConnectionManager(page) || page === "usage"
}

export default function App() {
  const mainRef = useRef<HTMLElement>(null)
  const [page, setPage] = useState<Page>("dashboard")
  const pageRef = useRef<Page>("dashboard")
  const [contextOpen, setContextOpen] = useState(false)
  const [contextMounted, setContextMounted] = useState(false)
  const [refreshRevision, setRefreshRevision] = useState(0)
  const [dashboard, setDashboard] = useState<Dashboard>()
  const [connections, setConnections] = useState<ProviderOverview>()
  const [selectedConnection, setSelectedConnection] = useState<string>()
  const [usageDays, setUsageDays] = useState(7)
  const [usageGroupBy, setUsageGroupBy] = useState<UsageGroupBy>("model")
  const [sessionQuery, setSessionQuery] = useState("")
  const [sessionQueryDraft, setSessionQueryDraft] = useState("")
  const [settingsSection, setSettingsSection] =
    useState<SettingsSection>("config")
  const [launching, setLaunching] = useState(false)
  const [stateLoading, setStateLoading] = useState(true)
  const [stateError, setStateError] = useState<string>()
  const loadedStateRevision = useRef<number | undefined>(undefined)
  const sharedStateActive = usesSharedConnectionState(page)

  useEffect(() => {
    if (!sharedStateActive) return
    if (loadedStateRevision.current === refreshRevision) return
    let cancelled = false
    void Promise.allSettled([call("dashboard_get"), call("connections_list")])
      .then(([dashboardResult, connectionsResult]) => {
        if (cancelled) return
        if (dashboardResult.status === "fulfilled") {
          setDashboard(dashboardResult.value)
        }
        if (connectionsResult.status === "fulfilled") {
          const nextConnections = connectionsResult.value
          setConnections(nextConnections)
          setSelectedConnection((current) => {
            const allConnections = [
              ...nextConnections.officialAccounts,
              ...nextConnections.providers,
            ]
            if (current && allConnections.some((item) => item.id === current)) {
              return current
            }
            return (
              nextConnections.officialAccounts.find((account) => account.active)
                ?.id ??
              nextConnections.providers.find((provider) => provider.active)
                ?.id ??
              allConnections[0]?.id
            )
          })
        }
        const errors = [dashboardResult, connectionsResult]
          .filter(
            (result): result is PromiseRejectedResult =>
              result.status === "rejected"
          )
          .map((result) => errorMessage(result.reason))
        const message = errors.join("；")
        setStateError(message || undefined)
        if (errors.length) {
          toast.add({
            title: "部分应用状态读取失败",
            description: message,
            type: "error",
          })
        } else {
          loadedStateRevision.current = refreshRevision
        }
      })
      .finally(() => {
        if (!cancelled) setStateLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [refreshRevision, sharedStateActive])

  const refresh = useCallback(() => {
    if (usesSharedConnectionState(pageRef.current)) {
      setStateLoading(true)
    }
    setRefreshRevision((value) => value + 1)
  }, [])

  const launch = useCallback(async () => {
    setLaunching(true)
    try {
      const result = await call("dashboard_launch")
      toast.add({
        title: "Codex 已启动",
        description: result.message,
        type: "success",
      })
      refresh()
    } catch (reason) {
      toast.add({
        title: "无法启动 Codex",
        description: errorMessage(reason),
        type: "error",
      })
    } finally {
      setLaunching(false)
    }
  }, [refresh])

  const contextLabel = useMemo(() => {
    if (page === "usage")
      return `最近 ${usageDays} 天 · ${usageGroupBy === "model" ? "按模型" : "按账号"}`
    if (page === "sessions") return sessionQuery || "全部会话"
    if (page === "settings") {
      return settingsSectionLabels[settingsSection]
    }
    if (page === "providers") {
      const selectedAccount = connections?.officialAccounts.find(
        (account) => account.id === selectedConnection
      )
      const selectedProvider = connections?.providers.find(
        (provider) => provider.id === selectedConnection
      )
      return (
        selectedAccount?.remark ||
        selectedAccount?.name ||
        selectedProvider?.name ||
        "选择连接"
      )
    }
    const activeAccount = connections?.officialAccounts.find(
      (account) => account.active
    )
    return (
      activeAccount?.remark ||
      dashboard?.activeAccount ||
      dashboard?.activeProvider ||
      "选择连接"
    )
  }, [
    connections,
    dashboard,
    page,
    selectedConnection,
    sessionQuery,
    settingsSection,
    usageDays,
    usageGroupBy,
  ])

  const context = contextMetadata[page]

  const sharedProps = useMemo(
    () => ({ refreshRevision, onRefresh: refresh }),
    [refreshRevision, refresh]
  )

  useEffect(() => {
    mainRef.current?.scrollTo({ top: 0 })
  }, [page])

  return (
    <TooltipProvider>
      <div className="grid size-full grid-cols-[52px_minmax(0,1fr)] overflow-hidden bg-background">
        <nav
          aria-label="主导航"
          className="flex min-h-0 flex-col items-center gap-2 border-r border-sidebar-border bg-sidebar px-2 py-2"
        >
          {navigation.map((item) => (
            <NavButton
              key={item.id}
              active={page === item.id}
              icon={item.icon}
              label={item.label}
              onClick={() => {
                pageRef.current = item.id
                const nextUsesSharedState = usesSharedConnectionState(item.id)
                if (!nextUsesSharedState) {
                  setStateLoading(false)
                } else if (!sharedStateActive) {
                  setStateLoading(
                    loadedStateRevision.current !== refreshRevision
                  )
                }
                setPage(item.id)
                setContextOpen(false)
              }}
            />
          ))}
        </nav>

        <div className="flex min-w-0 flex-col overflow-hidden">
          <header className="flex h-13 shrink-0 items-center gap-2 px-3">
            <Tooltip>
              <TooltipTrigger
                render={
                  <Button
                    variant="outline"
                    size="sm"
                    className="max-w-64 min-w-0"
                    aria-label={`${context.action}：${contextLabel}`}
                    aria-expanded={contextOpen}
                    onClick={() => {
                      setContextMounted(true)
                      if (page === "sessions")
                        setSessionQueryDraft(sessionQuery)
                      setContextOpen(true)
                    }}
                  />
                }
              >
                <HugeiconsIcon icon={context.icon} data-icon="inline-start" />
                <span className="min-w-0 truncate">{contextLabel}</span>
                <HugeiconsIcon
                  icon={ArrowDown01Icon}
                  data-icon="inline-end"
                  className="shrink-0"
                />
              </TooltipTrigger>
              <TooltipContent side="bottom">{context.action}</TooltipContent>
            </Tooltip>
            {sharedStateActive && (
              <Badge variant={stateError ? "destructive" : "secondary"}>
                {stateError ? "异常" : dashboard ? "正常" : "读取中"}
              </Badge>
            )}
            <div className="ml-auto flex items-center gap-2">
              <ThemeMenu />
              <Tooltip>
                <TooltipTrigger
                  render={
                    <Button
                      variant="outline"
                      size="icon-sm"
                      aria-label="刷新当前页面"
                      disabled={stateLoading && sharedStateActive}
                      onClick={refresh}
                    />
                  }
                >
                  {stateLoading && sharedStateActive ? (
                    <Spinner />
                  ) : (
                    <HugeiconsIcon icon={Refresh01Icon} />
                  )}
                </TooltipTrigger>
                <TooltipContent side="bottom">刷新</TooltipContent>
              </Tooltip>
              <Button
                size="sm"
                disabled={launching}
                onClick={() => void launch()}
              >
                {launching ? (
                  <Spinner data-icon="inline-start" />
                ) : (
                  <HugeiconsIcon icon={Rocket01Icon} data-icon="inline-start" />
                )}
                启动 Codex
              </Button>
            </div>
          </header>

          <main
            ref={mainRef}
            className="min-h-0 flex-1 overflow-y-auto overscroll-contain"
          >
            {stateError &&
              ((page === "dashboard" && !dashboard) ||
                ((page === "providers" || page === "usage") &&
                  !connections)) && (
                <div className="px-3 pt-1">
                  <Alert variant="destructive">
                    <HugeiconsIcon icon={InformationCircleIcon} />
                    <AlertTitle>无法读取应用状态</AlertTitle>
                    <AlertDescription>{stateError}</AlertDescription>
                  </Alert>
                </div>
              )}
            <Suspense fallback={<PageLoading />}>
              <ErrorBoundary
                key={page}
                label={`页面「${contextLabel}」渲染出错`}
              >
                {page === "dashboard" && (!stateError || dashboard) && (
                  <DashboardPage
                    dashboard={dashboard}
                    refreshRevision={refreshRevision}
                  />
                )}
                {page === "providers" && (!stateError || connections) && (
                  <ProvidersPage
                    {...sharedProps}
                    connections={connections}
                    selectedId={selectedConnection}
                    onSelectedIdChange={setSelectedConnection}
                  />
                )}
                {page === "usage" &&
                  (connections ? (
                    <UsagePage
                      {...sharedProps}
                      days={usageDays}
                      groupBy={usageGroupBy}
                      providers={connections.providers}
                    />
                  ) : stateError ? null : (
                    <PageLoading />
                  ))}
                {page === "sessions" && (
                  <SessionsPage
                    key={sessionQuery}
                    {...sharedProps}
                    query={sessionQuery}
                  />
                )}
                {page === "settings" && (
                  <SettingsPage {...sharedProps} section={settingsSection} />
                )}
              </ErrorBoundary>
            </Suspense>
          </main>
        </div>
      </div>

      {contextMounted && (
        <Suspense fallback={null}>
          <ErrorBoundary label="上下文面板渲染出错">
            <MemoizedContextSheet
              open={contextOpen}
              onOpenChange={setContextOpen}
              page={page}
              connections={connections}
              selectedConnection={selectedConnection}
              onSelectedConnectionChange={setSelectedConnection}
              usageDays={usageDays}
              onUsageDaysChange={setUsageDays}
              usageGroupBy={usageGroupBy}
              onUsageGroupByChange={setUsageGroupBy}
              sessionQueryDraft={sessionQueryDraft}
              onSessionQueryDraftChange={setSessionQueryDraft}
              onSessionQuerySubmit={setSessionQuery}
              settingsSection={settingsSection}
              onSettingsSectionChange={setSettingsSection}
              onChanged={refresh}
            />
          </ErrorBoundary>
        </Suspense>
      )}
      <Toaster timeout={4500} limit={3} />
    </TooltipProvider>
  )
}

function PageLoading() {
  return (
    <div
      className="flex h-full min-h-64 items-center justify-center gap-2 text-sm text-muted-foreground"
      role="status"
    >
      <Spinner />
      <span>正在加载页面</span>
    </div>
  )
}

function NavButton({
  active,
  icon,
  label,
  onClick,
}: {
  active: boolean
  icon: IconSvgElement
  label: string
  onClick: () => void
}) {
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Button
            variant={active ? "secondary" : "outline"}
            size="icon-sm"
            aria-current={active ? "page" : undefined}
            aria-label={label}
            className={
              active ? "shrink-0 ring-1 ring-foreground/10" : "shrink-0"
            }
            onClick={onClick}
          />
        }
      >
        <HugeiconsIcon icon={icon} />
      </TooltipTrigger>
      <TooltipContent side="right">{label}</TooltipContent>
    </Tooltip>
  )
}

function ThemeMenu() {
  const { theme, setTheme } = useTheme()
  const mounted = typeof window !== "undefined"

  const selectedTheme: ThemeOptionId =
    mounted && isThemeOptionId(theme) ? theme : "system"
  const currentTheme =
    themeOptions.find((option) => option.id === selectedTheme) ??
    themeOptions[0]
  const currentLabel = mounted ? currentTheme.label : "跟随系统"
  const currentIcon = mounted ? currentTheme.icon : ComputerIcon

  return (
    <DropdownMenu>
      <Tooltip>
        <TooltipTrigger
          render={
            <DropdownMenuTrigger
              render={
                <Button
                  variant="outline"
                  size="icon-sm"
                  aria-label={`界面主题：${currentLabel}`}
                />
              }
            />
          }
        >
          <HugeiconsIcon icon={currentIcon} />
        </TooltipTrigger>
        <TooltipContent side="bottom">界面主题：{currentLabel}</TooltipContent>
      </Tooltip>
      <DropdownMenuContent align="end" className="w-40">
        <DropdownMenuGroup>
          <DropdownMenuLabel>界面主题 · 当前：{currentLabel}</DropdownMenuLabel>
        </DropdownMenuGroup>
        <DropdownMenuRadioGroup
          value={selectedTheme}
          onValueChange={(value) => {
            if (isThemeOptionId(value)) setTheme(value)
          }}
        >
          {themeOptions.map((option) => (
            <DropdownMenuRadioItem
              key={option.id}
              value={option.id}
              closeOnClick
            >
              <HugeiconsIcon icon={option.icon} />
              <span>{option.label}</span>
            </DropdownMenuRadioItem>
          ))}
        </DropdownMenuRadioGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

function ContextSheet({
  open,
  onOpenChange,
  page,
  connections,
  selectedConnection,
  onSelectedConnectionChange,
  usageDays,
  onUsageDaysChange,
  usageGroupBy,
  onUsageGroupByChange,
  sessionQueryDraft,
  onSessionQueryDraftChange,
  onSessionQuerySubmit,
  settingsSection,
  onSettingsSectionChange,
  onChanged,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  page: Page
  connections?: ProviderOverview
  selectedConnection?: string
  onSelectedConnectionChange: (id: string) => void
  usageDays: number
  onUsageDaysChange: (days: number) => void
  usageGroupBy: UsageGroupBy
  onUsageGroupByChange: (groupBy: UsageGroupBy) => void
  sessionQueryDraft: string
  onSessionQueryDraftChange: (query: string) => void
  onSessionQuerySubmit: (query: string) => void
  settingsSection: SettingsSection
  onSettingsSectionChange: (section: SettingsSection) => void
  onChanged: () => void
}) {
  const applySessionQuery = () => {
    onSessionQuerySubmit(sessionQueryDraft.trim())
    onOpenChange(false)
  }

  if (usesConnectionManager(page)) {
    return (
      <ConnectionManagerSheet
        open={open}
        onOpenChange={onOpenChange}
        page={page}
        connections={connections}
        selectedId={selectedConnection}
        onSelectedIdChange={onSelectedConnectionChange}
        onChanged={onChanged}
      />
    )
  }

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="left">
        <SheetHeader>
          <SheetTitle>
            {page === "usage"
              ? "用量筛选"
              : page === "sessions"
                ? "搜索会话"
                : "设置章节"}
          </SheetTitle>
        </SheetHeader>

        <SheetBody className="gap-3">
          {page === "usage" && (
            <FieldGroup>
              <Field>
                <FieldLabel>时间范围</FieldLabel>
                <ToggleGroup
                  variant="outline"
                  spacing={0}
                  value={[String(usageDays)]}
                  onValueChange={(value) => {
                    if (value[0]) onUsageDaysChange(Number(value[0]))
                  }}
                >
                  <ToggleGroupItem value="1">今天</ToggleGroupItem>
                  <ToggleGroupItem value="7">7 天</ToggleGroupItem>
                  <ToggleGroupItem value="30">30 天</ToggleGroupItem>
                </ToggleGroup>
              </Field>
              <Field>
                <FieldLabel>汇总方式</FieldLabel>
                <ToggleGroup
                  variant="outline"
                  spacing={0}
                  value={[usageGroupBy]}
                  onValueChange={(value) => {
                    if (value[0]) onUsageGroupByChange(value[0] as UsageGroupBy)
                  }}
                >
                  <ToggleGroupItem value="model">按模型</ToggleGroupItem>
                  <ToggleGroupItem value="account">按账号</ToggleGroupItem>
                </ToggleGroup>
              </Field>
            </FieldGroup>
          )}

          {page === "sessions" && (
            <form
              onSubmit={(event) => {
                event.preventDefault()
                applySessionQuery()
              }}
            >
              <FieldGroup>
                <Field>
                  <FieldLabel htmlFor="session-search">搜索</FieldLabel>
                  <InputGroup>
                    <InputGroupAddon>
                      <HugeiconsIcon icon={Search01Icon} />
                    </InputGroupAddon>
                    <InputGroupInput
                      id="session-search"
                      value={sessionQueryDraft}
                      placeholder="标题或项目路径"
                      onChange={(event) =>
                        onSessionQueryDraftChange(event.target.value)
                      }
                    />
                  </InputGroup>
                </Field>
                <Button type="submit">应用搜索</Button>
              </FieldGroup>
            </form>
          )}

          {page === "settings" && (
            <FieldGroup>
              <Field>
                <FieldLabel>设置章节</FieldLabel>
                <ToggleGroup
                  orientation="vertical"
                  variant="outline"
                  className="w-full"
                  value={[settingsSection]}
                  onValueChange={(value) => {
                    if (!value[0]) return
                    onSettingsSectionChange(value[0] as SettingsSection)
                    onOpenChange(false)
                  }}
                >
                  {(
                    Object.entries(settingsSectionLabels) as Array<
                      [SettingsSection, string]
                    >
                  ).map(([value, label]) => (
                    <ToggleGroupItem key={value} value={value}>
                      {label}
                    </ToggleGroupItem>
                  ))}
                </ToggleGroup>
              </Field>
            </FieldGroup>
          )}
        </SheetBody>
      </SheetContent>
    </Sheet>
  )
}

const MemoizedContextSheet = memo(ContextSheet)
