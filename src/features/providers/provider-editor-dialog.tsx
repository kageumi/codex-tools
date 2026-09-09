import { useState } from "react"

import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import {
  Dialog,
  DialogBody,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Spinner } from "@/components/ui/spinner"
import { HugeiconsIcon } from "@hugeicons/react"
import { Add01Icon, Delete02Icon } from "@hugeicons/core-free-icons"
import { toast } from "@/components/ui/toast"
import { errorMessage } from "@/lib/format"
import { call } from "@/lib/ipc"
import type { Provider } from "@/types"

import {
  addCustomModelTo,
  allModelsSelected,
  effectiveModelsOf,
  contextWindowOverrideInputOf,
  contextWindowOverrideEditorSessionOf,
  isValidContextWindowOverride,
  providerSaveInputOf,
  removeCustomModelFrom,
  toggleModelSelected,
} from "./connection-utils"

// 纯数据复制函数由多个入口共享，不参与组件的 Fast Refresh 状态。
// eslint-disable-next-line react-refresh/only-export-components
export function cloneProviderForEditing(provider: Provider): Provider {
  return {
    ...provider,
    headers: { ...provider.headers },
    modelContextWindows: provider.modelContextWindows
      ? { ...provider.modelContextWindows }
      : undefined,
    availableModels: provider.availableModels
      ? [...provider.availableModels]
      : undefined,
    customModels: provider.customModels
      ? [...provider.customModels]
      : undefined,
    selectedModels: provider.selectedModels
      ? [...provider.selectedModels]
      : undefined,
    modelsDevMeta: provider.modelsDevMeta
      ? Object.fromEntries(
          Object.entries(provider.modelsDevMeta).map(([model, metadata]) => [
            model,
            { ...metadata },
          ])
        )
      : undefined,
  }
}

type ProviderEditorDialogProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  provider: Provider
  onProviderChange: (
    provider: Provider | ((prev: Provider) => Provider)
  ) => void
  onSaved: (provider: Provider) => void
}

export function ProviderEditorDialog(props: ProviderEditorDialogProps) {
  return (
    <ProviderEditorDialogContent
      key={contextWindowOverrideEditorSessionOf(props.provider, props.open)}
      {...props}
    />
  )
}

function ProviderEditorDialogContent({
  open,
  onOpenChange,
  provider,
  onProviderChange,
  onSaved,
}: ProviderEditorDialogProps) {
  const [saving, setSaving] = useState(false)
  const [modelSearch, setModelSearch] = useState("")
  const [customModelDraft, setCustomModelDraft] = useState("")
  const [contextWindowOverrideDraft, setContextWindowOverrideDraft] = useState(
    () => provider.contextWindowOverride?.toString() ?? ""
  )

  // 基于最新 prev 一次合并多个字段，避免渲染快照闭包与 React 批处理下的覆盖。
  const patch = (partial: Partial<Provider>) =>
    onProviderChange((prev) => ({ ...prev, ...partial }))

  const save = async () => {
    if (!isValidContextWindowOverride(contextWindowOverrideDraft)) {
      toast.add({
        title: "上下文窗口无效",
        description: "请输入大于 0 的整数 token。",
        type: "error",
      })
      return
    }
    setSaving(true)
    try {
      const saved = await call("connections_save_provider", {
        provider: {
          ...providerSaveInputOf(provider),
          contextWindowOverride: contextWindowOverrideInputOf(
            contextWindowOverrideDraft
          ),
        },
      })
      toast.add({ title: "API 服务已保存", type: "success" })
      onSaved(saved)
    } catch (reason) {
      toast.add({
        title: "保存失败",
        description: errorMessage(reason),
        type: "error",
      })
    } finally {
      setSaving(false)
    }
  }

  const customModels = provider.customModels ?? []
  // 有效模型 = /models 同步的模型 ∪ 用户手动添加的自定义模型（保序去重）。
  const effectiveModels = effectiveModelsOf(provider)
  const selectedSet = provider.selectedModels
    ? new Set(provider.selectedModels)
    : undefined
  const customSet = new Set(customModels)
  const allSelected =
    !selectedSet || effectiveModels.every((model) => selectedSet.has(model))
  const noneSelected = effectiveModels.length > 0 && selectedSet?.size === 0
  const normalizedSearch = modelSearch.trim().toLowerCase()
  const filteredModels = normalizedSearch
    ? effectiveModels.filter((model) =>
        model.toLowerCase().includes(normalizedSearch)
      )
    : effectiveModels

  const addCustomModel = () => {
    const trimmed = customModelDraft.trim()
    if (!trimmed || effectiveModels.includes(trimmed)) {
      setCustomModelDraft("")
      return
    }
    setCustomModelDraft("")
    // 一次合并 customModels + selectedModels，避免双 update 覆盖。
    onProviderChange((prev) => addCustomModelTo(prev, trimmed))
  }

  const removeCustomModel = (model: string) => {
    // 一次合并 customModels + selectedModels。
    onProviderChange((prev) => removeCustomModelFrom(prev, model))
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(nextOpen) => {
        if (!nextOpen && saving) return
        if (!nextOpen) setModelSearch("")
        onOpenChange(nextOpen)
      }}
    >
      <DialogContent showCloseButton={!saving} aria-busy={saving}>
        <DialogHeader>
          <DialogTitle>
            {provider.id ? "编辑 API 服务" : "添加 API 服务"}
          </DialogTitle>
          <DialogDescription>
            填写与 OpenAI 兼容的接口信息，模型列表将从 API 自动同步。
          </DialogDescription>
        </DialogHeader>
        <DialogBody>
          <FieldGroup>
            <Field data-disabled={saving}>
              <FieldLabel htmlFor="provider-name">名称</FieldLabel>
              <Input
                id="provider-name"
                disabled={saving}
                value={provider.name}
                onChange={(event) => patch({ name: event.target.value })}
              />
            </Field>
            <Field data-disabled={saving}>
              <FieldLabel htmlFor="provider-url">Base URL</FieldLabel>
              <Input
                id="provider-url"
                disabled={saving}
                value={provider.baseUrl}
                onChange={(event) => patch({ baseUrl: event.target.value })}
              />
            </Field>
            <Field data-disabled={saving}>
              <FieldLabel htmlFor="provider-key">API Key</FieldLabel>
              <Input
                id="provider-key"
                autoComplete="off"
                autoCapitalize="none"
                autoCorrect="off"
                data-1p-ignore
                data-lpignore="true"
                disabled={saving}
                spellCheck={false}
                type="password"
                value={provider.apiKey ?? ""}
                placeholder={
                  provider.hasApiKey ? "留空以保留现有密钥" : "sk-..."
                }
                onChange={(event) => patch({ apiKey: event.target.value })}
              />
            </Field>
            <Field data-disabled={saving}>
              <FieldLabel>接口类型</FieldLabel>
              <Select
                items={[
                  { label: "Responses API", value: "responses" },
                  { label: "Chat Completions", value: "chat" },
                ]}
                value={provider.apiType}
                disabled={saving}
                onValueChange={(value) =>
                  value && patch({ apiType: value as Provider["apiType"] })
                }
              >
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    <SelectItem value="responses">Responses API</SelectItem>
                    <SelectItem value="chat">Chat Completions</SelectItem>
                  </SelectGroup>
                </SelectContent>
              </Select>
            </Field>
            <Field data-disabled={saving}>
              <FieldLabel htmlFor="provider-context-window-override">
                自定义上下文窗口（token，可选）
              </FieldLabel>
              <Input
                id="provider-context-window-override"
                type="text"
                inputMode="numeric"
                placeholder="留空以自动匹配"
                value={contextWindowOverrideDraft}
                disabled={saving}
                onChange={(event) =>
                  setContextWindowOverrideDraft(event.target.value)
                }
              />
              <div className="text-xs text-muted-foreground">
                应用于该 API 的所有模型；留空则自动匹配上下文窗口。
              </div>
            </Field>
            <Field data-disabled={saving}>
              <FieldLabel>写入 Codex 的模型</FieldLabel>
              <label className="sr-only" htmlFor="provider-model-search">
                搜索模型
              </label>
              <Input
                id="provider-model-search"
                type="search"
                placeholder="搜索模型…"
                value={modelSearch}
                disabled={saving}
                onChange={(event) => setModelSearch(event.target.value)}
                className="mb-2"
              />
              <div className="mb-2 flex items-center gap-2">
                <label className="sr-only" htmlFor="provider-custom-model">
                  自定义模型 ID
                </label>
                <Input
                  id="provider-custom-model"
                  placeholder="添加自定义模型 ID…"
                  value={customModelDraft}
                  disabled={saving}
                  onChange={(event) => setCustomModelDraft(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") {
                      event.preventDefault()
                      addCustomModel()
                    }
                  }}
                />
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  disabled={saving || !customModelDraft.trim()}
                  onClick={addCustomModel}
                >
                  <HugeiconsIcon icon={Add01Icon} data-icon="inline-start" />
                  添加
                </Button>
              </div>
              <div className="max-h-48 space-y-1 overflow-y-auto rounded-xl border border-border/60 p-2">
                {/* 全选按钮放在模型列表容器内部顶部 */}
                {effectiveModels.length > 0 && (
                  <div className="mb-1 flex items-center justify-between px-1">
                    <span className="text-xs text-muted-foreground">
                      {effectiveModels.length} 个模型
                    </span>
                    <Button
                      type="button"
                      size="sm"
                      variant="ghost"
                      disabled={saving}
                      onClick={() =>
                        onProviderChange((prev) => ({
                          ...prev,
                          selectedModels: allModelsSelected(prev)
                            ? []
                            : undefined,
                        }))
                      }
                    >
                      {allSelected ? "取消全选" : "全选"}
                    </Button>
                  </div>
                )}
                {filteredModels.length === 0 ? (
                  <div className="px-2 py-3 text-center text-xs text-muted-foreground">
                    未找到匹配的模型
                  </div>
                ) : (
                  filteredModels.map((model) => {
                    const selected = selectedSet?.has(model) ?? true
                    const isCustom = customSet.has(model)
                    return (
                      <div
                        key={model}
                        className="group flex items-center gap-2 rounded-lg px-2 py-1.5 text-sm hover:bg-muted/60"
                      >
                        <label className="flex min-w-0 flex-1 items-center gap-2">
                          <Checkbox
                            checked={selected}
                            disabled={saving}
                            onCheckedChange={(checked) =>
                              // 函数式更新：基于最新 prev 计算，避免陈旧闭包。
                              onProviderChange((prev) => ({
                                ...prev,
                                selectedModels: toggleModelSelected(
                                  prev,
                                  model,
                                  Boolean(checked)
                                ),
                              }))
                            }
                          />
                          <span className="truncate">{model}</span>
                        </label>
                        {isCustom && (
                          <button
                            type="button"
                            aria-label={`移除自定义模型 ${model}`}
                            disabled={saving}
                            onClick={() => removeCustomModel(model)}
                            className="shrink-0 rounded-md p-1 text-muted-foreground opacity-0 transition-opacity group-hover:opacity-100 hover:bg-muted hover:text-foreground focus-visible:opacity-100 disabled:opacity-30"
                          >
                            <HugeiconsIcon icon={Delete02Icon} />
                          </button>
                        )}
                      </div>
                    )
                  })
                )}
              </div>
              <div className="text-xs text-muted-foreground">
                默认全部选中，取消选择后仅将选中模型写入 Codex。
              </div>
            </Field>
          </FieldGroup>
        </DialogBody>
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={saving}
            onClick={() => onOpenChange(false)}
          >
            取消
          </Button>
          <Button
            type="button"
            disabled={
              saving || !provider.name || !provider.baseUrl || noneSelected
            }
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
