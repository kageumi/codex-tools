import { useMemo, useState } from "react"

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
  FieldContent,
  FieldDescription,
  FieldGroup,
  FieldLabel,
} from "@/components/ui/field"
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
import { Switch } from "@/components/ui/switch"
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group"
import { toast } from "@/components/ui/toast"
import { errorMessage } from "@/lib/format"
import { call } from "@/lib/ipc"
import type { BillingMode, PricingRule, Provider, UsageRange } from "@/types"

import {
  findEquivalentPricingRule,
  pricingScopeForSource,
  pricingSourceFromValue,
  pricingSourceOptions,
  pricingSourceValueForRule,
} from "./pricing"

export function PricingEditor({
  open,
  range,
  modelOptions,
  providers,
  rules,
  editingRule,
  onOpenChange,
  onSaved,
}: {
  open: boolean
  range: UsageRange
  modelOptions: string[]
  providers: Provider[]
  rules: PricingRule[]
  editingRule?: PricingRule
  onOpenChange: (open: boolean) => void
  onSaved: () => void
}) {
  const [pattern, setPattern] = useState(editingRule?.modelPattern ?? "")
  const [billingMode, setBillingMode] = useState<
    Extract<BillingMode, "token" | "unpriced">
  >(editingRule?.billingMode === "token" ? "token" : "unpriced")
  const [input, setInput] = useState(editingRule?.inputUsdPerMillion ?? "2.50")
  const [cachedRead, setCachedRead] = useState(
    editingRule?.cachedReadUsdPerMillion ?? "0.25"
  )
  const [cacheWrite, setCacheWrite] = useState(
    editingRule?.cacheWriteUsdPerMillion ?? "3.00"
  )
  const [output, setOutput] = useState(
    editingRule?.outputUsdPerMillion ?? "10.00"
  )
  const [cacheWriteIncluded, setCacheWriteIncluded] = useState(
    editingRule?.cacheWriteIncludedInInput ?? true
  )
  const [sourceValue, setSourceValue] = useState(
    editingRule ? pricingSourceValueForRule(editingRule) : ""
  )
  const [busy, setBusy] = useState(false)

  const normalizedPattern = pattern.trim()
  const scope = pricingScopeForSource(pricingSourceFromValue(sourceValue))
  const sourceOptions = useMemo(
    () => pricingSourceOptions(providers),
    [providers]
  )

  const save = async () => {
    if (!scope || !normalizedPattern) return
    setBusy(true)
    const now = Date.now()
    const existing = findEquivalentPricingRule(rules, {
      ...scope,
      modelPattern: normalizedPattern,
      matchKind: "exact",
    })
    const rule: PricingRule = {
      id: editingRule?.id ?? existing?.id ?? "",
      version: editingRule?.version ?? existing?.version ?? 1,
      active: true,
      ...scope,
      modelPattern: normalizedPattern,
      matchKind: "exact",
      billingMode,
      inputUsdPerMillion: billingMode === "token" ? input : undefined,
      cachedReadUsdPerMillion: billingMode === "token" ? cachedRead : undefined,
      cacheWriteUsdPerMillion: billingMode === "token" ? cacheWrite : undefined,
      outputUsdPerMillion: billingMode === "token" ? output : undefined,
      cacheWriteIncludedInInput: cacheWriteIncluded,
      effectiveFromMs: range.startAtMs,
      createdAtMs: now,
      updatedAtMs: now,
    }
    try {
      await call("usage_save_pricing_rule", { input: rule })
      try {
        await call("usage_reprice", { range })
        toast.add({ title: "价格规则已保存", type: "success" })
      } catch (reason) {
        toast.add({
          title: "价格规则已保存，但重新计价失败",
          description: errorMessage(reason),
          type: "warning",
        })
      }
      onSaved()
    } catch (reason) {
      toast.add({
        title: "保存失败",
        description: errorMessage(reason),
        type: "error",
      })
    } finally {
      setBusy(false)
    }
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(nextOpen) => {
        if (!nextOpen && busy) return
        onOpenChange(nextOpen)
      }}
    >
      <DialogContent showCloseButton={!busy} aria-busy={busy}>
        <DialogHeader>
          <DialogTitle>
            {editingRule ? "编辑价格规则" : "添加价格规则"}
          </DialogTitle>
        </DialogHeader>
        <DialogBody>
          <FieldGroup>
            <Field>
              <FieldLabel htmlFor="price-source">来源</FieldLabel>
              <Select
                items={sourceOptions}
                value={sourceValue}
                onValueChange={(value) => setSourceValue(value ?? "")}
              >
                <SelectTrigger id="price-source" className="w-full">
                  <SelectValue placeholder="选择适用的第三方 API" />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    {sourceOptions.map((option) => (
                      <SelectItem key={option.value} value={option.value}>
                        {option.label}
                      </SelectItem>
                    ))}
                  </SelectGroup>
                </SelectContent>
              </Select>
            </Field>
            <Field>
              <FieldLabel htmlFor="price-model">模型</FieldLabel>
              {modelOptions.length > 0 && (
                <Select
                  value={modelOptions.includes(pattern) ? pattern : ""}
                  onValueChange={(value) => {
                    if (value) setPattern(value)
                  }}
                >
                  <SelectTrigger
                    className="w-full"
                    aria-label="从已使用模型中选择"
                  >
                    <SelectValue placeholder="从已使用模型中选择" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectGroup>
                      {modelOptions.map((model) => (
                        <SelectItem key={model} value={model}>
                          {model}
                        </SelectItem>
                      ))}
                    </SelectGroup>
                  </SelectContent>
                </Select>
              )}
              <Input
                id="price-model"
                value={pattern}
                placeholder="也可以手动输入，例如 gpt-5.6"
                onChange={(e) => setPattern(e.target.value)}
              />
            </Field>
            <Field>
              <FieldLabel>计费方式</FieldLabel>
              <ToggleGroup
                variant="outline"
                spacing={0}
                className="w-full"
                value={[billingMode]}
                onValueChange={(value) => {
                  if (value[0] === "token" || value[0] === "unpriced") {
                    setBillingMode(value[0])
                  }
                }}
              >
                <ToggleGroupItem className="flex-1" value="token">
                  按 Token 计价
                </ToggleGroupItem>
                <ToggleGroupItem className="flex-1" value="unpriced">
                  不计价
                </ToggleGroupItem>
              </ToggleGroup>
            </Field>
            {billingMode === "token" && (
              <FieldGroup className="grid grid-cols-2 gap-3">
                <Field>
                  <FieldLabel htmlFor="price-input">普通输入</FieldLabel>
                  <Input
                    id="price-input"
                    inputMode="decimal"
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                  />
                </Field>
                <Field>
                  <FieldLabel htmlFor="price-cached-read">缓存读取</FieldLabel>
                  <Input
                    id="price-cached-read"
                    inputMode="decimal"
                    value={cachedRead}
                    onChange={(e) => setCachedRead(e.target.value)}
                  />
                </Field>
                <Field>
                  <FieldLabel htmlFor="price-cache-write">缓存写入</FieldLabel>
                  <Input
                    id="price-cache-write"
                    inputMode="decimal"
                    value={cacheWrite}
                    onChange={(e) => setCacheWrite(e.target.value)}
                  />
                </Field>
                <Field>
                  <FieldLabel htmlFor="price-output">输出</FieldLabel>
                  <Input
                    id="price-output"
                    inputMode="decimal"
                    value={output}
                    onChange={(e) => setOutput(e.target.value)}
                  />
                </Field>
              </FieldGroup>
            )}
            {billingMode === "token" && (
              <Field orientation="horizontal">
                <FieldContent>
                  <FieldLabel htmlFor="price-cache-write-included">
                    输入总量包含缓存写入
                  </FieldLabel>
                  <FieldDescription>
                    开启后，普通输入计费会扣除缓存读取和缓存写入 Token。
                  </FieldDescription>
                </FieldContent>
                <Switch
                  id="price-cache-write-included"
                  checked={cacheWriteIncluded}
                  onCheckedChange={setCacheWriteIncluded}
                />
              </Field>
            )}
          </FieldGroup>
        </DialogBody>
        <DialogFooter>
          <Button
            variant="outline"
            disabled={busy}
            onClick={() => onOpenChange(false)}
          >
            取消
          </Button>
          <Button
            disabled={busy || !normalizedPattern || !scope}
            onClick={() => void save()}
          >
            {busy && <Spinner data-icon="inline-start" />}保存
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
