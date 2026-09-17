import { useEffect, useRef, useState } from "react"
import { createPortal } from "react-dom"
import { ClipboardPasteIcon, Copy01Icon } from "@hugeicons/core-free-icons"
import { HugeiconsIcon } from "@hugeicons/react"

import { floatingSurfaceStyles } from "@/components/ui/overlay-styles"
import { toast } from "@/components/ui/toast"
import { readClipboardText, writeClipboardText } from "@/lib/clipboard"
import {
  clampMenuPosition,
  editableMenuAvailability,
  editableTargetKind,
  replaceTextSelection,
  selectedTextFromRange,
} from "@/lib/editable-context-menu"
import { cn } from "@/lib/utils"

const MENU_WIDTH = 160
const MENU_HEIGHT = 88

type TextControlSnapshot = {
  kind: "input" | "textarea"
  element: HTMLInputElement | HTMLTextAreaElement
  start: number
  end: number
  selectedText: string
  readOnly: boolean
}

type ContentEditableSnapshot = {
  kind: "contenteditable"
  element: HTMLElement
  range: Range
  selectedText: string
}

type EditableSnapshot = TextControlSnapshot | ContentEditableSnapshot

type MenuState = {
  x: number
  y: number
  snapshot: EditableSnapshot
}

function snapshotEditableTarget(target: EventTarget | null) {
  if (!(target instanceof Element)) return null
  const element = target.closest("input, textarea, [contenteditable]")
  if (!(element instanceof HTMLElement)) return null

  const kind = editableTargetKind({
    tagName: element.tagName,
    inputType: element instanceof HTMLInputElement ? element.type : undefined,
    isContentEditable: element.isContentEditable,
    disabled:
      element instanceof HTMLInputElement ||
      element instanceof HTMLTextAreaElement
        ? element.disabled
        : false,
  })
  if (!kind) return null

  if (
    element instanceof HTMLInputElement ||
    element instanceof HTMLTextAreaElement
  ) {
    const start = element.selectionStart ?? element.value.length
    const end = element.selectionEnd ?? start
    return {
      kind: element instanceof HTMLInputElement ? "input" : "textarea",
      element,
      start,
      end,
      selectedText: selectedTextFromRange(element.value, start, end),
      readOnly: element.readOnly,
    } satisfies TextControlSnapshot
  }

  if (!element.isContentEditable) return null
  const selection = window.getSelection()
  const hasSelectionRange = Boolean(
    selection &&
    selection.rangeCount > 0 &&
    element.contains(selection.getRangeAt(0).commonAncestorContainer)
  )
  const range = hasSelectionRange
    ? selection!.getRangeAt(0).cloneRange()
    : document.createRange()
  if (!hasSelectionRange) {
    range.selectNodeContents(element)
    range.collapse(false)
  }
  return {
    kind: "contenteditable",
    element,
    range,
    selectedText: range.toString(),
  } satisfies ContentEditableSnapshot
}

function dispatchEditEvents(element: HTMLElement, insertedText: string) {
  element.dispatchEvent(
    new InputEvent("input", {
      bubbles: true,
      inputType: "insertFromPaste",
      data: insertedText,
    })
  )
  element.dispatchEvent(new Event("change", { bubbles: true }))
}

function pasteIntoTextControl(
  snapshot: TextControlSnapshot,
  insertion: string
) {
  const { element, start, end } = snapshot
  const next = replaceTextSelection(element.value, start, end, insertion)
  const prototype =
    element instanceof HTMLTextAreaElement
      ? HTMLTextAreaElement.prototype
      : HTMLInputElement.prototype
  Object.getOwnPropertyDescriptor(prototype, "value")?.set?.call(
    element,
    next.value
  )
  element.focus({ preventScroll: true })
  try {
    element.setSelectionRange(next.caret, next.caret)
  } catch {
    // Some text-like input types do not expose a selectable range.
  }
  dispatchEditEvents(element, insertion)
}

function pasteIntoContentEditable(
  snapshot: ContentEditableSnapshot,
  insertion: string
) {
  const { element, range } = snapshot
  element.focus({ preventScroll: true })
  range.deleteContents()
  const textNode = document.createTextNode(insertion)
  range.insertNode(textNode)
  range.setStartAfter(textNode)
  range.collapse(true)
  const selection = window.getSelection()
  selection?.removeAllRanges()
  selection?.addRange(range)
  dispatchEditEvents(element, insertion)
}

export function EditableContextMenu() {
  const [menu, setMenu] = useState<MenuState | null>(null)
  const menuRef = useRef<HTMLDivElement>(null)
  const copyRef = useRef<HTMLButtonElement>(null)
  const pasteRef = useRef<HTMLButtonElement>(null)

  useEffect(() => {
    const openMenu = (event: MouseEvent) => {
      event.preventDefault()
      const snapshot = snapshotEditableTarget(event.target)
      if (!snapshot) {
        setMenu(null)
        return
      }
      const position = clampMenuPosition({
        x: event.clientX,
        y: event.clientY,
        menuWidth: MENU_WIDTH,
        menuHeight: MENU_HEIGHT,
        viewportWidth: window.innerWidth,
        viewportHeight: window.innerHeight,
      })
      setMenu({ ...position, snapshot })
    }
    document.addEventListener("contextmenu", openMenu)
    return () => document.removeEventListener("contextmenu", openMenu)
  }, [])

  useEffect(() => {
    if (!menu) return
    const closeMenu = (event: Event) => {
      if (
        event.type === "pointerdown" &&
        event.target instanceof Node &&
        menuRef.current?.contains(event.target)
      ) {
        return
      }
      setMenu(null)
    }
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setMenu(null)
    }

    document.addEventListener("pointerdown", closeMenu, true)
    document.addEventListener("keydown", closeOnEscape)
    window.addEventListener("blur", closeMenu)
    window.addEventListener("resize", closeMenu)
    window.addEventListener("scroll", closeMenu, true)
    return () => {
      document.removeEventListener("pointerdown", closeMenu, true)
      document.removeEventListener("keydown", closeOnEscape)
      window.removeEventListener("blur", closeMenu)
      window.removeEventListener("resize", closeMenu)
      window.removeEventListener("scroll", closeMenu, true)
    }
  }, [menu])

  useEffect(() => {
    if (!menu) return
    const firstEnabled = menu.snapshot.selectedText
      ? copyRef.current
      : menu.snapshot.kind === "contenteditable" || !menu.snapshot.readOnly
        ? pasteRef.current
        : null
    ;(firstEnabled ?? menuRef.current)?.focus()
  }, [menu])

  if (!menu) return null

  const { canCopy, canPaste } = editableMenuAvailability({
    selectedText: menu.snapshot.selectedText,
    readOnly:
      menu.snapshot.kind === "contenteditable" ? false : menu.snapshot.readOnly,
  })

  const copy = async () => {
    if (!canCopy) return
    try {
      await writeClipboardText(menu.snapshot.selectedText)
      setMenu(null)
    } catch {
      toast.add({ title: "无法复制到剪贴板", type: "error" })
    }
  }

  const paste = async () => {
    if (!canPaste) return
    try {
      const insertion = await readClipboardText()
      if (menu.snapshot.kind === "contenteditable") {
        pasteIntoContentEditable(menu.snapshot, insertion)
      } else {
        pasteIntoTextControl(menu.snapshot, insertion)
      }
      setMenu(null)
    } catch {
      toast.add({ title: "无法读取剪贴板", type: "error" })
    }
  }

  const focusOtherItem = (event: React.KeyboardEvent) => {
    if (event.key !== "ArrowDown" && event.key !== "ArrowUp") return
    event.preventDefault()
    const enabledItems = [copyRef.current, pasteRef.current].filter(
      (item): item is HTMLButtonElement => Boolean(item && !item.disabled)
    )
    if (enabledItems.length === 0) return
    const currentIndex = enabledItems.indexOf(
      document.activeElement as HTMLButtonElement
    )
    const delta = event.key === "ArrowDown" ? 1 : -1
    enabledItems[
      (currentIndex + delta + enabledItems.length) % enabledItems.length
    ]?.focus()
  }

  const itemStyles =
    "flex w-full items-center gap-2.5 rounded-xl px-3 py-2 text-left text-sm outline-none select-none transition-colors duration-150 ease-[var(--motion-ease-out)] hover:bg-accent focus-visible:bg-accent focus-visible:text-accent-foreground disabled:pointer-events-none disabled:opacity-50"

  return createPortal(
    <div
      ref={menuRef}
      role="menu"
      aria-label="编辑菜单"
      tabIndex={-1}
      className={cn(
        "fixed z-50 w-40 rounded-2xl p-1 outline-none",
        floatingSurfaceStyles
      )}
      style={{ left: menu.x, top: menu.y }}
      onKeyDown={focusOtherItem}
      onContextMenu={(event) => event.preventDefault()}
    >
      <button
        ref={copyRef}
        type="button"
        role="menuitem"
        className={itemStyles}
        disabled={!canCopy}
        onPointerDown={(event) => event.preventDefault()}
        onClick={() => void copy()}
      >
        <HugeiconsIcon icon={Copy01Icon} className="size-4" />
        复制
      </button>
      <button
        ref={pasteRef}
        type="button"
        role="menuitem"
        className={itemStyles}
        disabled={!canPaste}
        onPointerDown={(event) => event.preventDefault()}
        onClick={() => void paste()}
      >
        <HugeiconsIcon icon={ClipboardPasteIcon} className="size-4" />
        粘贴
      </button>
    </div>,
    document.body
  )
}
