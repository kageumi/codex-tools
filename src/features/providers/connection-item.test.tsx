import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"

import { ConnectionItem } from "./connection-item"

function markupFor({
  active,
  selected,
  unavailable = false,
  name = "账号名称",
}: {
  active: boolean
  selected: boolean
  unavailable?: boolean
  name?: string
}) {
  return renderToStaticMarkup(
    <ConnectionItem
      kind="account"
      id="account-1"
      name={name}
      description="账号说明"
      details={<div data-testid="details">邮箱和 5H/7D 额度详情</div>}
      active={active}
      canView
      selected={selected}
      unavailable={unavailable}
      unavailableLabel="凭据失效"
      frozen={false}
      onView={() => {}}
      onActivate={() => {}}
      onEdit={() => {}}
      onDelete={() => {}}
      moreActions={[]}
    />
  )
}

describe("ConnectionItem 状态与详情布局", () => {
  it.each([
    { name: "无状态徽章", active: false, selected: false, badges: [] },
    { name: "当前", active: true, selected: false, badges: ["当前"] },
    { name: "已选", active: false, selected: true, badges: ["已选"] },
    {
      name: "当前和已选",
      active: true,
      selected: true,
      badges: ["当前", "已选"],
    },
  ])(
    "$name 时标题和徽章在 header，详情保持独立全宽",
    ({ active, selected, badges }) => {
      const markup = markupFor({ active, selected })
      const headerStart = markup.indexOf('data-slot="item-header"')
      const contentStart = markup.indexOf('data-slot="item-content"')
      const footerStart = markup.indexOf('data-slot="item-footer"')
      const titleStart = markup.indexOf('data-slot="item-title"')
      const detailsStart = markup.indexOf('data-testid="details"')
      const headerMarkup = markup.slice(headerStart, contentStart)

      expect(headerStart).toBeGreaterThanOrEqual(0)
      expect(titleStart).toBeGreaterThan(headerStart)
      expect(titleStart).toBeLessThan(contentStart)
      expect(contentStart).toBeGreaterThan(headerStart)
      expect(markup).toMatch(
        /data-slot="item-header"[^>]*class="[^"]*\bmin-h-5\b/
      )
      expect(detailsStart).toBeGreaterThan(contentStart)
      expect(detailsStart).toBeLessThan(footerStart)
      expect(markup).toMatch(
        /data-slot="item-content"[^>]*class="[^"]*\bbasis-full\b/
      )
      for (const badge of badges) {
        expect(headerMarkup).toContain(badge)
      }
      expect(headerMarkup).not.toContain("凭据失效")
    }
  )

  it("失效徽章与长账号名不会把详情移回标题行", () => {
    const markup = markupFor({
      active: true,
      selected: true,
      unavailable: true,
      name: "这是一个用于回归测试的很长账号名称，状态徽章不应影响邮箱和额度详情宽度",
    })
    const contentStart = markup.indexOf('data-slot="item-content"')
    const footerStart = markup.indexOf('data-slot="item-footer"')
    const detailsStart = markup.indexOf('data-testid="details"')

    expect(markup.slice(0, contentStart)).toContain("当前")
    expect(markup.slice(0, contentStart)).toContain("已选")
    expect(markup.slice(0, contentStart)).toContain("凭据失效")
    expect(detailsStart).toBeGreaterThan(contentStart)
    expect(detailsStart).toBeLessThan(footerStart)
  })

  it("隐藏查看按钮时保留与 xs 按钮同高的占位", () => {
    const markup = markupFor({ active: false, selected: true })

    expect(markup).toMatch(
      /<span aria-hidden="true" class="[^"]*\bh-6\b[^"]*"><\/span>/
    )
    expect(markup).toMatch(/data-slot="button"[^>]*class="[^"]*\bh-6\b/)
  })
})
