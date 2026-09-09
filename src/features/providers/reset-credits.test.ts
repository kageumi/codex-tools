import { describe, expect, it } from "vitest"

import type { OfficialAccountView } from "@/types"

import { resetCreditCountText } from "./reset-credits"

const account = (availableCount?: number): OfficialAccountView => ({
  id: "account-1",
  name: "账号",
  remark: "",
  accountId: "workspace-1",
  email: "",
  source: "open_ai_oauth",
  expiresAt: null,
  credentialRefresh: { status: "unknown" },
  quota: {
    status: "success",
    resetCredits: {
      availableCount,
      detailsStatus: availableCount === undefined ? "unknown" : "complete",
    },
  },
  active: false,
  createdAt: 0,
  updatedAt: 0,
})

describe("resetCreditCountText", () => {
  it("does not turn a missing server count into zero", () => {
    expect(resetCreditCountText(account())).toBe("重置卡未知")
  })

  it("uses the server-authoritative count including zero", () => {
    expect(resetCreditCountText(account(0))).toBe("重置卡 0 张")
    expect(resetCreditCountText(account(3))).toBe("重置卡 3 张")
  })
})
