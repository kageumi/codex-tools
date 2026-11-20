import { describe, expect, it } from "vitest"

import type { OfficialAccountView } from "@/types"

import { resetCreditCountText, resetCreditFailure } from "./reset-credits"

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

it("仅在后端确认未提交时允许用户重试", () => {
  const failure = (stage: string) =>
    new Error("IPC", { cause: { stage, message: "网络中断" } })
  expect(resetCreditFailure(failure("not_submitted"))).toEqual({
    unknown: false,
    message: "未提交使用请求。网络中断",
  })
  expect(resetCreditFailure(failure("unknown"))).toEqual({
    unknown: true,
    message: "网络中断 结果可能未知，未自动重试。",
  })
  expect(resetCreditFailure(new Error("IPC disconnected")).unknown).toBe(true)
})
