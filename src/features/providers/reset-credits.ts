import { errorMessage } from "@/lib/format"
import type { OfficialAccountView } from "@/types"

export function resetCreditCountText(account: OfficialAccountView) {
  const count = account.quota.resetCredits?.availableCount
  return count == null ? "重置卡未知" : `重置卡 ${count} 张`
}

export function resetCreditFailure(reason: unknown) {
  const error = reason instanceof Error ? reason.cause : reason
  let message = errorMessage(reason)
  if (
    typeof error === "object" &&
    error !== null &&
    "stage" in error &&
    "message" in error &&
    typeof error.message === "string"
  ) {
    if (error.stage === "not_submitted")
      return { unknown: false, message: `未提交使用请求。${error.message}` }
    if (error.stage === "unknown") message = error.message
  }
  return {
    unknown: true,
    message: message.includes("结果未知")
      ? message
      : `${message} 结果可能未知，未自动重试。`,
  }
}
