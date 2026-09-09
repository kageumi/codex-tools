import type { OfficialAccountView } from "@/types"

export function resetCreditCountText(account: OfficialAccountView) {
  const count = account.quota.resetCredits?.availableCount
  return count == null ? "重置卡未知" : `重置卡 ${count} 张`
}
