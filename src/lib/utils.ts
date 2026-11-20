import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function sameHttpOrigin(first: string, second: string): boolean {
  try {
    const url = new URL(first)
    return (
      (url.protocol === "http:" || url.protocol === "https:") &&
      url.origin === new URL(second).origin
    )
  } catch {
    return false
  }
}
