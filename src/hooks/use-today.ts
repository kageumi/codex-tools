import { useEffect, useState } from "react"

const CHECK_INTERVAL_MS = 60_000

export function useToday() {
  const [today, setToday] = useState(() => new Date())

  useEffect(() => {
    const timer = window.setInterval(() => {
      setToday((current) => {
        const next = new Date()
        return next.toDateString() === current.toDateString() ? current : next
      })
    }, CHECK_INTERVAL_MS)
    return () => window.clearInterval(timer)
  }, [])

  return today
}
