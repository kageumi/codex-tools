import { Component, type ErrorInfo, type ReactNode } from "react"
import { InformationCircleIcon } from "@hugeicons/core-free-icons"
import { HugeiconsIcon } from "@hugeicons/react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { isLazyChunkLoadError } from "@/components/error-boundary-utils"

type ErrorBoundaryProps = {
  children: ReactNode
  label?: string
}

type ErrorBoundaryState = {
  error?: Error
}

export class ErrorBoundary extends Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  state: ErrorBoundaryState = {}

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("React render error:", error, info.componentStack)
  }

  private reset = () => {
    this.setState({ error: undefined })
  }

  render() {
    if (this.state.error) {
      const chunkLoadFailed = isLazyChunkLoadError(this.state.error)
      return (
        <Alert variant="destructive" className="m-3">
          <HugeiconsIcon icon={InformationCircleIcon} />
          <AlertTitle>
            {chunkLoadFailed
              ? "页面资源加载失败"
              : (this.props.label ?? "页面渲染出错")}
          </AlertTitle>
          <AlertDescription>
            <p className="mb-3 break-words">
              {chunkLoadFailed
                ? "应用资源可能已更新，请重新加载后重试。"
                : this.state.error.message}
            </p>
            <Button
              type="button"
              size="sm"
              variant="outline"
              onClick={
                chunkLoadFailed ? () => window.location.reload() : this.reset
              }
            >
              {chunkLoadFailed ? "重新加载" : "重试"}
            </Button>
          </AlertDescription>
        </Alert>
      )
    }
    return this.props.children
  }
}
