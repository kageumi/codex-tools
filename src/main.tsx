import { createElement, lazy, StrictMode, Suspense } from "react"
import { createRoot } from "react-dom/client"
import { ThemeProvider } from "next-themes"

import "./index.css"
import { EditableContextMenu } from "./components/editable-context-menu.tsx"
import { ErrorBoundary } from "./components/error-boundary.tsx"

const appModule = lazy(() => import("./App.tsx"))

document.getElementById("root")?.removeAttribute("data-startup-pending")
document.title = "Codex Tools"

const requestedTheme = import.meta.env.DEV
  ? new URLSearchParams(window.location.search).get("theme")
  : null
const previewTheme =
  requestedTheme === "light" || requestedTheme === "dark"
    ? requestedTheme
    : undefined

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ErrorBoundary label="应用初始化失败">
      <ThemeProvider
        attribute="class"
        defaultTheme="system"
        enableSystem
        forcedTheme={previewTheme}
        disableTransitionOnChange
        storageKey="codex-tools-theme"
      >
        <EditableContextMenu />
        <Suspense
          fallback={
            <main
              role="status"
              className="grid min-h-screen place-items-center p-6 text-center"
            >
              <div>
                <h1 className="mb-2 text-xl font-medium">
                  正在启动 Codex Tools
                </h1>
                <p className="text-sm text-muted-foreground">
                  正在加载应用界面。
                </p>
              </div>
            </main>
          }
        >
          {createElement(appModule)}
        </Suspense>
      </ThemeProvider>
    </ErrorBoundary>
  </StrictMode>
)
