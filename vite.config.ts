import path from "path"
import tailwindcss from "@tailwindcss/vite"
import react from "@vitejs/plugin-react"
import { defineConfig } from "vite"

// https://vite.dev/config/
export default defineConfig({
  server: { port: 1420, strictPort: true },
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: {
    target: "es2022",
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes("node_modules")) return
          if (/\/node_modules\/(react|react-dom|scheduler)\//.test(id)) return "react"
          if (/\/node_modules\/@base-ui\/react\//.test(id)) return "baseui"
          if (/\/node_modules\/(next-themes|class-variance-authority|clsx|tailwind-merge)\//.test(id)) return "ui"
        },
      },
    },
  },
})
