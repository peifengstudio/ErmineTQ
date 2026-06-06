import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [tailwindcss(), react()],
  base: './',
  build: {
    // Output to internal/dashboard/dist so Go's embed.FS can reference it
    // directly from internal/dashboard/embed.go without needing ../
    outDir: '../internal/dashboard/dist',
    emptyOutDir: true,
    // Inline assets up to 100 KB to reduce the number of files embedded in binary
    assetsInlineLimit: 102400,
    rollupOptions: {
      output: {
        manualChunks: undefined,
      },
    },
  },
  server: {
    port: 5173,
    // Proxy API and SSE requests to the Go backend during development
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
})
