import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'
import { fileURLToPath } from 'url'
import { dirname } from 'path'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), 'VITE_')
  const backendUrl = env.VITE_BACKEND_URL || `http://localhost:${env.VITE_BACKEND_PORT || '8000'}`

  const apiProxy = {
    '/api': {
      target: backendUrl,
      changeOrigin: true
    }
  }

  return {
    plugins: [react()],
    server: {
      host: '0.0.0.0',
      port: 8001,
      allowedHosts: true,
      proxy: apiProxy
    },
    preview: {
      host: '0.0.0.0',
      port: 8001,
      proxy: apiProxy
    },
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src')
      }
    }
  }
})
