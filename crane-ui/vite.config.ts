import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    proxy: {
      // Dev only: forward /api/* to the main-deployed crane-manager API.
      // Production builds hit VITE_API_URL instead (see src/services/api.ts).
      '/api': {
        target: 'https://crane-manager-gamma.onrender.com',
        changeOrigin: true,
      },
    },
  },
});
