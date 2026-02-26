import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig({
  plugins: [react()],
  base: '/framekit-js/',
  resolve: {
    alias: {
      'framekit-js': path.resolve(__dirname, '../../dist/browser.js'),
      'framekit-js/browser': path.resolve(__dirname, '../../dist/browser.js'),
      // Node.js built-in shims for browser build
      os: path.resolve(__dirname, './src/shims/os.ts'),
    },
  },
  optimizeDeps: {
    exclude: ['esbuild-wasm'],
  },
  worker: {
    format: 'es',
  },
  assetsInclude: ['**/*.csv', '**/*.wasm'],
  build: {
    outDir: 'dist',
    sourcemap: true,
  },
});
