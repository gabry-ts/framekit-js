import { defineConfig } from 'tsup';

export default defineConfig([
  {
    entry: ['src/index.ts', 'src/cli.ts'],
    format: ['cjs', 'esm'],
    dts: true,
    splitting: false,
    sourcemap: true,
    clean: true,
    tsconfig: 'tsconfig.build.json',
  },
  {
    entry: ['src/browser.ts'],
    format: ['esm'],
    dts: true,
    splitting: false,
    sourcemap: true,
    clean: false,
    tsconfig: 'tsconfig.build.json',
    platform: 'browser',
    external: ['fs', 'fs/promises', 'os', 'worker_threads', 'path', 'stream', 'exceljs', 'parquet-wasm', 'apache-arrow'],
  },
]);
