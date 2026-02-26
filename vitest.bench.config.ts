import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/benchmarks/**/*.bench.ts'],
    passWithNoTests: false,
  },
});
