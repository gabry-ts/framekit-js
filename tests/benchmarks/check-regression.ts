import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';

type Result = {
  benchmark: string;
  relative?: number;
};

async function main(): Promise<void> {
  const threshold = Number(process.env.BENCH_REGRESSION_THRESHOLD ?? '1.15');
  const resultsDir = path.resolve('benchmarks/results');
  const entries = await readdir(resultsDir);
  const jsonFiles = entries.filter((name) => name.endsWith('.json'));

  let violations = 0;

  for (const file of jsonFiles) {
    const fullPath = path.join(resultsDir, file);
    const raw = await readFile(fullPath, 'utf-8');
    const result = JSON.parse(raw) as Result;
    if (typeof result.relative !== 'number') {
      continue;
    }
    if (result.relative > threshold) {
      violations++;
      console.error(
        `[bench-regression] ${result.benchmark} median ratio ${result.relative.toFixed(3)} exceeds threshold ${threshold.toFixed(3)}`,
      );
    }
  }

  if (violations > 0) {
    process.exit(1);
  }
}

main().catch((err: unknown) => {
  const message = err instanceof Error ? err.message : String(err);
  console.error(`[bench-regression] failed: ${message}`);
  process.exit(1);
});
