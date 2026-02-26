import { DataFrame } from '../../../src';

function lcg(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (1664525 * state + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

function pick<T>(rng: () => number, values: T[]): T {
  return values[Math.floor(rng() * values.length)]!;
}

export function numericDataset(rows: number, seed = 42): DataFrame<Record<string, unknown>> {
  const rand = lcg(seed);
  const out: Record<string, unknown>[] = [];
  for (let i = 0; i < rows; i++) {
    out.push({
      id: i,
      group: `g_${String(i % 100)}`,
      amount: Math.floor(rand() * 10000),
      score: rand() * 100,
    });
  }
  return DataFrame.fromRows(out);
}

export function stringDataset(rows: number, seed = 43): DataFrame<Record<string, unknown>> {
  const rand = lcg(seed);
  const words = ['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta'];
  const out: Record<string, unknown>[] = [];
  for (let i = 0; i < rows; i++) {
    out.push({
      id: i,
      city: pick(rand, words),
      channel: pick(rand, ['web', 'app', 'api']),
      tag: `${pick(rand, words)}_${String(i % 50)}`,
    });
  }
  return DataFrame.fromRows(out);
}

export function mixedDataset(rows: number, seed = 44): DataFrame<Record<string, unknown>> {
  const rand = lcg(seed);
  const out: Record<string, unknown>[] = [];
  for (let i = 0; i < rows; i++) {
    out.push({
      id: i,
      region: pick(rand, ['NA', 'EMEA', 'APAC']),
      active: rand() > 0.5,
      revenue: Math.round(rand() * 100000) / 100,
      orders: Math.floor(rand() * 25),
      tags: [pick(rand, ['new', 'vip', 'repeat']), pick(rand, ['promo', 'organic', 'paid'])],
    });
  }
  return DataFrame.fromRows(out);
}

export function makeStandardDatasets(seed = 101): {
  numeric10k: DataFrame<Record<string, unknown>>;
  numeric100k: DataFrame<Record<string, unknown>>;
  numeric1m: DataFrame<Record<string, unknown>>;
  strings100k: DataFrame<Record<string, unknown>>;
  mixed100k: DataFrame<Record<string, unknown>>;
} {
  return {
    numeric10k: numericDataset(10_000, seed),
    numeric100k: numericDataset(100_000, seed + 1),
    numeric1m: numericDataset(1_000_000, seed + 2),
    strings100k: stringDataset(100_000, seed + 3),
    mixed100k: mixedDataset(100_000, seed + 4),
  };
}
