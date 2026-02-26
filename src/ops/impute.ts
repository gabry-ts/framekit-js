import type { DataFrame } from '../dataframe';
import { ColumnNotFoundError } from '../errors';

export type ImputeValue = unknown | ((d: Record<string, unknown[]>) => unknown);

export interface ImputeOptions {
  expand?: string[];
}

function rowKey(row: Record<string, unknown>, keys: string[]): string {
  return keys
    .map((k) => {
      const value = row[k];
      if (value === null) return '__null__';
      if (value instanceof Date) return `__date__${String(value.getTime())}`;
      return `__${typeof value}__${JSON.stringify(value)}`;
    })
    .join('|');
}

function cartesian(values: unknown[][]): unknown[][] {
  if (values.length === 0) return [[]];
  let out: unknown[][] = [[]];
  for (const set of values) {
    const next: unknown[][] = [];
    for (const prefix of out) {
      for (const item of set) {
        next.push([...prefix, item]);
      }
    }
    out = next;
  }
  return out;
}

export function impute(
  df: DataFrame<Record<string, unknown>>,
  values: Record<string, ImputeValue>,
  options: ImputeOptions = {},
): DataFrame<Record<string, unknown>> {
  for (const colName of Object.keys(values)) {
    if (!df.columns.includes(colName)) {
      throw new ColumnNotFoundError(colName, df.columns);
    }
  }

  const expandKeys = options.expand ?? [];
  for (const key of expandKeys) {
    if (!df.columns.includes(key)) {
      throw new ColumnNotFoundError(key, df.columns);
    }
  }

  const sourceRows = df.toArray() as Record<string, unknown>[];
  let workingRows = sourceRows;

  if (expandKeys.length > 0) {
    const keyDomains = expandKeys.map((key) => {
      const seen = new Set<string>();
      const out: unknown[] = [];
      for (const row of sourceRows) {
        const raw = row[key];
        const normalized = raw instanceof Date ? `d:${String(raw.getTime())}` : JSON.stringify(raw);
        if (!seen.has(normalized)) {
          seen.add(normalized);
          out.push(raw ?? null);
        }
      }
      return out;
    });

    const combos = cartesian(keyDomains);
    const existing = new Map<string, Record<string, unknown>>();
    for (const row of sourceRows) {
      existing.set(rowKey(row, expandKeys), row);
    }

    workingRows = combos.map((combo) => {
      const comboRow: Record<string, unknown> = {};
      for (let i = 0; i < expandKeys.length; i++) {
        comboRow[expandKeys[i]!] = combo[i] ?? null;
      }

      const existingRow = existing.get(rowKey(comboRow, expandKeys));
      if (existingRow) {
        return existingRow;
      }

      const generated: Record<string, unknown> = {};
      for (const colName of df.columns) {
        generated[colName] = comboRow[colName] ?? null;
      }
      return generated;
    });
  }

  const columnData: Record<string, unknown[]> = {};
  for (const colName of df.columns) {
    columnData[colName] = workingRows.map((row) => row[colName] ?? null);
  }

  const resolvedValues: Record<string, unknown> = {};
  for (const [colName, spec] of Object.entries(values)) {
    resolvedValues[colName] =
      typeof spec === 'function'
        ? (spec as (d: Record<string, unknown[]>) => unknown)(columnData)
        : spec;
  }

  const resultRows = workingRows.map((row) => {
    const next = { ...row };
    for (const [colName, fillValue] of Object.entries(resolvedValues)) {
      if ((next[colName] ?? null) === null) {
        next[colName] = fillValue;
      }
    }
    return next;
  });

  return (
    df.constructor as typeof df.constructor & {
      fromRows: (rows: Record<string, unknown>[]) => DataFrame<Record<string, unknown>>;
    }
  ).fromRows(resultRows);
}
