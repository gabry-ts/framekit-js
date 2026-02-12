import type { SQLWriteOptions } from '../../types/options';

function escapeIdentifier(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

function escapeSQLValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'NULL';
  }
  if (typeof value === 'boolean') {
    return value ? 'TRUE' : 'FALSE';
  }
  if (typeof value === 'number') {
    if (!isFinite(value)) {
      return 'NULL';
    }
    return String(value);
  }
  if (typeof value === 'bigint') {
    return String(value);
  }
  if (value instanceof Date) {
    return `'${value.toISOString()}'`;
  }
  if (typeof value === 'string') {
    return `'${value.replace(/'/g, "''")}'`;
  }
  // Objects/arrays: serialize as JSON string
  const json = JSON.stringify(value);
  return `'${json.replace(/'/g, "''")}'`;
}

export function writeSQL(
  tableName: string,
  header: string[],
  rows: unknown[][],
  options: SQLWriteOptions = {},
): string {
  const batchSize = options.batchSize ?? 1000;

  if (header.length === 0 || rows.length === 0) {
    return '';
  }

  const columnList = header.map(escapeIdentifier).join(', ');
  const statements: string[] = [];

  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const valueRows = batch.map((row) => {
      const values = row.map(escapeSQLValue).join(', ');
      return `(${values})`;
    });

    statements.push(
      `INSERT INTO ${escapeIdentifier(tableName)} (${columnList}) VALUES\n${valueRows.join(',\n')};`,
    );
  }

  return statements.join('\n\n');
}
