#!/usr/bin/env node

import { DataFrame } from './dataframe';

interface ParsedQuery {
  columns: string[] | '*';
  from: string;
  where?: { column: string; op: string; value: string | number };
  orderBy?: { column: string; order: 'asc' | 'desc' };
  limit?: number;
}

function parseValue(raw: string): string | number {
  // Remove quotes if present
  if ((raw.startsWith("'") && raw.endsWith("'")) || (raw.startsWith('"') && raw.endsWith('"'))) {
    return raw.slice(1, -1);
  }
  const num = Number(raw);
  if (!isNaN(num)) return num;
  return raw;
}

function parseSQL(sql: string): ParsedQuery {
  // Normalize whitespace
  const normalized = sql.replace(/\s+/g, ' ').trim();

  // Case-insensitive keyword matching
  const selectMatch = normalized.match(/^SELECT\s+(.+?)\s+FROM\s+/i);
  if (!selectMatch) {
    throw new Error('Invalid query: expected SELECT ... FROM ...');
  }

  const columnsRaw = selectMatch[1]!.trim();
  const columns: string[] | '*' =
    columnsRaw === '*' ? '*' : columnsRaw.split(',').map((c) => c.trim());

  // Extract FROM clause (file path)
  const afterSelect = normalized.slice(selectMatch[0].length);
  // FROM value ends at next keyword (WHERE, ORDER, LIMIT) or end of string
  const fromMatch = afterSelect.match(/^(\S+)(?:\s|$)/);
  if (!fromMatch) {
    throw new Error('Invalid query: expected file path after FROM');
  }
  const from = fromMatch[1]!;

  let remaining = afterSelect.slice(fromMatch[0].length).trim();
  const result: ParsedQuery = { columns, from };

  // Parse WHERE
  const whereMatch = remaining.match(/^WHERE\s+(\S+)\s+(=|!=|>=|<=|>|<)\s+(\S+)/i);
  if (whereMatch) {
    result.where = {
      column: whereMatch[1]!,
      op: whereMatch[2]!,
      value: parseValue(whereMatch[3]!),
    };
    remaining = remaining.slice(whereMatch[0].length).trim();
  }

  // Parse ORDER BY
  const orderMatch = remaining.match(/^ORDER\s+BY\s+(\S+)(?:\s+(ASC|DESC))?/i);
  if (orderMatch) {
    result.orderBy = {
      column: orderMatch[1]!,
      order: (orderMatch[2]?.toLowerCase() as 'asc' | 'desc') ?? 'asc',
    };
    remaining = remaining.slice(orderMatch[0].length).trim();
  }

  // Parse LIMIT
  const limitMatch = remaining.match(/^LIMIT\s+(\d+)/i);
  if (limitMatch) {
    result.limit = parseInt(limitMatch[1]!, 10);
  }

  return result;
}

function detectFormat(filePath: string): 'csv' | 'json' | 'ndjson' {
  const lower = filePath.toLowerCase();
  if (lower.endsWith('.json')) return 'json';
  if (lower.endsWith('.ndjson') || lower.endsWith('.jsonl')) return 'ndjson';
  return 'csv';
}

async function readFile(filePath: string): Promise<DataFrame> {
  const format = detectFormat(filePath);
  switch (format) {
    case 'json':
      return DataFrame.fromJSON(filePath);
    case 'ndjson':
      return DataFrame.fromNDJSON(filePath);
    case 'csv':
    default:
      return DataFrame.fromCSV(filePath);
  }
}

function formatOutput(df: DataFrame, format: string): string {
  switch (format) {
    case 'csv':
      return df.toCSV();
    case 'json':
      return df.toJSON();
    case 'ndjson':
      return df.toNDJSON();
    default:
      return df.toString({ maxRows: 1000, maxCols: 100 });
  }
}

function printUsage(): void {
  console.log(`Usage: framekit query '<SQL>' [options]

Options:
  --format csv|json|ndjson   Output format (default: table)
  --output <file>            Write output to file instead of stdout

Examples:
  framekit query 'SELECT * FROM data.csv'
  framekit query 'SELECT name, age FROM data.csv WHERE age > 30 ORDER BY name LIMIT 10'
  framekit query 'SELECT * FROM data.json' --format csv
  framekit query 'SELECT * FROM data.csv WHERE amount > 100' --output result.csv --format csv`);
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);

  if (args.length === 0 || args[0] === '--help' || args[0] === '-h') {
    printUsage();
    process.exit(0);
  }

  const command = args[0];
  if (command !== 'query') {
    console.error(`Unknown command: ${command}`);
    printUsage();
    process.exit(1);
  }

  const sqlQuery = args[1];
  if (!sqlQuery) {
    console.error('Error: missing SQL query');
    printUsage();
    process.exit(1);
  }

  // Parse flags
  let outputFormat = 'table';
  let outputFile: string | undefined;

  for (let i = 2; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--format' && args[i + 1]) {
      outputFormat = args[i + 1]!;
      i++;
    } else if (arg === '--output' && args[i + 1]) {
      outputFile = args[i + 1]!;
      i++;
    }
  }

  try {
    const parsed = parseSQL(sqlQuery);
    let df = await readFile(parsed.from);

    // Apply SELECT
    if (parsed.columns !== '*') {
      df = df.select(...parsed.columns);
    }

    // Apply WHERE
    if (parsed.where) {
      const { column, op, value } = parsed.where;
      df = df.where(column, op as '=' | '!=' | '>' | '>=' | '<' | '<=', value);
    }

    // Apply ORDER BY
    if (parsed.orderBy) {
      df = df.sortBy(parsed.orderBy.column, parsed.orderBy.order);
    }

    // Apply LIMIT
    if (parsed.limit !== undefined) {
      df = df.head(parsed.limit);
    }

    // Output
    const output = formatOutput(df, outputFormat);

    if (outputFile) {
      const fs = await import('fs/promises');
      await fs.writeFile(outputFile, output, 'utf-8');
      console.log(`Output written to ${outputFile}`);
    } else {
      console.log(output);
    }
  } catch (err) {
    console.error(`Error: ${err instanceof Error ? err.message : String(err)}`);
    process.exit(1);
  }
}

void main();
