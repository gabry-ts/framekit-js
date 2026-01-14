import type { CSVReadOptions } from '../../types/options';
import { DType } from '../../types/dtype';
import { ParseError } from '../../errors';

export interface ParsedColumns {
  header: string[];
  columns: Record<string, (string | null)[]>;
  inferredTypes: Record<string, DType>;
}

const DEFAULT_NULL_VALUES = ['', 'null', 'NULL', 'NA', 'N/A', 'NaN', 'nan', 'None', 'none'];

/**
 * Single-pass RFC 4180 CSV parser with auto-detection of delimiter and column types.
 */
export function parseCSV(content: string, options: CSVReadOptions = {}): ParsedColumns {
  const lines = splitLines(content, options);
  if (lines.length === 0) {
    return { header: [], columns: {}, inferredTypes: {} };
  }

  const delimiter = options.delimiter ?? detectDelimiter(lines.slice(0, 10));
  const hasHeader = options.hasHeader !== false;
  const nullValues = new Set(options.nullValues ?? DEFAULT_NULL_VALUES);

  // Parse all rows
  const allRows = lines.map((line) => parseLine(line, delimiter));

  // Skip rows
  let dataStart = 0;
  if (options.skipRows && options.skipRows > 0) {
    dataStart = options.skipRows;
  }

  // Extract header
  let header: string[];
  let rowStart: number;
  if (options.header) {
    header = options.header;
    rowStart = dataStart + (hasHeader ? 1 : 0);
  } else if (hasHeader && dataStart < allRows.length) {
    header = allRows[dataStart]!.map((h) => h.trim());
    rowStart = dataStart + 1;
  } else {
    const firstRow = allRows[dataStart];
    if (!firstRow) {
      return { header: [], columns: {}, inferredTypes: {} };
    }
    header = firstRow.map((_, i) => `column_${i}`);
    rowStart = dataStart;
  }

  // Limit rows if nRows specified
  let rowEnd = allRows.length;
  if (options.nRows !== undefined && options.nRows >= 0) {
    rowEnd = Math.min(rowEnd, rowStart + options.nRows);
  }

  // Determine which columns to include
  const selectedColumns = options.columns
    ? new Set(options.columns)
    : null;

  // Build column arrays
  const columns: Record<string, (string | null)[]> = {};
  const activeHeader: string[] = [];
  for (const name of header) {
    if (selectedColumns && !selectedColumns.has(name)) continue;
    activeHeader.push(name);
    columns[name] = [];
  }

  // Column index mapping for selected columns
  const headerIndexMap = new Map<number, string>();
  for (let i = 0; i < header.length; i++) {
    const name = header[i]!;
    if (selectedColumns && !selectedColumns.has(name)) continue;
    headerIndexMap.set(i, name);
  }

  // Fill columns from data rows
  for (let r = rowStart; r < rowEnd; r++) {
    const row = allRows[r];
    if (!row) continue;
    for (const [colIdx, colName] of headerIndexMap) {
      const raw = colIdx < row.length ? row[colIdx]! : '';
      const value = nullValues.has(raw) ? null : raw;
      columns[colName]!.push(value);
    }
  }

  // Infer types by sampling first 100 rows
  const inferredTypes = inferColumnTypes(columns, activeHeader, options);

  return { header: activeHeader, columns, inferredTypes };
}

/**
 * Split content into logical lines, handling quoted multiline fields.
 * Also filters out comment lines.
 */
function splitLines(content: string, options: CSVReadOptions): string[] {
  const comment = options.comment;
  const lines: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < content.length; i++) {
    const ch = content[i]!;
    if (ch === '"') {
      if (inQuotes && i + 1 < content.length && content[i + 1] === '"') {
        current += '""';
        i++;
      } else {
        inQuotes = !inQuotes;
        current += ch;
      }
    } else if ((ch === '\n' || ch === '\r') && !inQuotes) {
      if (ch === '\r' && i + 1 < content.length && content[i + 1] === '\n') {
        i++; // skip \n after \r
      }
      if (current.length > 0 || lines.length > 0) {
        if (!comment || !current.trimStart().startsWith(comment)) {
          lines.push(current);
        }
      }
      current = '';
    } else {
      current += ch;
    }
  }

  // Last line
  if (current.length > 0) {
    if (!comment || !current.trimStart().startsWith(comment)) {
      lines.push(current);
    }
  }

  return lines;
}

/**
 * Parse a single CSV line into fields, respecting quoted fields and escaped quotes.
 */
function parseLine(line: string, delimiter: string): string[] {
  const fields: string[] = [];
  let current = '';
  let inQuotes = false;
  let i = 0;

  while (i < line.length) {
    const ch = line[i]!;

    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          // Escaped quote
          current += '"';
          i += 2;
        } else {
          // End of quoted field
          inQuotes = false;
          i++;
        }
      } else {
        current += ch;
        i++;
      }
    } else {
      if (ch === '"' && current.length === 0) {
        // Start of quoted field
        inQuotes = true;
        i++;
      } else if (line.startsWith(delimiter, i)) {
        fields.push(current);
        current = '';
        i += delimiter.length;
      } else {
        current += ch;
        i++;
      }
    }
  }

  if (inQuotes) {
    throw new ParseError('Unterminated quoted field in CSV');
  }

  fields.push(current);
  return fields;
}

/**
 * Auto-detect delimiter from first N lines by scoring candidates.
 */
function detectDelimiter(lines: string[]): string {
  const candidates = [',', ';', '\t', '|'];
  let bestDelimiter = ',';
  let bestScore = -1;

  for (const delim of candidates) {
    const counts = lines.map((line) => {
      let count = 0;
      let inQuotes = false;
      for (let i = 0; i < line.length; i++) {
        const ch = line[i]!;
        if (ch === '"') inQuotes = !inQuotes;
        else if (!inQuotes && line.startsWith(delim, i)) count++;
      }
      return count;
    });

    // Score: consistency (low variance) * average count
    if (counts.length === 0) continue;
    const avg = counts.reduce((a, b) => a + b, 0) / counts.length;
    if (avg === 0) continue;

    // All lines should have the same count for a good delimiter
    const allSame = counts.every((c) => c === counts[0]);
    const score = allSame ? avg * 2 : avg;

    if (score > bestScore) {
      bestScore = score;
      bestDelimiter = delim;
    }
  }

  return bestDelimiter;
}

/**
 * Infer column types by sampling values.
 */
function inferColumnTypes(
  columns: Record<string, (string | null)[]>,
  header: string[],
  options: CSVReadOptions,
): Record<string, DType> {
  const types: Record<string, DType> = {};
  const parseNumbers = options.parseNumbers !== false;
  const parseDates = options.parseDates !== false;

  for (const name of header) {
    // Check if user specified dtype
    if (options.dtypes && name in options.dtypes) {
      types[name] = options.dtypes[name]!;
      continue;
    }

    const values = columns[name]!;
    const sample = values.slice(0, 100).filter((v): v is string => v !== null);

    if (sample.length === 0) {
      types[name] = DType.Utf8;
      continue;
    }

    // Try number
    if (parseNumbers && sample.every(isNumericString)) {
      types[name] = sample.every(isIntegerString) ? DType.Int32 : DType.Float64;
      continue;
    }

    // Try boolean
    if (sample.every(isBooleanString)) {
      types[name] = DType.Boolean;
      continue;
    }

    // Try date
    if (parseDates && sample.every(isDateString)) {
      types[name] = DType.Date;
      continue;
    }

    types[name] = DType.Utf8;
  }

  return types;
}

function isNumericString(s: string): boolean {
  if (s.length === 0) return false;
  const n = Number(s);
  return !Number.isNaN(n) && s.trim().length > 0;
}

function isIntegerString(s: string): boolean {
  if (!isNumericString(s)) return false;
  const n = Number(s);
  return Number.isInteger(n) && !s.includes('.') && !s.includes('e') && !s.includes('E');
}

function isBooleanString(s: string): boolean {
  const lower = s.toLowerCase();
  return lower === 'true' || lower === 'false';
}

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$/;

function isDateString(s: string): boolean {
  if (!ISO_DATE_RE.test(s)) return false;
  const d = new Date(s);
  return !Number.isNaN(d.getTime());
}
