import { ParseError } from '../errors';
import { col, lit, Expr, AggExpr } from '../expr/expr';
import type { DataFrame } from '../dataframe';
import { Series } from '../series';
import { BooleanColumn } from '../storage/boolean';

// ── Token types ──────────────────────────────────────────────────────

enum TokenType {
  // Keywords
  SELECT = 'SELECT',
  FROM = 'FROM',
  WHERE = 'WHERE',
  ORDER = 'ORDER',
  BY = 'BY',
  LIMIT = 'LIMIT',
  GROUP = 'GROUP',
  HAVING = 'HAVING',
  AND = 'AND',
  OR = 'OR',
  IN = 'IN',
  LIKE = 'LIKE',
  IS = 'IS',
  NOT = 'NOT',
  NULL = 'NULL',
  ASC = 'ASC',
  DESC = 'DESC',
  AS = 'AS',
  // Literals & identifiers
  IDENTIFIER = 'IDENTIFIER',
  NUMBER = 'NUMBER',
  STRING = 'STRING',
  STAR = 'STAR',
  // Operators
  EQ = 'EQ',
  NEQ = 'NEQ',
  GT = 'GT',
  GTE = 'GTE',
  LT = 'LT',
  LTE = 'LTE',
  // Punctuation
  COMMA = 'COMMA',
  LPAREN = 'LPAREN',
  RPAREN = 'RPAREN',
  // End
  EOF = 'EOF',
}

interface Token {
  type: TokenType;
  value: string;
  position: number;
}

// ── Lexer ────────────────────────────────────────────────────────────

const KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'ORDER', 'BY', 'LIMIT', 'GROUP',
  'HAVING', 'AND', 'OR', 'IN', 'LIKE', 'IS', 'NOT', 'NULL',
  'ASC', 'DESC', 'AS',
]);

function tokenize(input: string): Token[] {
  const tokens: Token[] = [];
  let i = 0;

  while (i < input.length) {
    // Skip whitespace
    if (/\s/.test(input[i]!)) {
      i++;
      continue;
    }

    const pos = i;

    // String literal (single-quoted)
    if (input[i] === "'") {
      i++;
      let value = '';
      while (i < input.length && input[i] !== "'") {
        if (input[i] === "'" && input[i + 1] === "'") {
          value += "'";
          i += 2;
        } else {
          value += input[i]!;
          i++;
        }
      }
      if (i >= input.length) {
        throw new ParseError(`Unterminated string literal at position ${pos}`);
      }
      i++; // skip closing quote
      tokens.push({ type: TokenType.STRING, value, position: pos });
      continue;
    }

    // Number
    if (/\d/.test(input[i]!) || (input[i] === '-' && i + 1 < input.length && /\d/.test(input[i + 1]!))) {
      let value = '';
      if (input[i] === '-') {
        value += '-';
        i++;
      }
      while (i < input.length && /[\d.]/.test(input[i]!)) {
        value += input[i]!;
        i++;
      }
      tokens.push({ type: TokenType.NUMBER, value, position: pos });
      continue;
    }

    // Operators and punctuation
    if (input[i] === '*') { tokens.push({ type: TokenType.STAR, value: '*', position: pos }); i++; continue; }
    if (input[i] === ',') { tokens.push({ type: TokenType.COMMA, value: ',', position: pos }); i++; continue; }
    if (input[i] === '(') { tokens.push({ type: TokenType.LPAREN, value: '(', position: pos }); i++; continue; }
    if (input[i] === ')') { tokens.push({ type: TokenType.RPAREN, value: ')', position: pos }); i++; continue; }

    if (input[i] === '!' && input[i + 1] === '=') {
      tokens.push({ type: TokenType.NEQ, value: '!=', position: pos });
      i += 2;
      continue;
    }
    if (input[i] === '<' && input[i + 1] === '>') {
      tokens.push({ type: TokenType.NEQ, value: '<>', position: pos });
      i += 2;
      continue;
    }
    if (input[i] === '>' && input[i + 1] === '=') {
      tokens.push({ type: TokenType.GTE, value: '>=', position: pos });
      i += 2;
      continue;
    }
    if (input[i] === '<' && input[i + 1] === '=') {
      tokens.push({ type: TokenType.LTE, value: '<=', position: pos });
      i += 2;
      continue;
    }
    if (input[i] === '>') { tokens.push({ type: TokenType.GT, value: '>', position: pos }); i++; continue; }
    if (input[i] === '<') { tokens.push({ type: TokenType.LT, value: '<', position: pos }); i++; continue; }
    if (input[i] === '=') { tokens.push({ type: TokenType.EQ, value: '=', position: pos }); i++; continue; }

    // Identifiers and keywords
    if (/[a-zA-Z_]/.test(input[i]!)) {
      let value = '';
      while (i < input.length && /[a-zA-Z0-9_]/.test(input[i]!)) {
        value += input[i]!;
        i++;
      }
      const upper = value.toUpperCase();
      if (KEYWORDS.has(upper)) {
        tokens.push({ type: upper as TokenType, value: upper, position: pos });
      } else {
        tokens.push({ type: TokenType.IDENTIFIER, value, position: pos });
      }
      continue;
    }

    throw new ParseError(`Unexpected character '${input[i]!}' at position ${pos}`);
  }

  tokens.push({ type: TokenType.EOF, value: '', position: i });
  return tokens;
}

// ── AST types ────────────────────────────────────────────────────────

interface SelectItem {
  column: string;
  alias: string | undefined;
  aggregate: string | undefined; // 'SUM', 'AVG', 'COUNT', 'MIN', 'MAX'
}

interface OrderByItem {
  column: string;
  direction: 'asc' | 'desc';
}

interface ParsedQuery {
  selectItems: SelectItem[];
  selectAll: boolean;
  whereExpr: Expr<boolean> | undefined;
  groupByColumns: string[];
  havingExpr: Expr<boolean> | undefined;
  orderByItems: OrderByItem[];
  limit: number | undefined;
}

// ── Parser ───────────────────────────────────────────────────────────

class Parser {
  private tokens: Token[];
  private pos: number;
  private _selectItems: SelectItem[] = [];

  constructor(tokens: Token[]) {
    this.tokens = tokens;
    this.pos = 0;
  }

  private current(): Token {
    return this.tokens[this.pos]!;
  }

  private peek(): Token {
    return this.tokens[this.pos]!;
  }

  private advance(): Token {
    const token = this.tokens[this.pos]!;
    this.pos++;
    return token;
  }

  private expect(type: TokenType): Token {
    const token = this.current();
    if (token.type !== type) {
      throw new ParseError(
        `Expected ${type} but got '${token.value}' at position ${token.position}`,
      );
    }
    return this.advance();
  }

  private match(type: TokenType): boolean {
    if (this.current().type === type) {
      this.advance();
      return true;
    }
    return false;
  }

  parse(): ParsedQuery {
    this.expect(TokenType.SELECT);

    // Parse SELECT items
    let selectAll = false;
    const selectItems: SelectItem[] = [];

    if (this.current().type === TokenType.STAR) {
      selectAll = true;
      this.advance();
    } else {
      selectItems.push(this.parseSelectItem());
      while (this.match(TokenType.COMMA)) {
        selectItems.push(this.parseSelectItem());
      }
    }

    this._selectItems = selectItems;

    // FROM clause (required, always 'this')
    this.expect(TokenType.FROM);
    const fromToken = this.expect(TokenType.IDENTIFIER);
    if (fromToken.value !== 'this') {
      throw new ParseError(
        `FROM clause must reference 'this', got '${fromToken.value}' at position ${fromToken.position}`,
      );
    }

    // Optional WHERE
    let whereExpr: Expr<boolean> | undefined;
    if (this.current().type === TokenType.WHERE) {
      this.advance();
      whereExpr = this.parseOrExpr();
    }

    // Optional GROUP BY
    const groupByColumns: string[] = [];
    if (this.current().type === TokenType.GROUP) {
      this.advance();
      this.expect(TokenType.BY);
      groupByColumns.push(this.expect(TokenType.IDENTIFIER).value);
      while (this.match(TokenType.COMMA)) {
        groupByColumns.push(this.expect(TokenType.IDENTIFIER).value);
      }
    }

    // Optional HAVING
    let havingExpr: Expr<boolean> | undefined;
    if (this.current().type === TokenType.HAVING) {
      this.advance();
      havingExpr = this.parseOrExpr();
    }

    // Optional ORDER BY
    const orderByItems: OrderByItem[] = [];
    if (this.current().type === TokenType.ORDER) {
      this.advance();
      this.expect(TokenType.BY);
      orderByItems.push(this.parseOrderByItem());
      while (this.match(TokenType.COMMA)) {
        orderByItems.push(this.parseOrderByItem());
      }
    }

    // Optional LIMIT
    let limit: number | undefined;
    if (this.current().type === TokenType.LIMIT) {
      this.advance();
      const num = this.expect(TokenType.NUMBER);
      limit = parseInt(num.value, 10);
    }

    if (this.current().type !== TokenType.EOF) {
      throw new ParseError(
        `Unexpected token '${this.current().value}' at position ${this.current().position}`,
      );
    }

    return { selectItems, selectAll, whereExpr, groupByColumns, havingExpr, orderByItems, limit };
  }

  private parseSelectItem(): SelectItem {
    // Check for aggregate function: SUM(col), AVG(col), COUNT(col), MIN(col), MAX(col)
    const token = this.current();
    const upperVal = token.value.toUpperCase();
    if (
      token.type === TokenType.IDENTIFIER &&
      ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX'].includes(upperVal) &&
      this.tokens[this.pos + 1]?.type === TokenType.LPAREN
    ) {
      const aggName = upperVal;
      this.advance(); // consume function name
      this.advance(); // consume '('
      let columnName: string;
      if (this.current().type === TokenType.STAR) {
        columnName = '*';
        this.advance();
      } else {
        columnName = this.expect(TokenType.IDENTIFIER).value;
      }
      this.expect(TokenType.RPAREN);

      let alias: string | undefined;
      if (this.current().type === TokenType.AS) {
        this.advance();
        alias = this.expect(TokenType.IDENTIFIER).value;
      }

      return { column: columnName, alias, aggregate: aggName };
    }

    // Plain column
    const colName = this.expect(TokenType.IDENTIFIER).value;
    let alias: string | undefined;
    if (this.current().type === TokenType.AS) {
      this.advance();
      alias = this.expect(TokenType.IDENTIFIER).value;
    }

    return { column: colName, alias, aggregate: undefined };
  }

  private parseOrderByItem(): OrderByItem {
    const column = this.expect(TokenType.IDENTIFIER).value;
    let direction: 'asc' | 'desc' = 'asc';
    if (this.current().type === TokenType.ASC) {
      this.advance();
      direction = 'asc';
    } else if (this.current().type === TokenType.DESC) {
      this.advance();
      direction = 'desc';
    }
    return { column, direction };
  }

  // ── Expression parsing (WHERE / HAVING) ──

  private parseOrExpr(): Expr<boolean> {
    let left = this.parseAndExpr();
    while (this.current().type === TokenType.OR) {
      this.advance();
      const right = this.parseAndExpr();
      left = left.or(right);
    }
    return left;
  }

  private parseAndExpr(): Expr<boolean> {
    let left = this.parseComparison();
    while (this.current().type === TokenType.AND) {
      this.advance();
      const right = this.parseComparison();
      left = left.and(right);
    }
    return left;
  }

  private parseComparison(): Expr<boolean> {
    // Handle parenthesized expressions
    if (this.current().type === TokenType.LPAREN) {
      this.advance();
      const expr = this.parseOrExpr();
      this.expect(TokenType.RPAREN);
      return expr;
    }

    // Handle NOT
    if (this.current().type === TokenType.NOT) {
      this.advance();
      const expr = this.parseComparison();
      return expr.not();
    }

    const leftToken = this.current();
    // Check for aggregate in HAVING: SUM(col) > 100
    const upperVal = leftToken.value.toUpperCase();
    if (
      leftToken.type === TokenType.IDENTIFIER &&
      ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX'].includes(upperVal) &&
      this.tokens[this.pos + 1]?.type === TokenType.LPAREN
    ) {
      // This is an aggregate comparison in HAVING
      return this.parseAggregateComparison();
    }

    const columnName = this.expect(TokenType.IDENTIFIER).value;
    const colExpr = col(columnName);

    // IS NULL / IS NOT NULL
    if (this.current().type === TokenType.IS) {
      this.advance();
      if (this.current().type === TokenType.NOT) {
        this.advance();
        this.expect(TokenType.NULL);
        return new IsNullExpr(columnName, true);
      }
      this.expect(TokenType.NULL);
      return new IsNullExpr(columnName, false);
    }

    // NOT IN / IN
    if (this.current().type === TokenType.NOT) {
      this.advance();
      if (this.current().type === TokenType.IN) {
        this.advance();
        const values = this.parseValueList();
        // Build OR chain: NOT (col = v1 OR col = v2 OR ...)
        let inExpr: Expr<boolean> = colExpr.eq(lit(values[0]));
        for (let i = 1; i < values.length; i++) {
          inExpr = inExpr.or(colExpr.eq(lit(values[i])));
        }
        return inExpr.not();
      }
      if (this.current().type === TokenType.LIKE) {
        this.advance();
        const pattern = this.expect(TokenType.STRING).value;
        return this.buildLikeExpr(columnName, pattern).not();
      }
      throw new ParseError(`Expected IN or LIKE after NOT at position ${this.current().position}`);
    }

    if (this.current().type === TokenType.IN) {
      this.advance();
      const values = this.parseValueList();
      let inExpr: Expr<boolean> = colExpr.eq(lit(values[0]));
      for (let i = 1; i < values.length; i++) {
        inExpr = inExpr.or(colExpr.eq(lit(values[i])));
      }
      return inExpr;
    }

    // LIKE
    if (this.current().type === TokenType.LIKE) {
      this.advance();
      const pattern = this.expect(TokenType.STRING).value;
      return this.buildLikeExpr(columnName, pattern);
    }

    // Standard comparison: =, !=, >, <, >=, <=
    const op = this.current();
    this.advance();

    const rightValue = this.parseLiteralValue();

    switch (op.type) {
      case TokenType.EQ: return colExpr.eq(lit(rightValue));
      case TokenType.NEQ: return colExpr.neq(lit(rightValue));
      case TokenType.GT: return colExpr.gt(lit(rightValue));
      case TokenType.GTE: return colExpr.gte(lit(rightValue));
      case TokenType.LT: return colExpr.lt(lit(rightValue));
      case TokenType.LTE: return colExpr.lte(lit(rightValue));
      default:
        throw new ParseError(`Expected comparison operator at position ${op.position}, got '${op.value}'`);
    }
  }

  private parseAggregateComparison(): Expr<boolean> {
    // Parse aggregate function in HAVING context
    const aggName = this.advance().value.toUpperCase();
    this.expect(TokenType.LPAREN);
    let columnName: string;
    if (this.current().type === TokenType.STAR) {
      columnName = '*';
      this.advance();
    } else {
      columnName = this.expect(TokenType.IDENTIFIER).value;
    }
    this.expect(TokenType.RPAREN);

    // Look up the alias from SELECT items for this aggregate
    const matchingSelect = this._selectItems.find(
      (item) => item.aggregate === aggName && item.column === columnName,
    );
    const aggAlias = matchingSelect?.alias
      ?? (columnName === '*'
        ? `${aggName.toLowerCase()}`
        : `${aggName.toLowerCase()}_${columnName}`);

    const aggColExpr = col(aggAlias);

    const op = this.current();
    this.advance();
    const rightValue = this.parseLiteralValue();

    switch (op.type) {
      case TokenType.EQ: return aggColExpr.eq(lit(rightValue));
      case TokenType.NEQ: return aggColExpr.neq(lit(rightValue));
      case TokenType.GT: return aggColExpr.gt(lit(rightValue));
      case TokenType.GTE: return aggColExpr.gte(lit(rightValue));
      case TokenType.LT: return aggColExpr.lt(lit(rightValue));
      case TokenType.LTE: return aggColExpr.lte(lit(rightValue));
      default:
        throw new ParseError(`Expected comparison operator at position ${op.position}, got '${op.value}'`);
    }
  }

  private parseLiteralValue(): unknown {
    const token = this.current();
    if (token.type === TokenType.NUMBER) {
      this.advance();
      return parseFloat(token.value);
    }
    if (token.type === TokenType.STRING) {
      this.advance();
      return token.value;
    }
    if (token.type === TokenType.NULL) {
      this.advance();
      return null;
    }
    // Also handle identifiers as possible values (e.g., true/false)
    if (token.type === TokenType.IDENTIFIER) {
      const upper = token.value.toUpperCase();
      if (upper === 'TRUE') { this.advance(); return true; }
      if (upper === 'FALSE') { this.advance(); return false; }
    }
    throw new ParseError(`Expected literal value at position ${token.position}, got '${token.value}'`);
  }

  private parseValueList(): unknown[] {
    this.expect(TokenType.LPAREN);
    const values: unknown[] = [];
    values.push(this.parseLiteralValue());
    while (this.match(TokenType.COMMA)) {
      values.push(this.parseLiteralValue());
    }
    this.expect(TokenType.RPAREN);
    return values;
  }

  private buildLikeExpr(columnName: string, pattern: string): Expr<boolean> {
    // Convert SQL LIKE pattern to a regex-based filter
    // SQL LIKE: % = any chars, _ = single char
    // We build this as a filter function expression
    const regexStr = '^' + pattern
      .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')  // escape regex special chars
      .replace(/%/g, '.*')                        // but then handle % → .*
      .replace(/_/g, '.')                          // and _ → .
      + '$';

    // Use str.contains would not be exact — we need full match
    // Build a custom expression using col().str accessor if available
    // For now, use a LikeExpr wrapper
    return new LikeExpr(columnName, regexStr);
  }
}

// ── Custom expression for LIKE ───────────────────────────────────────

class LikeExpr extends Expr<boolean> {
  private readonly _columnName: string;
  private readonly _regexStr: string;

  constructor(columnName: string, regexStr: string) {
    super();
    this._columnName = columnName;
    this._regexStr = regexStr;
  }

  get dependencies(): string[] {
    return [this._columnName];
  }

  evaluate(df: DataFrame): Series<boolean> {
    const series = df.col(this._columnName);
    const regex = new RegExp(this._regexStr);
    const results: (boolean | null)[] = [];
    for (let i = 0; i < series.length; i++) {
      const val = series.column.get(i);
      if (val === null) {
        results.push(null);
      } else {
        results.push(regex.test(typeof val === 'string' ? val : `${val as number}`));
      }
    }
    return new Series<boolean>('like_result', BooleanColumn.from(results));
  }

  toString(): string {
    return `LIKE(${this._columnName}, ${this._regexStr})`;
  }
}

// ── Custom expression for IS NULL / IS NOT NULL ──────────────────────

class IsNullExpr extends Expr<boolean> {
  private readonly _columnName: string;
  private readonly _invert: boolean;

  constructor(columnName: string, invert: boolean) {
    super();
    this._columnName = columnName;
    this._invert = invert;
  }

  get dependencies(): string[] {
    return [this._columnName];
  }

  evaluate(df: DataFrame): Series<boolean> {
    const series = df.col(this._columnName);
    return this._invert ? series.isNotNull() : series.isNull();
  }

  toString(): string {
    return `${this._columnName} IS ${this._invert ? 'NOT ' : ''}NULL`;
  }
}

// ── Query executor ───────────────────────────────────────────────────

type DataFrameType = DataFrame<Record<string, unknown>>;

function buildAggSpec(
  selectItems: SelectItem[],
  groupByColumns: string[],
): Record<string, AggExpr<unknown>> {
  const specs: Record<string, AggExpr<unknown>> = {};

  for (const item of selectItems) {
    if (groupByColumns.includes(item.column)) continue;
    if (item.aggregate === undefined) continue;

    const alias = item.alias ?? (item.column === '*'
      ? `${item.aggregate.toLowerCase()}`
      : `${item.aggregate.toLowerCase()}_${item.column}`);

    const sourceCol = item.column === '*' ? groupByColumns[0]! : item.column;
    const colRef = col(sourceCol);

    switch (item.aggregate) {
      case 'SUM': specs[alias] = colRef.sum() as AggExpr<unknown>; break;
      case 'AVG': specs[alias] = colRef.mean() as AggExpr<unknown>; break;
      case 'COUNT': specs[alias] = colRef.count() as AggExpr<unknown>; break;
      case 'MIN': specs[alias] = colRef.min() as AggExpr<unknown>; break;
      case 'MAX': specs[alias] = colRef.max() as AggExpr<unknown>; break;
    }
  }

  return specs;
}

export function executeQuery(df: DataFrameType, queryStr: string): DataFrameType {
  const tokens = tokenize(queryStr);
  const parser = new Parser(tokens);
  const query = parser.parse();

  let result: DataFrameType = df;

  // 1. Apply WHERE
  if (query.whereExpr !== undefined) {
    result = result.filter(query.whereExpr);
  }

  // 2. GROUP BY + aggregation
  if (query.groupByColumns.length > 0) {
    const aggSpec = buildAggSpec(query.selectItems, query.groupByColumns);
    const group = result.groupBy(...(query.groupByColumns as [string, ...string[]]));
    result = group.agg(aggSpec);

    // 3. Apply HAVING (filters on aggregated result)
    if (query.havingExpr !== undefined) {
      result = result.filter(query.havingExpr);
    }

    // 4. SELECT columns (after aggregation)
    if (!query.selectAll) {
      const selectedCols: string[] = [];
      for (const item of query.selectItems) {
        if (item.aggregate !== undefined) {
          const alias = item.alias ?? (item.column === '*'
            ? `${item.aggregate.toLowerCase()}`
            : `${item.aggregate.toLowerCase()}_${item.column}`);
          selectedCols.push(alias);
        } else {
          selectedCols.push(item.alias ?? item.column);
        }
      }
      result = result.select(...selectedCols);
    }
  } else {
    // No GROUP BY — simple SELECT
    if (!query.selectAll) {
      const columnNames = query.selectItems.map((item) => item.column);
      result = result.select(...columnNames);

      // Handle aliases via rename
      const renameMap: Record<string, string> = {};
      for (const item of query.selectItems) {
        if (item.alias !== undefined) {
          renameMap[item.column] = item.alias;
        }
      }
      if (Object.keys(renameMap).length > 0) {
        result = result.rename(renameMap);
      }
    }
  }

  // 5. ORDER BY
  if (query.orderByItems.length > 0) {
    const columns = query.orderByItems.map((item) => item.column);
    const orders = query.orderByItems.map((item) => item.direction);
    result = result.sortBy(columns, orders);
  }

  // 6. LIMIT
  if (query.limit !== undefined) {
    result = result.head(query.limit);
  }

  return result;
}
