import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { execFile } from 'child_process';
import { promisify } from 'util';
import * as fs from 'fs/promises';
import * as path from 'path';

const exec = promisify(execFile);
const fixturesDir = path.join(__dirname, '..', 'fixtures');
const csvFixture = path.join(fixturesDir, 'cli-test.csv');
const jsonFixture = path.join(fixturesDir, 'cli-test.json');
const ndjsonFixture = path.join(fixturesDir, 'cli-test.ndjson');
const cliPath = path.join(__dirname, '..', '..', 'dist', 'cli.js');

describe('CLI tool', () => {
  beforeAll(async () => {
    // Create test fixtures
    await fs.writeFile(
      csvFixture,
      'name,amount,category\nAlice,150,A\nBob,50,B\nCharlie,200,A\nDiana,80,B\nEve,300,A\n',
    );
    await fs.writeFile(
      jsonFixture,
      JSON.stringify([
        { name: 'Alice', amount: 150, category: 'A' },
        { name: 'Bob', amount: 50, category: 'B' },
        { name: 'Charlie', amount: 200, category: 'A' },
      ]),
    );
    await fs.writeFile(
      ndjsonFixture,
      '{"name":"Alice","amount":150}\n{"name":"Bob","amount":50}\n{"name":"Charlie","amount":200}\n',
    );
  });

  afterAll(async () => {
    await fs.unlink(csvFixture).catch(() => {});
    await fs.unlink(jsonFixture).catch(() => {});
    await fs.unlink(ndjsonFixture).catch(() => {});
    // Clean up any output files
    await fs.unlink(path.join(fixturesDir, 'cli-output.csv')).catch(() => {});
  });

  it('should show help with --help', async () => {
    const { stdout } = await exec('node', [cliPath, '--help']);
    expect(stdout).toContain('Usage:');
    expect(stdout).toContain('framekit query');
  });

  it('should SELECT * FROM csv file', async () => {
    const { stdout } = await exec('node', [cliPath, 'query', `SELECT * FROM ${csvFixture}`]);
    expect(stdout).toContain('Alice');
    expect(stdout).toContain('Bob');
    expect(stdout).toContain('Charlie');
  });

  it('should SELECT specific columns', async () => {
    const { stdout } = await exec('node', [
      cliPath,
      'query',
      `SELECT name,amount FROM ${csvFixture}`,
    ]);
    expect(stdout).toContain('Alice');
    expect(stdout).toContain('150');
    // Should not contain category column header in table output
    expect(stdout).not.toContain('category');
  });

  it('should filter with WHERE clause', async () => {
    const { stdout } = await exec('node', [
      cliPath,
      'query',
      `SELECT * FROM ${csvFixture} WHERE amount > 100`,
    ]);
    expect(stdout).toContain('Alice');
    expect(stdout).toContain('Charlie');
    expect(stdout).toContain('Eve');
    expect(stdout).not.toContain('Bob');
    expect(stdout).not.toContain('Diana');
  });

  it('should ORDER BY column', async () => {
    const { stdout } = await exec('node', [
      cliPath,
      'query',
      `SELECT * FROM ${csvFixture} ORDER BY amount DESC`,
      '--format',
      'csv',
    ]);
    const lines = stdout.trim().split('\n');
    // First data line should be Eve (300)
    expect(lines[1]).toContain('Eve');
    expect(lines[1]).toContain('300');
  });

  it('should apply LIMIT', async () => {
    const { stdout } = await exec('node', [
      cliPath,
      'query',
      `SELECT * FROM ${csvFixture} LIMIT 2`,
      '--format',
      'csv',
    ]);
    const lines = stdout.trim().split('\n');
    // Header + 2 data rows
    expect(lines.length).toBe(3);
  });

  it('should read JSON files', async () => {
    const { stdout } = await exec('node', [
      cliPath,
      'query',
      `SELECT * FROM ${jsonFixture}`,
    ]);
    expect(stdout).toContain('Alice');
    expect(stdout).toContain('Charlie');
  });

  it('should read NDJSON files', async () => {
    const { stdout } = await exec('node', [
      cliPath,
      'query',
      `SELECT * FROM ${ndjsonFixture}`,
    ]);
    expect(stdout).toContain('Alice');
    expect(stdout).toContain('Bob');
  });

  it('should output CSV with --format csv', async () => {
    const { stdout } = await exec('node', [
      cliPath,
      'query',
      `SELECT * FROM ${csvFixture} LIMIT 2`,
      '--format',
      'csv',
    ]);
    expect(stdout).toContain('name,amount,category');
    expect(stdout).toContain('Alice,150,A');
  });

  it('should output JSON with --format json', async () => {
    const { stdout } = await exec('node', [
      cliPath,
      'query',
      `SELECT * FROM ${csvFixture} LIMIT 1`,
      '--format',
      'json',
    ]);
    const parsed = JSON.parse(stdout) as { name: string }[];
    expect(parsed).toHaveLength(1);
    expect(parsed[0]!.name).toBe('Alice');
  });

  it('should output NDJSON with --format ndjson', async () => {
    const { stdout } = await exec('node', [
      cliPath,
      'query',
      `SELECT * FROM ${csvFixture} LIMIT 2`,
      '--format',
      'ndjson',
    ]);
    const lines = stdout.trim().split('\n');
    expect(lines.length).toBe(2);
    expect((JSON.parse(lines[0]!) as { name: string }).name).toBe('Alice');
  });

  it('should write to file with --output', async () => {
    const outFile = path.join(fixturesDir, 'cli-output.csv');
    await exec('node', [
      cliPath,
      'query',
      `SELECT * FROM ${csvFixture} LIMIT 2`,
      '--format',
      'csv',
      '--output',
      outFile,
    ]);
    const content = await fs.readFile(outFile, 'utf-8');
    expect(content).toContain('name,amount,category');
    expect(content).toContain('Alice,150,A');
  });

  it('should combine WHERE, ORDER BY, and LIMIT', async () => {
    const { stdout } = await exec('node', [
      cliPath,
      'query',
      `SELECT name,amount FROM ${csvFixture} WHERE amount > 100 ORDER BY amount LIMIT 2`,
      '--format',
      'csv',
    ]);
    const lines = stdout.trim().split('\n');
    // Header + 2 data rows
    expect(lines.length).toBe(3);
    // Sorted ascending, first should be Alice (150)
    expect(lines[1]).toContain('Alice');
    expect(lines[1]).toContain('150');
  });
});
