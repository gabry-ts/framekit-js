import * as esbuild from 'esbuild-wasm/esm/browser';
import esbuildWasmURL from 'esbuild-wasm/esbuild.wasm?url';
import type {
  WorkerOutgoingMessage,
  WorkerIncomingMessage,
  LogEntry,
  SchemaField,
} from '../types/playground';

// Capture import.meta.url here — this worker IS an ES module, so it's valid.
// We'll pass it to esbuild's `define` so user code can reference it too,
// even though the final execution runs inside new Function() (script context).
const WORKER_URL = import.meta.url;
const DATASETS_BASE = new URL('../datasets/', WORKER_URL).href;

// ─── esbuild init ─────────────────────────────────────────────────────────────

let esbuildReady = false;

async function ensureEsbuild(): Promise<void> {
  if (esbuildReady) return;
  await esbuild.initialize({ wasmURL: esbuildWasmURL, worker: false });
  esbuildReady = true;
}

// ─── FrameKit scope → worker globalThis ──────────────────────────────────────
//
// Injecting into self (= globalThis in a worker) means new Function() sees
// all FrameKit exports. User code like `const df = ...` creates a LOCAL
// binding inside the wrapper function — no conflict with globalThis.df.

let scopeReady = false;

async function ensureScope(): Promise<void> {
  if (scopeReady) return;
  const fk = (await import('framekit-js')) as Record<string, unknown>;
  // Also expose a helper so dataset snippets can resolve CSV paths
  Object.assign(self, fk, { __DATASETS_BASE__: DATASETS_BASE });
  scopeReady = true;
}

// ─── Log capture ─────────────────────────────────────────────────────────────

function withLogCapture<T>(
  fn: () => Promise<T>,
): Promise<{ logs: LogEntry[]; result: T | null; error: unknown }> {
  return new Promise((resolve) => {
    const logs: LogEntry[] = [];
    const orig = {
      log: console.log,
      warn: console.warn,
      error: console.error,
      info: console.info,
    };

    const capture =
      (level: LogEntry['level']) =>
      (...args: unknown[]) =>
        logs.push({ timestamp: performance.now(), level, args });

    console.log = capture('log');
    console.warn = capture('warn');
    console.error = capture('error');
    console.info = capture('info');

    const restore = () => Object.assign(console, orig);

    fn()
      .then((r) => {
        restore();
        resolve({ logs, result: r, error: null });
      })
      .catch((e: unknown) => {
        restore();
        resolve({ logs, result: null, error: e });
      });
  });
}

// ─── DataFrame duck-type ──────────────────────────────────────────────────────

interface FK_DataFrame {
  toArray(): Record<string, unknown>[];
  dtypes: Record<string, string>;
  columns: string[];
}

function isDataFrame(v: unknown): v is FK_DataFrame {
  return (
    v !== null &&
    typeof v === 'object' &&
    typeof (v as Record<string, unknown>)['toArray'] === 'function' &&
    Array.isArray((v as Record<string, unknown>)['columns'])
  );
}

function extractSchema(d: FK_DataFrame): SchemaField[] {
  return d.columns.map((name) => ({ name, dtype: String(d.dtypes[name] ?? 'unknown') }));
}

// ─── Transpile + execute ──────────────────────────────────────────────────────

async function runUserCode(
  code: string,
): Promise<{ rows: Record<string, unknown>[]; schema: SchemaField[] }> {
  // Strip module-level imports — FrameKit lives on globalThis already.
  const stripped = code.replace(/^\s*import\s[^;]+;?\s*$/gm, '');

  // Pre-wrap in async function BEFORE esbuild sees it so that:
  //   • top-level return → inside a function (valid)
  //   • top-level await  → inside an async function (valid)
  //   • const df = ...   → local binding, no conflict with globalThis.df
  const src = `async function __run__() {\n${stripped}\n}`;

  const { code: js } = await esbuild.transform(src, {
    loader: 'ts',
    format: 'esm',
    target: 'es2022',
    // Replace import.meta.url at compile-time so new Function() (script
    // context) doesn't throw "import.meta may only appear in a module".
    define: {
      'import.meta.url': JSON.stringify(WORKER_URL),
    },
  });

  // new Function() is scoped to the worker's globalThis → FrameKit exports
  // (DataFrame, col, lit, …) are visible without explicit parameter passing.
  // eslint-disable-next-line @typescript-eslint/no-implied-eval
  const returnValue = await new Function(`${js}\nreturn __run__();`)();

  if (isDataFrame(returnValue)) {
    return {
      rows: returnValue.toArray() as Record<string, unknown>[],
      schema: extractSchema(returnValue),
    };
  }
  return { rows: [], schema: [] };
}

// ─── Message handler ──────────────────────────────────────────────────────────

self.onmessage = async (event: MessageEvent<WorkerOutgoingMessage>) => {
  const msg = event.data;
  if (msg.type !== 'run') return;

  const start = performance.now();

  try {
    await ensureEsbuild();
    await ensureScope();
  } catch (err) {
    self.postMessage({
      type: 'error',
      error: `Init failed: ${String(err)}`,
      logs: [],
      executionTime: 0,
    } satisfies WorkerIncomingMessage);
    return;
  }

  const { logs, result, error } = await withLogCapture(() => runUserCode(msg.code));
  const elapsed = performance.now() - start;

  if (error !== null) {
    self.postMessage({
      type: 'error',
      error: error instanceof Error ? `${error.message}\n${error.stack ?? ''}` : String(error),
      logs,
      executionTime: elapsed,
    } satisfies WorkerIncomingMessage);
    return;
  }

  const { rows, schema } = result as { rows: Record<string, unknown>[]; schema: SchemaField[] };
  self.postMessage({
    type: 'success',
    rows,
    schema,
    logs,
    executionTime: elapsed,
  } satisfies WorkerIncomingMessage);
};
