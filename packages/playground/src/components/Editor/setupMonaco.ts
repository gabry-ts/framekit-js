import type * as Monaco from 'monaco-editor';

// Strip Node.js imports from browser.d.ts before injecting into Monaco
function sanitizeDts(dts: string): string {
  return dts
    .replace(/^import\s.*?from\s+['"]stream['"]\s*;?\s*$/gm, '')
    .replace(/^import\s.*?from\s+['"]node:[^'"]+['"]\s*;?\s*$/gm, '')
    .replace(/stream\.Readable/g, 'unknown')
    .replace(/stream\.Writable/g, 'unknown');
}

/**
 * Called in `beforeMount` — runs before any editor model is created,
 * so compiler options are in place before the TS worker starts.
 */
export function configureMonacoCompilerOptions(monaco: typeof Monaco): void {
  const ts = monaco.typescript;

  ts.typescriptDefaults.setCompilerOptions({
    target: ts.ScriptTarget.ES2020,
    moduleResolution: ts.ModuleResolutionKind.NodeJs,
    module: ts.ModuleKind.ESNext,
    allowSyntheticDefaultImports: true,
    strict: false,
    noUnusedLocals: false,
    noUnusedParameters: false,
  });

  // Disable Monaco's TS diagnostics — the playground wraps user code in
  // `async function __run__()` at execution time, so top-level `return` and
  // `await` are intentionally valid. Real errors surface via esbuild in the Console.
  ts.typescriptDefaults.setDiagnosticsOptions({
    noSemanticValidation: true,
    noSyntaxValidation: true,
  });

  // Add globals so user code doesn't need to import anything
  ts.typescriptDefaults.addExtraLib(
    `
    declare const DataFrame: any;
    declare const Series: any;
    declare const col: any;
    declare const lit: any;
    declare const when: any;
    declare const df: any;
    declare const DType: any;
    `,
    'file:///framekit-globals.d.ts',
  );
}

/**
 * Called in `onMount` (async) — fetches and injects proper FrameKit types.
 * Replaces the placeholder `any` declarations above with real types.
 */
export async function injectFrameKitTypes(monaco: typeof Monaco): Promise<void> {
  let dts = '';
  try {
    // browser.d.ts is in ../../dist, resolved relative to this file at build time
    const url = new URL('../../../../dist/browser.d.ts', import.meta.url).href;
    const res = await fetch(url);
    if (res.ok) dts = await res.text();
  } catch {
    return; // silently skip — placeholder declarations are still active
  }

  if (!dts) return;

  const sanitized = sanitizeDts(dts);
  const ts = monaco.typescript;

  ts.typescriptDefaults.addExtraLib(
    `declare module 'framekit-js' {\n${sanitized}\n}`,
    'file:///node_modules/framekit-js/index.d.ts',
  );

  // Re-declare globals with proper types once the real .d.ts is available
  ts.typescriptDefaults.addExtraLib(
    `
    import * as fk from 'framekit-js';
    declare global {
      const DataFrame: typeof fk.DataFrame;
      const Series: typeof fk.Series;
      const col: typeof fk.col;
      const lit: typeof fk.lit;
      const when: typeof fk.when;
      const df: typeof fk.df;
      const DType: typeof fk.DType;
    }
    `,
    'file:///framekit-globals.d.ts',
  );
}
