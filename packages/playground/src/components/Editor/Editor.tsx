import { useRef, useCallback } from 'react';
import MonacoEditor, { type BeforeMount, type OnMount } from '@monaco-editor/react';
import { configureMonacoCompilerOptions, injectFrameKitTypes } from './setupMonaco';

const DEFAULT_CODE = `// FrameKit Playground
// DataFrame, col, lit, when, df, DType are available globally
// Use Cmd+Enter / Ctrl+Enter to run

const result = DataFrame.fromColumns({
  name: ['Alice', 'Bob', 'Charlie', 'Diana'],
  age: [28, 35, 22, 41],
  score: [88.5, 92.0, 78.3, 95.1],
});

const filtered = result.filter(col('age').gt(25));
filtered.print();

// Return a DataFrame to display it in the table
return filtered;
`;

interface EditorProps {
  code: string;
  onChange: (code: string) => void;
  onRun: () => void;
}

export function Editor({ code, onChange, onRun }: EditorProps) {
  const onRunRef = useRef(onRun);
  onRunRef.current = onRun;

  // beforeMount: runs before any model is created — set compiler options here
  const handleBeforeMount: BeforeMount = useCallback((monaco) => {
    configureMonacoCompilerOptions(monaco);
  }, []);

  // onMount: editor is ready — inject full FrameKit types async
  const handleMount: OnMount = useCallback((editor, monaco) => {
    void injectFrameKitTypes(monaco);

    // Cmd+Enter / Ctrl+Enter to run
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
      onRunRef.current();
    });
  }, []);

  return (
    <MonacoEditor
      height="100%"
      language="typescript"
      path="file:///playground.ts"
      value={code}
      theme="vs-dark"
      onChange={(value) => onChange(value ?? '')}
      beforeMount={handleBeforeMount}
      onMount={handleMount}
      options={{
        fontSize: 14,
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        wordWrap: 'on',
        lineNumbers: 'on',
        renderLineHighlight: 'line',
        tabSize: 2,
        insertSpaces: true,
        automaticLayout: true,
        padding: { top: 12, bottom: 12 },
      }}
    />
  );
}

export { DEFAULT_CODE };
