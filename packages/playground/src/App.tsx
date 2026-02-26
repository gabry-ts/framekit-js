import { useState, useCallback } from 'react';
import { Editor, DEFAULT_CODE } from './components/Editor/Editor';
import { DataTable } from './components/DataTable/DataTable';
import { Console } from './components/Console/Console';
import { Sidebar } from './components/Sidebar/Sidebar';
import { SplitPane } from './components/Layout/SplitPane';
import { useExecutor } from './hooks/useExecutor';
import type { Dataset } from './types/playground';
import './App.css';

type BottomTab = 'table' | 'console';

export default function App() {
  const [code, setCode] = useState(DEFAULT_CODE);
  const [bottomTab, setBottomTab] = useState<BottomTab>('table');
  const { state, run } = useExecutor();

  const handleRun = useCallback(() => {
    run(code);
    // Auto-switch to console if there's likely logs, table otherwise
    // We'll stay on current tab; let user switch manually
  }, [code, run]);

  const handleSnippetSelect = useCallback((snippetCode: string) => {
    setCode(snippetCode);
  }, []);

  const handleDatasetLoad = useCallback((dataset: Dataset) => {
    setCode(dataset.defaultCode);
  }, []);

  const handleFileUpload = useCallback((content: string, filename: string) => {
    const isJson =
      filename.endsWith('.json') || filename.endsWith('.ndjson') || filename.endsWith('.jsonl');
    const varName = filename.replace(/[^a-zA-Z0-9]/g, '_').replace(/^_+|_+$/g, '');
    const parseMethod = isJson ? 'fromJSON' : 'fromCSV';

    const uploadCode = `// Uploaded: ${filename}
const ${varName}_text = ${JSON.stringify(content.slice(0, 2000))}${content.length > 2000 ? '... (truncated)' : ''};

// Parse the file
const result = await DataFrame.${parseMethod}(${varName}_text, { parse: 'string', parseNumbers: true });
console.log('Shape:', result.shape);
result.print();
return result;`;

    setCode(uploadCode);
  }, []);

  const handleCopy = useCallback(() => {
    void navigator.clipboard.writeText(code);
  }, [code]);

  const hasError = state.status === 'error';
  const isRunning = state.status === 'running';
  const consoleHasBadge = state.logs.length > 0 || hasError;

  const sidebar = (
    <Sidebar
      onSnippetSelect={handleSnippetSelect}
      onDatasetLoad={handleDatasetLoad}
      onFileUpload={handleFileUpload}
    />
  );

  const editorPanel = (
    <div className="editor-panel">
      <Editor code={code} onChange={setCode} onRun={handleRun} />
    </div>
  );

  const bottomPanel = (
    <div className="bottom-panel">
      <div className="bottom-tabs">
        <button
          className={`bottom-tab ${bottomTab === 'table' ? 'bottom-tab-active' : ''}`}
          onClick={() => setBottomTab('table')}
        >
          Table
          {state.rows.length > 0 && <span className="tab-badge">{state.rows.length}</span>}
        </button>
        <button
          className={`bottom-tab ${bottomTab === 'console' ? 'bottom-tab-active' : ''} ${hasError ? 'bottom-tab-error' : ''}`}
          onClick={() => setBottomTab('console')}
        >
          Console
          {consoleHasBadge && (
            <span className={`tab-badge ${hasError ? 'tab-badge-error' : ''}`}>
              {hasError ? '!' : state.logs.length}
            </span>
          )}
        </button>
      </div>
      <div className="bottom-content">
        {bottomTab === 'table' ? (
          <DataTable rows={state.rows} schema={state.schema} executionTime={state.executionTime} />
        ) : (
          <Console logs={state.logs} error={state.error} executionTime={state.executionTime} />
        )}
      </div>
    </div>
  );

  const mainContent = (
    <SplitPane
      left={editorPanel}
      right={bottomPanel}
      defaultSplit={60}
      direction="vertical"
      minLeft={20}
      minRight={20}
    />
  );

  return (
    <div className="app">
      <header className="app-header">
        <div className="app-header-left">
          <span className="app-logo">FrameKit</span>
          <span className="app-title">Playground</span>
        </div>
        <div className="app-header-right">
          {isRunning && <span className="app-running">Running...</span>}
          <button className="btn-secondary" onClick={handleCopy} title="Copy code">
            Copy
          </button>
          <button
            className="btn-primary"
            onClick={handleRun}
            disabled={isRunning}
            title="Run (Cmd+Enter)"
          >
            {isRunning ? 'Running...' : 'Run'}
          </button>
        </div>
      </header>
      <div className="app-body">
        <SplitPane
          left={sidebar}
          right={mainContent}
          defaultSplit={18}
          minLeft={14}
          minRight={40}
        />
      </div>
    </div>
  );
}
