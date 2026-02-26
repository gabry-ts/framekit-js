import { useRef, useEffect } from 'react';
import type { LogEntry } from '../../types/playground';
import './Console.css';

interface ConsoleProps {
  logs: LogEntry[];
  error: string | null;
  executionTime: number | null;
}

function formatArg(arg: unknown): string {
  if (arg === null) return 'null';
  if (arg === undefined) return 'undefined';
  if (typeof arg === 'string') return arg;
  if (typeof arg === 'bigint') return arg.toString() + 'n';
  if (arg instanceof Error) return `${arg.name}: ${arg.message}`;
  try {
    return JSON.stringify(arg, null, 2);
  } catch {
    return String(arg);
  }
}

function formatTimestamp(ms: number): string {
  return `+${ms.toFixed(1)}ms`;
}

const LEVEL_CLASS: Record<LogEntry['level'], string> = {
  log: 'log-level-log',
  info: 'log-level-info',
  warn: 'log-level-warn',
  error: 'log-level-error',
};

const LEVEL_PREFIX: Record<LogEntry['level'], string> = {
  log: '',
  info: '[info] ',
  warn: '[warn] ',
  error: '[error] ',
};

export function Console({ logs, error, executionTime }: ConsoleProps) {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs, error]);

  const isEmpty = logs.length === 0 && error === null;

  return (
    <div className="console-wrapper">
      <div className="console-header">
        <span>Console</span>
        {executionTime !== null && (
          <span className="console-exec-time">{executionTime.toFixed(1)}ms</span>
        )}
      </div>
      <div className="console-output">
        {isEmpty && (
          <div className="console-empty">
            No output. Use <code>console.log()</code> to print values.
          </div>
        )}
        {logs.map((entry, i) => (
          <div key={i} className={`console-entry ${LEVEL_CLASS[entry.level]}`}>
            <span className="console-ts">{formatTimestamp(entry.timestamp)}</span>
            <span className="console-msg">
              {LEVEL_PREFIX[entry.level]}
              {entry.args.map(formatArg).join(' ')}
            </span>
          </div>
        ))}
        {error !== null && (
          <div className="console-entry console-error-block">
            <span className="console-ts">error</span>
            <pre className="console-stack">{error}</pre>
          </div>
        )}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
