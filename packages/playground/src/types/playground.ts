export type WorkerStatus = 'idle' | 'running' | 'success' | 'error';

export interface SchemaField {
  name: string;
  dtype: string;
}

export interface ExecutorState {
  status: WorkerStatus;
  rows: Record<string, unknown>[];
  schema: SchemaField[];
  logs: LogEntry[];
  error: string | null;
  executionTime: number | null;
}

export interface LogEntry {
  timestamp: number;
  level: 'log' | 'warn' | 'error' | 'info';
  args: unknown[];
}

// Messages: main → worker
export interface WorkerRunMessage {
  type: 'run';
  code: string;
}

// Messages: worker → main
export interface WorkerSuccessMessage {
  type: 'success';
  rows: Record<string, unknown>[];
  schema: SchemaField[];
  logs: LogEntry[];
  executionTime: number;
}

export interface WorkerErrorMessage {
  type: 'error';
  error: string;
  logs: LogEntry[];
  executionTime: number;
}

export type WorkerIncomingMessage = WorkerSuccessMessage | WorkerErrorMessage;
export type WorkerOutgoingMessage = WorkerRunMessage;

export interface Snippet {
  category: string;
  label: string;
  code: string;
}

export interface Dataset {
  label: string;
  filename: string;
  description: string;
  defaultCode: string;
}
