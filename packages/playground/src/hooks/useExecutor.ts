import { useState, useCallback } from 'react';
import type { ExecutorState, WorkerIncomingMessage } from '../types/playground';
import { useWorker } from './useWorker';

const INITIAL_STATE: ExecutorState = {
  status: 'idle',
  rows: [],
  schema: [],
  logs: [],
  error: null,
  executionTime: null,
};

export function useExecutor() {
  const [state, setState] = useState<ExecutorState>(INITIAL_STATE);

  const handleWorkerMessage = useCallback((msg: WorkerIncomingMessage) => {
    if (msg.type === 'success') {
      setState({
        status: 'success',
        rows: msg.rows,
        schema: msg.schema,
        logs: msg.logs,
        error: null,
        executionTime: msg.executionTime,
      });
    } else {
      setState((prev: ExecutorState) => ({
        ...prev,
        status: 'error',
        logs: msg.logs,
        error: msg.error,
        executionTime: msg.executionTime,
      }));
    }
  }, []);

  const { postMessage } = useWorker(handleWorkerMessage);

  const run = useCallback(
    (code: string) => {
      setState((prev: ExecutorState) => ({
        ...prev,
        status: 'running',
        error: null,
        logs: [],
      }));
      postMessage({ type: 'run', code });
    },
    [postMessage],
  );

  const reset = useCallback(() => {
    setState(INITIAL_STATE);
  }, []);

  return { state, run, reset };
}
