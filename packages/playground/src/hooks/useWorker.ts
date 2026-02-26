import { useEffect, useRef, useCallback } from 'react';
import type { WorkerIncomingMessage, WorkerOutgoingMessage } from '../types/playground';

type MessageHandler = (msg: WorkerIncomingMessage) => void;

export function useWorker(onMessage: MessageHandler) {
  const workerRef = useRef<Worker | null>(null);
  const handlerRef = useRef<MessageHandler>(onMessage);

  // Keep handler ref up to date without recreating worker
  useEffect(() => {
    handlerRef.current = onMessage;
  }, [onMessage]);

  useEffect(() => {
    const worker = new Worker(new URL('../workers/executor.worker.ts', import.meta.url), {
      type: 'module',
    });

    worker.onmessage = (event: MessageEvent<WorkerIncomingMessage>) => {
      handlerRef.current(event.data);
    };

    workerRef.current = worker;

    return () => {
      worker.terminate();
      workerRef.current = null;
    };
  }, []);

  const postMessage = useCallback((msg: WorkerOutgoingMessage) => {
    workerRef.current?.postMessage(msg);
  }, []);

  return { postMessage };
}
