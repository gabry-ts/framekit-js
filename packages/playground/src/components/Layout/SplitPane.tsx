import {
  useState,
  useRef,
  useCallback,
  type ReactNode,
  type MouseEvent as ReactMouseEvent,
} from 'react';
import './SplitPane.css';

interface SplitPaneProps {
  left: ReactNode;
  right: ReactNode;
  defaultSplit?: number; // percentage 0-100
  minLeft?: number;
  minRight?: number;
  direction?: 'horizontal' | 'vertical';
}

export function SplitPane({
  left,
  right,
  defaultSplit = 50,
  minLeft = 15,
  minRight = 15,
  direction = 'horizontal',
}: SplitPaneProps) {
  const [split, setSplit] = useState(defaultSplit);
  const containerRef = useRef<HTMLDivElement>(null);
  const isDragging = useRef(false);

  const startDrag = useCallback(
    (e: ReactMouseEvent<HTMLDivElement>) => {
      e.preventDefault();
      isDragging.current = true;
      const container = containerRef.current;
      if (!container) return;

      const onMove = (moveEvent: globalThis.MouseEvent) => {
        if (!isDragging.current) return;
        const rect = container.getBoundingClientRect();
        let pct: number;
        if (direction === 'horizontal') {
          pct = ((moveEvent.clientX - rect.left) / rect.width) * 100;
        } else {
          pct = ((moveEvent.clientY - rect.top) / rect.height) * 100;
        }
        const clamped = Math.max(minLeft, Math.min(100 - minRight, pct));
        setSplit(clamped);
      };

      const onUp = () => {
        isDragging.current = false;
        window.removeEventListener('mousemove', onMove);
        window.removeEventListener('mouseup', onUp);
      };

      window.addEventListener('mousemove', onMove);
      window.addEventListener('mouseup', onUp);
    },
    [direction, minLeft, minRight],
  );

  const isH = direction === 'horizontal';

  return (
    <div ref={containerRef} className={`split-pane split-pane-${direction}`}>
      <div
        className="split-pane-panel"
        style={isH ? { width: `${split}%` } : { height: `${split}%` }}
      >
        {left}
      </div>
      <div
        className={`split-pane-divider split-pane-divider-${direction}`}
        onMouseDown={startDrag}
      />
      <div className="split-pane-panel split-pane-panel-fill">{right}</div>
    </div>
  );
}
