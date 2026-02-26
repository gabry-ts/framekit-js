import { useRef } from 'react';
import type { ChangeEvent } from 'react';

interface FileUploadProps {
  onLoad: (content: string, filename: string) => void;
}

export function FileUpload({ onLoad }: FileUploadProps) {
  const inputRef = useRef<HTMLInputElement>(null);

  const handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
      const content = ev.target?.result;
      if (typeof content === 'string') {
        onLoad(content, file.name);
      }
    };
    reader.readAsText(file);
    // Reset so same file can be re-selected
    if (inputRef.current) inputRef.current.value = '';
  };

  return (
    <div className="file-upload">
      <label className="file-upload-btn" title="Upload CSV or JSON file">
        Upload file
        <input
          ref={inputRef}
          type="file"
          accept=".csv,.json,.ndjson,.jsonl"
          onChange={handleChange}
          style={{ display: 'none' }}
        />
      </label>
      <span className="file-upload-hint">CSV / JSON</span>
    </div>
  );
}
