import { SnippetList } from './SnippetList';
import { FileUpload } from './FileUpload';
import { snippets, datasets } from '../../snippets/index';
import type { Dataset } from '../../types/playground';
import './Sidebar.css';

interface SidebarProps {
  onSnippetSelect: (code: string) => void;
  onDatasetLoad: (dataset: Dataset) => void;
  onFileUpload: (content: string, filename: string) => void;
}

export function Sidebar({ onSnippetSelect, onDatasetLoad, onFileUpload }: SidebarProps) {
  return (
    <aside className="sidebar">
      <div className="sidebar-section">
        <div className="sidebar-section-title">Snippets</div>
        <SnippetList snippets={snippets} onSelect={onSnippetSelect} />
      </div>

      <div className="sidebar-divider" />

      <div className="sidebar-section">
        <div className="sidebar-section-title">Example Datasets</div>
        <div className="dataset-list">
          {datasets.map((dataset) => (
            <button
              key={dataset.filename}
              className="dataset-item"
              onClick={() => onDatasetLoad(dataset)}
              title={dataset.description}
            >
              <span className="dataset-label">{dataset.label}</span>
              <span className="dataset-desc">{dataset.description}</span>
            </button>
          ))}
        </div>
      </div>

      <div className="sidebar-divider" />

      <div className="sidebar-section">
        <div className="sidebar-section-title">Upload</div>
        <FileUpload onLoad={onFileUpload} />
      </div>
    </aside>
  );
}
