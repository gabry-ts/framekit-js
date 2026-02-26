import { useState } from 'react';
import type { Snippet } from '../../types/playground';

interface SnippetListProps {
  snippets: Snippet[];
  onSelect: (code: string) => void;
}

export function SnippetList({ snippets, onSelect }: SnippetListProps) {
  const [expanded, setExpanded] = useState<Set<string>>(() => {
    const cats = new Set<string>();
    if (snippets[0]) cats.add(snippets[0].category);
    return cats;
  });

  const grouped = snippets.reduce<Record<string, Snippet[]>>((acc, s) => {
    if (!acc[s.category]) acc[s.category] = [];
    acc[s.category].push(s);
    return acc;
  }, {});

  const toggleCategory = (cat: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat);
      else next.add(cat);
      return next;
    });
  };

  return (
    <div className="snippet-list">
      {Object.entries(grouped).map(([category, items]) => (
        <div key={category} className="snippet-group">
          <button className="snippet-category" onClick={() => toggleCategory(category)}>
            <span className="snippet-caret">{expanded.has(category) ? '▾' : '▸'}</span>
            {category}
          </button>
          {expanded.has(category) && (
            <div className="snippet-items">
              {items.map((snippet) => (
                <button
                  key={snippet.label}
                  className="snippet-item"
                  onClick={() => onSelect(snippet.code)}
                  title={snippet.label}
                >
                  {snippet.label}
                </button>
              ))}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
