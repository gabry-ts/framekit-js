import { useMemo } from 'react';
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getPaginationRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
} from '@tanstack/react-table';
import { useState } from 'react';
import type { SchemaField } from '../../types/playground';
import './DataTable.css';

interface DataTableProps {
  rows: Record<string, unknown>[];
  schema: SchemaField[];
  executionTime: number | null;
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return 'null';
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'bigint') return value.toString();
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

function isCellNull(value: unknown): boolean {
  return value === null || value === undefined;
}

const PAGE_SIZE = 100;

export function DataTable({ rows, schema, executionTime }: DataTableProps) {
  const [sorting, setSorting] = useState<SortingState>([]);

  const columns = useMemo<ColumnDef<Record<string, unknown>>[]>(() => {
    if (schema.length === 0) return [];
    return schema.map((field) => ({
      accessorKey: field.name,
      header: () => (
        <div className="dt-header-cell">
          <span className="dt-col-name">{field.name}</span>
          <span className="dt-col-dtype">{field.dtype}</span>
        </div>
      ),
      cell: ({ getValue }) => {
        const val = getValue();
        const isNull = isCellNull(val);
        return <span className={isNull ? 'dt-cell-null' : undefined}>{formatCell(val)}</span>;
      },
    }));
  }, [schema]);

  const table = useReactTable({
    data: rows,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: { pagination: { pageSize: PAGE_SIZE } },
  });

  if (schema.length === 0) {
    return (
      <div className="dt-empty">
        <p>Run code to see results here.</p>
        <p className="dt-empty-hint">
          Return a <code>DataFrame</code> from your code to display it.
        </p>
      </div>
    );
  }

  const { pageIndex, pageSize } = table.getState().pagination;
  const totalRows = rows.length;
  const from = pageIndex * pageSize + 1;
  const to = Math.min((pageIndex + 1) * pageSize, totalRows);

  return (
    <div className="dt-wrapper">
      <div className="dt-meta">
        <span>
          {totalRows.toLocaleString()} rows × {schema.length} cols
        </span>
        {executionTime !== null && (
          <span className="dt-exec-time">{executionTime.toFixed(1)}ms</span>
        )}
      </div>
      <div className="dt-scroll">
        <table className="dt-table">
          <thead>
            {table.getHeaderGroups().map((hg) => (
              <tr key={hg.id}>
                {hg.headers.map((header) => (
                  <th
                    key={header.id}
                    onClick={header.column.getToggleSortingHandler()}
                    className={header.column.getCanSort() ? 'dt-sortable' : undefined}
                  >
                    {flexRender(header.column.columnDef.header, header.getContext())}
                    {header.column.getIsSorted() === 'asc' && (
                      <span className="dt-sort-icon"> ↑</span>
                    )}
                    {header.column.getIsSorted() === 'desc' && (
                      <span className="dt-sort-icon"> ↓</span>
                    )}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((row) => (
              <tr key={row.id}>
                {row.getVisibleCells().map((cell) => (
                  <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {totalRows > PAGE_SIZE && (
        <div className="dt-pagination">
          <button onClick={() => table.previousPage()} disabled={!table.getCanPreviousPage()}>
            ← Prev
          </button>
          <span>
            {from}–{to} of {totalRows.toLocaleString()}
          </span>
          <button onClick={() => table.nextPage()} disabled={!table.getCanNextPage()}>
            Next →
          </button>
        </div>
      )}
    </div>
  );
}
