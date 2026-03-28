import type { ReactNode } from "react";

interface ResultsTableColumn<Row> {
  key: string;
  header: string;
  align?: "left" | "right";
  render: (row: Row) => string | number | null | undefined | ReactNode;
}

interface ResultsTableProps<Row> {
  rows: Row[];
  columns: ResultsTableColumn<Row>[];
  emptyText?: string;
  getRowClassName?: (row: Row, index: number) => string;
}

export function ResultsTable<Row>({ rows, columns, emptyText = "No rows available yet.", getRowClassName }: ResultsTableProps<Row>) {
  if (!rows.length) {
    return <div className="rounded-[1.6rem] border border-dashed border-border/80 bg-ink/30 p-8 text-sm text-muted">{emptyText}</div>;
  }

  return (
    <div className="overflow-hidden rounded-[1.6rem] border border-border/80 bg-[linear-gradient(180deg,rgba(9,13,22,0.92),rgba(16,21,34,0.92))] shadow-[inset_0_1px_0_rgba(255,255,255,0.03),0_20px_40px_rgba(0,0,0,0.18)]">
      <div className="max-h-[32rem] overflow-auto">
        <table className="min-w-full border-collapse text-sm">
          <thead className="sticky top-0 z-10 bg-panel/95 backdrop-blur-xl">
            <tr>
              {columns.map((column) => (
                <th key={column.key} className={`border-b border-border/80 px-4 py-3 text-xs font-semibold uppercase tracking-[0.16em] text-muted ${column.align === "right" ? "text-right" : "text-left"}`}>
                  {column.header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, rowIndex) => (
              <tr key={rowIndex} className={`border-b border-border/70 transition hover:bg-white/[0.03] last:border-b-0 ${getRowClassName ? getRowClassName(row, rowIndex) : ""}`.trim()}>
                {columns.map((column) => {
                  const value = column.render(row);
                  return (
                    <td key={column.key} className={`max-w-[16rem] px-4 py-3.5 text-slate-100 ${column.align === "right" ? "text-right tabular-nums" : "text-left"}`}>
                      <div className="truncate whitespace-nowrap md:whitespace-normal md:break-words">{value ?? "--"}</div>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
