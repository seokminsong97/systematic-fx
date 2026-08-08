"use client";

import {
  type ColumnDef,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table";
import Link from "next/link";
import { useMemo, useState } from "react";

import { StatusBadge } from "@/components/ui/status-badge";
import type { Hypothesis, ResearchSnapshot } from "@/domain/research/types";
import { useLiveSnapshot } from "@/lib/use-live-snapshot";

export function HypothesisExplorer({ initialSnapshot, initialFamily = "ALL" }: { initialSnapshot: ResearchSnapshot; initialFamily?: string }) {
  const { data: snapshot = initialSnapshot } = useLiveSnapshot(initialSnapshot);
  const [query, setQuery] = useState("");
  const [family, setFamily] = useState(initialFamily);
  const [decision, setDecision] = useState("ALL");
  const [sorting, setSorting] = useState<SortingState>([{ id: "family", desc: false }]);
  const data = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    return snapshot.hypotheses.filter((item) => {
      const matchesQuery = !normalized || `${item.title} ${item.hypothesis} ${item.id}`.toLowerCase().includes(normalized);
      return matchesQuery && (family === "ALL" || item.family === family) && (decision === "ALL" || item.decision === decision);
    });
  }, [snapshot.hypotheses, query, family, decision]);
  const columns = useMemo<ColumnDef<Hypothesis>[]>(() => [
    { accessorKey: "family", header: "Family", cell: ({ row }) => <strong>{row.original.family}</strong> },
    {
      accessorKey: "title",
      header: "Hypothesis",
      cell: ({ row }) => (
        <Link href={`/hypotheses/${row.original.id}`}>
          <span className="table-title">{row.original.title}</span>
          <span className="table-subtitle">{row.original.id}</span>
        </Link>
      ),
    },
    { accessorKey: "modelFamily", header: "Model", cell: ({ row }) => <span className="table-subtitle">{row.original.modelFamily.replaceAll("_", " ")}</span> },
    { accessorKey: "direction", header: "Direction" },
    { accessorKey: "status", header: "Lifecycle", cell: ({ row }) => <StatusBadge value={row.original.status} kind="plain" /> },
    { accessorKey: "decision", header: "Evidence", cell: ({ row }) => <StatusBadge value={row.original.decision} kind="decision" /> },
    { id: "patterns", header: "Patterns", cell: ({ row }) => row.original.observedPatternIds.length.toLocaleString() },
    { accessorKey: "supportCount", header: "Support", cell: ({ row }) => row.original.supportCount.toLocaleString() },
  ], []);
  // TanStack Table intentionally returns callable state that React Compiler does not memoize.
  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({ data, columns, state: { sorting }, onSortingChange: setSorting, getCoreRowModel: getCoreRowModel(), getSortedRowModel: getSortedRowModel() });
  const decisions = Array.from(new Set(snapshot.hypotheses.map((item) => item.decision)));
  return (
    <>
      <div className="toolbar">
        <input className="field" aria-label="Search hypotheses" placeholder="Search title, claim, or identifier…" value={query} onChange={(event) => setQuery(event.target.value)} />
        <select className="field" aria-label="Filter by family" value={family} onChange={(event) => setFamily(event.target.value)}>
          <option value="ALL">All families</option>
          {snapshot.families.map((item) => <option key={item.id} value={item.id}>{item.id} · {item.title}</option>)}
        </select>
        <select className="field" aria-label="Filter by evidence state" value={decision} onChange={(event) => setDecision(event.target.value)}>
          <option value="ALL">All evidence states</option>
          {decisions.map((item) => <option key={item} value={item}>{item.replaceAll("_", " ")}</option>)}
        </select>
      </div>
      <div className="data-table-wrap">
        <table className="data-table">
          <thead><tr>{table.getHeaderGroups()[0]?.headers.map((header) => <th key={header.id}><button onClick={header.column.getToggleSortingHandler()}>{flexRender(header.column.columnDef.header, header.getContext())}{header.column.getIsSorted() === "asc" ? " ↑" : header.column.getIsSorted() === "desc" ? " ↓" : ""}</button></th>)}</tr></thead>
          <tbody>
            {table.getRowModel().rows.length > 0
              ? table.getRowModel().rows.map((row) => <tr key={row.id}>{row.getVisibleCells().map((cell) => <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}</tr>)
              : <tr><td colSpan={columns.length} className="table-empty">No hypotheses match the current filters.</td></tr>}
          </tbody>
        </table>
      </div>
      <p className="panel-caption" style={{ marginTop: 12 }}>{data.length} of {snapshot.hypotheses.length} hypotheses shown · revision {snapshot.metadata.revision}</p>
    </>
  );
}
