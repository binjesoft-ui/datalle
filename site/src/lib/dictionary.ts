export interface Column { name: string; type: string; comment: string }
export interface Table { name: string; layer: string; comment: string; rows: number | null; columns: Column[] }
export interface Edge { from: string; to: string }
export interface Node { name: string; layer: string }

export const LAYERS = ['bronze', 'silver', 'gold'] as const;

export function esc(s: unknown): string {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]!));
}

/** Everything upstream and downstream of a table (transitively), not including the table itself. */
export function related(name: string, edges: Edge[]): { up: Set<string>; down: Set<string> } {
  const walk = (start: string, next: (e: Edge) => [string, string]) => {
    const seen = new Set<string>();
    const queue = [start];
    while (queue.length) {
      const cur = queue.pop()!;
      for (const e of edges) {
        const [a, b] = next(e);
        if (a === cur && !seen.has(b) && b !== start) { seen.add(b); queue.push(b); }
      }
    }
    return seen;
  };
  return { up: walk(name, (e) => [e.to, e.from]), down: walk(name, (e) => [e.from, e.to]) };
}

/** Case-insensitive match on table name/description or any column name/description. */
export function filterTables(tables: Table[], query: string): Table[] {
  const q = query.trim().toLowerCase();
  if (!q) return tables;
  return tables.filter((t) =>
    t.name.toLowerCase().includes(q) || t.comment.toLowerCase().includes(q) ||
    t.columns.some((c) => c.name.toLowerCase().includes(q) || c.comment.toLowerCase().includes(q)));
}

export function columnsByLayer(nodes: Node[]): Record<string, Node[]> {
  const out: Record<string, Node[]> = { bronze: [], silver: [], gold: [] };
  for (const n of nodes) (out[n.layer] ??= []).push(n);
  return out;
}

export function undocumentedCount(tables: Table[]): number {
  return tables.reduce((n, t) => n + t.columns.filter((c) => !c.comment).length, 0);
}
