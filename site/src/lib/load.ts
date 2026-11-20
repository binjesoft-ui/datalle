import { parquetReadObjects } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';

import type { Row } from './engine.ts';

/** Parquet int64 arrives as BigInt and dates as Date objects; convert to plain numbers/strings. */
export function normalise(row: Row): Row {
  const out: Row = {};
  for (const [k, v] of Object.entries(row)) {
    if (typeof v === 'bigint') out[k] = Number(v);
    else if (v instanceof Date) out[k] = v.toISOString().slice(0, 10);
    else out[k] = v;
  }
  return out;
}

export async function loadCube(url: string): Promise<Row[]> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Could not load ${url} (${res.status})`);
  const file = await res.arrayBuffer();
  const rows = await parquetReadObjects({ file, compressors });
  return rows.map(normalise);
}
