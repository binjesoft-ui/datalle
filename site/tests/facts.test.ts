import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, statSync } from 'node:fs';
import { parquetMetadata } from 'hyparquet';
import { FACTS as F } from '../src/lib/facts.ts';

const data = (f: string) => new URL(`../public/data/${f}`, import.meta.url);
const json = (f: string) => JSON.parse(readFileSync(data(f), 'utf8'));
const rows = (name: string) => json('data_dictionary.json').tables.find((t: any) => t.name === name).rows;

test('row counts match the Unity Catalog export and reconcile exactly', () => {
  assert.equal(rows('bronze_yellow_trips'), F.bronzeRows);
  assert.equal(rows('silver_yellow_trips'), F.cleanRows);
  assert.equal(rows('silver_yellow_quarantine'), F.quarantinedRows);
  assert.equal(F.cleanRows + F.quarantinedRows, F.evaluatedRows);
  assert.equal(F.evaluatedRows + F.duplicatesRemoved, F.bronzeRows);
  assert.equal(Math.round((F.quarantinedRows / F.evaluatedRows) * 1000) / 10, F.quarantinePct);
});

test('rule figures match the quality report and the pipeline', () => {
  const q = json('quality_report.json').data;
  assert.equal(Number(q[0].total_rows), F.evaluatedRows);
  const p = q.find((r: any) => r.rule === 'passenger_count_1_to_6');
  assert.equal(Number(p.rows_failed), F.passengerFailed);
  assert.equal(Math.round(p.pct_of_all_rows * 10) / 10, F.passengerPct);
  assert.equal(json('pipeline_expectations.json').expectations.length, F.rulesCount);
});

test('cube size and row count match the Parquet files', () => {
  let bytes = 0, total = 0;
  for (const f of ['cube_daily.parquet', 'cube_time.parquet', 'cube_zone.parquet']) {
    const buf = readFileSync(data(f));
    bytes += statSync(data(f)).size;
    total += Number(parquetMetadata(buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength)).num_rows);
  }
  assert.equal(total, F.cubeRows);
  assert.equal(Math.round(bytes / 1e5) / 10, F.cubeMB);
});
