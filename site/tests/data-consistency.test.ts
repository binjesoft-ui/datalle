import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';

const read = (n: string) => JSON.parse(readFileSync(new URL(`../public/data/${n}.json`, import.meta.url), 'utf8'));
const both = existsSync(new URL('../public/data/pipeline_expectations.json', import.meta.url));

// The notebook (quality_rules.py) and the declarative pipeline implement the same rules independently.
// If both are published, their per-rule failure counts must agree exactly.
test('notebook and pipeline agree on every rule', { skip: !both }, () => {
  const nb = new Map(read('quality_report').data.map((r: any) => [r.rule, Number(r.rows_failed)]));
  const sdp = read('pipeline_expectations').expectations;
  for (const e of sdp) assert.equal(nb.get(e.rule) ?? 0, e.failed, e.rule);
  for (const rule of nb.keys()) assert.ok(sdp.some((e: any) => e.rule === rule), `${rule} missing from pipeline`);
});

test('quality report total equals passed + failed of every pipeline rule', { skip: !both }, () => {
  const total = Number(read('quality_report').data[0].total_rows);
  for (const e of read('pipeline_expectations').expectations) assert.equal(e.passed + e.failed, total, e.rule);
});
