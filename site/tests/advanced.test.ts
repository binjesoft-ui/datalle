import { test } from 'node:test';
import assert from 'node:assert/strict';
import { compareRules, benchLines, benchmarkIsTrustworthy, type SizeRow } from '../src/lib/advanced.ts';

test('compareRules treats a rule missing from the notebook report as zero failures', () => {
  const rows = compareRules(
    [{ rule: 'a', rows_failed: '5', pct_of_all_rows: 1 }],
    [{ rule: 'a', failed: 5, pct_failed: 1 }, { rule: 'zones', failed: 0, pct_failed: 0 }],
  );
  assert.deepEqual(rows.map((r) => r.match), [true, true]);
  assert.equal(rows[1].notebook, 0);
});

test('compareRules flags a disagreement', () => {
  const [r] = compareRules([{ rule: 'a', rows_failed: 5, pct_of_all_rows: 1 }], [{ rule: 'a', failed: 6, pct_failed: 1 }]);
  assert.equal(r.match, false);
});

const ok = (m: number) => ({ ok: true, median_s: m, runs_s: [m] });
const size = (months: number, spark: any, duckdb: any, agree = true): SizeRow =>
  ({ months, input_mb: 10 * months, rows: 1000, spark, duckdb, engines_agree: agree });

test('benchLines names the faster engine and survives a failed engine', () => {
  const lines = benchLines([
    size(1, ok(20), ok(2)), size(6, ok(10), ok(40)), size(12, ok(30), { ok: false, runs_s: [], error: 'OOM' }),
  ]);
  assert.equal(lines[0].ratio, 'DuckDB 10.0× faster');
  assert.equal(lines[1].ratio, 'Spark 4.0× faster');
  assert.equal(lines[2].duckdb, 'failed');
  assert.equal(lines[2].ratio, '–');
  assert.equal(lines[2].agree, null);   // nothing to compare when one engine failed
  assert.equal(lines[0].agree, true);
});

test('benchmark is untrustworthy if both engines ran but disagreed', () => {
  assert.equal(benchmarkIsTrustworthy([size(1, ok(1), ok(1))]), true);
  assert.equal(benchmarkIsTrustworthy([size(1, ok(1), ok(1), false)]), false);
  assert.equal(benchmarkIsTrustworthy([]), false);
});
