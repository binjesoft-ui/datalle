import { test } from 'node:test';
import assert from 'node:assert/strict';
import { related, filterTables, columnsByLayer, undocumentedCount, esc, type Table } from '../src/lib/dictionary.ts';

const edges = [
  { from: 'b', to: 's' }, { from: 's', to: 'g1' }, { from: 's', to: 'g2' }, { from: 'z', to: 'g1' },
];

test('related walks transitively in both directions', () => {
  const r = related('s', edges);
  assert.deepEqual([...r.up].sort(), ['b']);
  assert.deepEqual([...r.down].sort(), ['g1', 'g2']);
  assert.deepEqual([...related('g1', edges).up].sort(), ['b', 's', 'z']);
});

test('related survives cycles and unknown names', () => {
  const r = related('a', [{ from: 'a', to: 'b' }, { from: 'b', to: 'a' }]);
  assert.deepEqual([...r.down], ['b']);
  assert.equal(related('nope', edges).up.size, 0);
});

const tables: Table[] = [
  { name: 'gold_daily', layer: 'gold', comment: 'Per day', rows: 3, columns: [{ name: 'trips', type: 'bigint', comment: 'Count' }, { name: 'x', type: 'int', comment: '' }] },
  { name: 'bronze_raw', layer: 'bronze', comment: 'Raw', rows: null, columns: [{ name: 'tip_amount', type: 'double', comment: 'Tip in dollars' }] },
];

test('filterTables searches names, comments and columns', () => {
  assert.equal(filterTables(tables, '').length, 2);
  assert.deepEqual(filterTables(tables, 'TIP').map((t) => t.name), ['bronze_raw']);
  assert.deepEqual(filterTables(tables, 'per day').map((t) => t.name), ['gold_daily']);
  assert.equal(filterTables(tables, 'zzz').length, 0);
});

test('columnsByLayer always has the three layers and undocumentedCount counts blanks', () => {
  assert.deepEqual(Object.keys(columnsByLayer([])), ['bronze', 'silver', 'gold']);
  assert.equal(undocumentedCount(tables), 1);
});

test('esc neutralises html', () => {
  assert.equal(esc('<img onerror="x">'), '&lt;img onerror=&quot;x&quot;&gt;');
});
