import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

import { parquetReadObjects } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';

import { ALL, METRICS, aggregate, topN, total } from '../src/lib/engine.ts';
import { normalise } from '../src/lib/load.ts';

const base = { trips: 0, fare_sum: 0, total_sum: 0, distance_sum: 0, minutes_sum: 0, card_fare_sum: 0, card_tip_sum: 0 };
const rows = [
  { ...base, month: '2024-01', borough: 'Manhattan', payment: 'Credit card', trips: 2, fare_sum: 30, total_sum: 40, distance_sum: 7, minutes_sum: 30, card_fare_sum: 30, card_tip_sum: 6 },
  { ...base, month: '2024-01', borough: 'Manhattan', payment: 'Cash', trips: 1, fare_sum: 8, total_sum: 10, distance_sum: 1, minutes_sum: 8 },
  { ...base, month: '2024-02', borough: 'Queens', payment: 'Credit card', trips: 1, fare_sum: 60, total_sum: 80, distance_sum: 17, minutes_sum: 40, card_fare_sum: 60, card_tip_sum: 6 },
];
const all = { from: '2024-01', to: '2024-12', borough: ALL, payment: ALL };
const month = (r: any) => r.month;

test('filters by month range, borough and payment', () => {
  assert.equal(total(rows, month, all).trips, 4);
  assert.equal(total(rows, month, { ...all, to: '2024-01' }).trips, 3);
  assert.equal(total(rows, month, { ...all, borough: 'Queens' }).trips, 1);
  assert.equal(total(rows, month, { ...all, payment: 'Cash' }).trips, 1);
});

test('averages are ratios of sums for exactly the selection, not averages of averages', () => {
  const t = total(rows, month, all);
  assert.equal(METRICS.avg_fare.value(t), (30 + 8 + 60) / 4);
  assert.equal(METRICS.avg_distance.value(t), (7 + 1 + 17) / 4);
});

test('tip % uses card rows only and is null when there are no card trips', () => {
  assert.equal(METRICS.tip_pct.value(total(rows, month, all)), ((6 + 6) / (30 + 60)) * 100);
  assert.equal(METRICS.tip_pct.value(total(rows, month, { ...all, payment: 'Cash' })), null);
});

test('empty selection gives null ratios, not NaN', () => {
  const t = total(rows, month, { ...all, borough: 'Bronx' });
  assert.equal(METRICS.avg_fare.value(t), null);
});

test('topN ignores tiny groups for ratio metrics but not for counts', () => {
  const g = aggregate(rows, month, (r) => r.borough, all);
  assert.deepEqual(topN(g, 'avg_fare', 5, 2).map((e) => e.key), ['Manhattan']);
  assert.deepEqual(topN(g, 'trips', 5, 2).map((e) => e.key), ['Manhattan', 'Queens']);
});

// --- real data: the three cubes must describe exactly the same trips ---
async function readCube(name: string) {
  const buf = readFileSync(new URL(`../public/data/cube_${name}.parquet`, import.meta.url));
  const file = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
  return (await parquetReadObjects({ file, compressors })).map(normalise);
}

test('real cubes reconcile with each other and with every filter', async () => {
  const daily = await readCube('daily');
  const time = await readCube('time');
  const zone = await readCube('zone');
  const dayMonth = (r: any) => r.pickup_date.slice(0, 7);

  const a = total(daily, dayMonth, all).trips;
  assert.ok(a > 0);
  assert.equal(total(time, month, all).trips, a);
  assert.equal(total(zone, month, all).trips, a);

  for (const f of [
    { ...all, from: '2024-03', to: '2024-05' },
    { ...all, borough: 'Manhattan' },
    { ...all, payment: 'Credit card', from: '2024-06', to: '2024-06' },
  ]) {
    const d = total(daily, dayMonth, f).trips;
    assert.equal(total(time, month, f).trips, d);
    assert.equal(total(zone, month, f).trips, d);
  }
});

test('real data: types are plain numbers and ISO dates', async () => {
  const daily = await readCube('daily');
  assert.equal(typeof daily[0].trips, 'number');
  assert.match(daily[0].pickup_date, /^\d{4}-\d{2}-\d{2}$/);
});
