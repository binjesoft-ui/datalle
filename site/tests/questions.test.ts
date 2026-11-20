import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

import { parquetReadObjects } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';

import { ALL, aggregate, total } from '../src/lib/engine.ts';
import { normalise } from '../src/lib/load.ts';
import { QUESTIONS, holidayName } from '../src/lib/questions.ts';

async function readCube(name: string) {
  const buf = readFileSync(new URL(`../public/data/cube_${name}.parquet`, import.meta.url));
  const file = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
  return (await parquetReadObjects({ file, compressors })).map(normalise);
}

const data = { daily: await readCube('daily'), time: await readCube('time'), zone: await readCube('zone') };
const months = [...new Set(data.time.map((r: any) => r.month))].sort() as string[];
const all = { from: months[0], to: months[months.length - 1], borough: ALL, payment: ALL };

test('there are exactly 10 questions with unique ids, hints and a metric to try', () => {
  assert.equal(QUESTIONS.length, 10);
  assert.equal(new Set(QUESTIONS.map((q) => q.id)).size, 10);
  for (const q of QUESTIONS) {
    assert.ok(q.question.endsWith('?') || q.question.includes('?'), q.id);
    assert.ok(q.hint.length > 10, q.id);
    assert.ok(q.view.metric, q.id);
  }
});

test('every answer is real text: no NaN, undefined or empty values', () => {
  for (const q of QUESTIONS) {
    const a = q.answer(data);
    assert.ok(a.length > 40, q.id);
    assert.doesNotMatch(a, /NaN|undefined|null|Infinity|\[object/, `${q.id}: ${a}`);
  }
});

test('busiest hour answer matches an independent calculation', () => {
  const byHour = new Map<number, number>();
  for (const r of data.time as any[]) byHour.set(r.pickup_hour, (byHour.get(r.pickup_hour) ?? 0) + r.trips);
  const [bestHour, bestTrips] = [...byHour.entries()].sort((a, b) => b[1] - a[1])[0];
  const answer = QUESTIONS.find((q) => q.id === 'busiest-hour')!.answer(data);
  assert.ok(answer.includes(`${bestHour}:00`), answer);
  assert.ok(answer.includes(new Intl.NumberFormat('en-US').format(bestTrips)), answer);
});

test('payment shares add up to the whole', () => {
  const g = aggregate(data.time, (r: any) => r.month, (r: any) => r.payment as string, all);
  const sum = [...g.values()].reduce((s, a) => s + a.trips, 0);
  assert.equal(sum, total(data.time, (r: any) => r.month, all).trips);
});

test('quiet-days answer names the real minimum day', () => {
  const byDay = new Map<string, number>();
  for (const r of data.daily as any[]) byDay.set(r.pickup_date, (byDay.get(r.pickup_date) ?? 0) + r.trips);
  const [minDay, minTrips] = [...byDay.entries()].sort((a, b) => a[1] - b[1])[0];
  const answer = QUESTIONS.find((q) => q.id === 'quiet-days')!.answer(data);
  assert.ok(answer.includes(new Intl.NumberFormat('en-US').format(minTrips)), `${minDay} ${answer}`);
});

test('holiday detection follows the calendar rules', () => {
  assert.equal(holidayName('2024-12-25'), 'Christmas Day');
  assert.equal(holidayName('2024-07-04'), 'Independence Day');
  assert.equal(holidayName('2024-05-27'), 'Memorial Day'); // last Monday of May 2024
  assert.equal(holidayName('2024-05-20'), null);
  assert.equal(holidayName('2024-09-02'), 'Labor Day');
  assert.equal(holidayName('2024-11-28'), 'Thanksgiving');
  assert.equal(holidayName('2024-03-15'), null);
});
