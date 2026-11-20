// Pure aggregation helpers for the interactive dashboard (no DOM, no network, easy to test).
//
// The cubes store SUMS and COUNTS. To answer any filter selection we add the matching rows up
// and only then divide, e.g. average fare = sum of fares / sum of trips for exactly that selection.

export const SUM_FIELDS = [
  'trips', 'fare_sum', 'total_sum', 'distance_sum', 'minutes_sum', 'card_fare_sum', 'card_tip_sum',
] as const;

export type Field = (typeof SUM_FIELDS)[number];
export type Acc = Record<Field, number>;
export type Row = Record<string, any>;
export type Filters = { from: string; to: string; borough: string; payment: string };

export const ALL = 'All';

export function emptyAcc(): Acc {
  return Object.fromEntries(SUM_FIELDS.map((f) => [f, 0])) as Acc;
}

export function addRow(acc: Acc, row: Row): void {
  for (const f of SUM_FIELDS) acc[f] += Number(row[f]) || 0;
}

/** True when a row's month is inside [from, to] and it matches the borough and payment filters. */
export function matches(month: string, row: Row, f: Filters): boolean {
  return (
    month >= f.from &&
    month <= f.to &&
    (f.borough === ALL || row.borough === f.borough) &&
    (f.payment === ALL || row.payment === f.payment)
  );
}

export function aggregate<K>(
  rows: Row[],
  monthOf: (r: Row) => string,
  keyOf: (r: Row) => K,
  f: Filters,
): Map<K, Acc> {
  const out = new Map<K, Acc>();
  for (const r of rows) {
    if (!matches(monthOf(r), r, f)) continue;
    const k = keyOf(r);
    let acc = out.get(k);
    if (!acc) out.set(k, (acc = emptyAcc()));
    addRow(acc, r);
  }
  return out;
}

export function total(rows: Row[], monthOf: (r: Row) => string, f: Filters): Acc {
  const acc = emptyAcc();
  for (const r of rows) if (matches(monthOf(r), r, f)) addRow(acc, r);
  return acc;
}

export type MetricId = 'trips' | 'revenue' | 'avg_fare' | 'avg_distance' | 'avg_minutes' | 'tip_pct';

const int = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
const compact = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 });

export const METRICS: Record<
  MetricId,
  { label: string; value: (a: Acc) => number | null; format: (v: number) => string; isRatio: boolean }
> = {
  trips: { label: 'Trips', value: (a) => a.trips, format: (v) => int.format(v), isRatio: false },
  revenue: {
    label: 'Total paid',
    value: (a) => a.total_sum,
    format: (v) => '$' + compact.format(v),
    isRatio: false,
  },
  avg_fare: {
    label: 'Average fare',
    value: (a) => (a.trips > 0 ? a.fare_sum / a.trips : null),
    format: (v) => '$' + v.toFixed(2),
    isRatio: true,
  },
  avg_distance: {
    label: 'Average distance',
    value: (a) => (a.trips > 0 ? a.distance_sum / a.trips : null),
    format: (v) => v.toFixed(2) + ' mi',
    isRatio: true,
  },
  avg_minutes: {
    label: 'Average duration',
    value: (a) => (a.trips > 0 ? a.minutes_sum / a.trips : null),
    format: (v) => v.toFixed(1) + ' min',
    isRatio: true,
  },
  // Card payments only: cash tips are not recorded in the TLC data.
  tip_pct: {
    label: 'Tip % (card)',
    value: (a) => (a.card_fare_sum > 0 ? (a.card_tip_sum / a.card_fare_sum) * 100 : null),
    format: (v) => v.toFixed(1) + '%',
    isRatio: true,
  },
};

/** Top N entries by metric. Ratio metrics ignore tiny groups so one odd trip cannot top the ranking. */
export function topN<K>(
  groups: Map<K, Acc>,
  metric: MetricId,
  n: number,
  minTrips: number,
): { key: K; value: number; acc: Acc }[] {
  const m = METRICS[metric];
  return [...groups.entries()]
    .filter(([, acc]) => !m.isRatio || acc.trips >= minTrips)
    .map(([key, acc]) => ({ key, value: m.value(acc), acc }))
    .filter((e): e is { key: K; value: number; acc: Acc } => e.value !== null)
    .sort((a, b) => b.value - a.value)
    .slice(0, n);
}
