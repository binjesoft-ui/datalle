// "Read the data" quiz. Answers are COMPUTED from the cubes, never typed in, so they stay correct
// when new data is loaded and can be checked by tests.

import { ALL, METRICS, aggregate, topN } from './engine.ts';
import type { Acc, Filters, MetricId, Row } from './engine.ts';

export type Dataset = { daily: Row[]; time: Row[]; zone: Row[] };
export type View = { metric: MetricId };
export type Question = {
  id: string;
  question: string;
  hint: string;
  view: View;
  answer: (d: Dataset) => string;
};

const WEEKDAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']; // dow 1 = Sunday
const timeMonth = (r: Row) => r.month as string;
const dayMonth = (r: Row) => (r.pickup_date as string).slice(0, 7);

const int = new Intl.NumberFormat('en-US');
const fmt = (m: MetricId, v: number) => METRICS[m].format(v);
const hour = (h: number) => `${h}:00`;
const pct = (part: number, whole: number) => ((part / whole) * 100).toFixed(1) + '%';
const monthName = (m: string) =>
  new Date(m + '-01T00:00:00Z').toLocaleString('en-US', { month: 'long', timeZone: 'UTC' });
const dateLabel = (iso: string) =>
  new Date(iso + 'T00:00:00Z').toLocaleDateString('en-US', {
    weekday: 'long', month: 'long', day: 'numeric', year: 'numeric', timeZone: 'UTC',
  });

/** US public holidays that fall on a fixed rule, so the answer can point at them without hard-coding a year. */
export function holidayName(iso: string): string | null {
  const [y, m, d] = iso.split('-').map(Number);
  const wd = new Date(Date.UTC(y, m - 1, d)).getUTCDay(); // 0 = Sunday
  if (m === 1 && d === 1) return "New Year's Day";
  if (m === 7 && d === 4) return 'Independence Day';
  if (m === 12 && d === 25) return 'Christmas Day';
  if (m === 5 && wd === 1 && d + 7 > 31) return 'Memorial Day';
  if (m === 9 && wd === 1 && d <= 7) return 'Labor Day';
  if (m === 11 && wd === 4 && d >= 22 && d <= 28) return 'Thanksgiving';
  return null;
}

function fullRange(d: Dataset): Filters {
  const months = [...new Set(d.time.map(timeMonth))].sort();
  return { from: months[0], to: months[months.length - 1], borough: ALL, payment: ALL };
}

function ranked<K>(groups: Map<K, Acc>, metric: MetricId, minTrips = 0) {
  return topN(groups, metric, groups.size, minTrips);
}

export const QUESTIONS: Question[] = [
  {
    id: 'busiest-hour',
    question: 'At what hour of the day are the most trips picked up, and at what hour the fewest?',
    hint: 'Show Trips and look at the "by pickup hour" chart. Hover a bar to read its value.',
    view: { metric: 'trips' },
    answer: (d) => {
      const r = ranked(aggregate(d.time, timeMonth, (x) => x.pickup_hour as number, fullRange(d)), 'trips');
      const hi = r[0], lo = r[r.length - 1];
      return `Busiest: ${hour(hi.key)} (${fmt('trips', hi.value)} trips). Quietest: ${hour(lo.key)} (${fmt('trips', lo.value)} trips). ` +
        `The busiest hour has about ${(hi.value / lo.value).toFixed(0)} times as many trips as the quietest.`;
    },
  },
  {
    id: 'busiest-weekday',
    question: 'Which day of the week has the most trips, and which has the fewest? Is it the day you expected?',
    hint: 'Show Trips and compare the rows of the weekday and hour heatmap: which row is darkest overall?',
    view: { metric: 'trips' },
    answer: (d) => {
      const r = ranked(aggregate(d.time, timeMonth, (x) => x.pickup_dow as number, fullRange(d)), 'trips');
      const hi = r[0], lo = r[r.length - 1];
      return `Most trips: ${WEEKDAYS[hi.key - 1]} (${fmt('trips', hi.value)}). Fewest: ${WEEKDAYS[lo.key - 1]} (${fmt('trips', lo.value)}). ` +
        `Full order: ${r.map((e) => WEEKDAYS[e.key - 1]).join(', ')}.`;
    },
  },
  {
    id: 'top-zone-trips-vs-money',
    question: 'Which pickup zone has the most trips? Is it also the zone where passengers paid the most money in total?',
    hint: 'Look at the "Top 10 pickup zones" chart with Trips selected, then switch to Total paid and compare.',
    view: { metric: 'trips' },
    answer: (d) => {
      const g = aggregate(d.zone, timeMonth, (x) => x.zone as string, fullRange(d));
      const byTrips = ranked(g, 'trips'), byMoney = ranked(g, 'revenue');
      const a = byTrips[0], b = byMoney[0];
      const base = `Most trips: ${a.key} (${fmt('trips', a.value)}). Most money paid: ${b.key} (${fmt('revenue', b.value)}). `;
      if (a.key !== b.key) return base + 'They are different zones, so a zone with many trips is not automatically the one that earns the most.';
      const second = byTrips[1];
      const perTrip = (acc: Acc) => acc.total_sum / acc.trips;
      return base + `Compare with ${second.key}, the number 2 zone by trips: a trip from ${a.key} brought in about ` +
        `$${perTrip(a.acc).toFixed(0)} on average, against about $${perTrip(second.acc).toFixed(0)} from ${second.key}.`;
    },
  },
  {
    id: 'highest-fare-zones',
    question: 'Which zones have the highest average fare, and what do they have in common?',
    hint: 'Switch to Average fare. The "Top 10 pickup zones" chart now ranks zones by it (zones with under 200 trips are left out). Look the names up on a map.',
    view: { metric: 'avg_fare' },
    answer: (d) => {
      const f = fullRange(d);
      const r = ranked(aggregate(d.zone, timeMonth, (x) => x.zone as string, f), 'avg_fare', 200).slice(0, 10);
      const airports = r.filter((e) => String(e.key).includes('Airport')).length;
      const topAcc = r.slice(0, 5).reduce((s, e) => ({ trips: s.trips + e.acc.trips, distance: s.distance + e.acc.distance_sum }), { trips: 0, distance: 0 });
      const allAcc = [...aggregate(d.zone, timeMonth, () => 'all', f).values()][0];
      return `Top 5: ${r.slice(0, 5).map((e) => `${e.key} (${fmt('avg_fare', e.value)})`).join(', ')}. ` +
        `${airports} of the top 10 are airports. Trips from the top 5 average ${(topAcc.distance / topAcc.trips).toFixed(1)} miles, ` +
        `against ${(allAcc.distance_sum / allAcc.trips).toFixed(1)} miles for all trips. ` +
        `Check where the other zones are on a map: how far are they from Midtown Manhattan?`;
    },
  },
  {
    id: 'borough-fare',
    question: 'Which pickup borough has the highest average fare, and which the lowest? (Ignore boroughs with under 50,000 trips and the "Unknown" and "N/A" groups.)',
    hint: 'Show Average fare, then pick one borough at a time in the filter and read the "Average fare" tile.',
    view: { metric: 'avg_fare' },
    answer: (d) => {
      const g = aggregate(d.zone, timeMonth, (x) => x.borough as string, fullRange(d));
      g.delete('Unknown'); g.delete('N/A');
      const r = ranked(g, 'avg_fare', 50000);
      const hi = r[0], lo = r[r.length - 1];
      return `Highest: ${hi.key} (${fmt('avg_fare', hi.value)}). Lowest: ${lo.key} (${fmt('avg_fare', lo.value)}). ` +
        `Most taxi pickups happen in ${[...g.entries()].sort((a, b) => b[1].trips - a[1].trips)[0][0]}, ` +
        `so ask yourself what kind of trip dominates there, and what kind of trip a ${hi.key} pickup tends to be.`;
    },
  },
  {
    id: 'tip-hours',
    question: 'At which hour do card-paying passengers tip the highest percentage, and at which the lowest?',
    hint: 'Show Tip % (card) and read the "by pickup hour" chart. Remember cash tips are not recorded.',
    view: { metric: 'tip_pct' },
    answer: (d) => {
      const r = ranked(aggregate(d.time, timeMonth, (x) => x.pickup_hour as number, fullRange(d)), 'tip_pct');
      const hi = r[0], lo = r[r.length - 1];
      return `Highest: ${hour(hi.key)} (${fmt('tip_pct', hi.value)}). Lowest: ${hour(lo.key)} (${fmt('tip_pct', lo.value)}). ` +
        `That is a gap of ${(hi.value - lo.value).toFixed(1)} percentage points between the best and worst hour. ` +
        `Is that a big difference to you? Compare it with how much the busiest hour differs from the quietest in question 1.`;
    },
  },
  {
    id: 'payment-share',
    question: 'What share of trips is paid by card and what share in cash?',
    hint: 'Show Trips. Choose a payment type in the filter and compare the Trips tile with the all-types total.',
    view: { metric: 'trips' },
    answer: (d) => {
      const g = aggregate(d.time, timeMonth, (x) => x.payment as string, fullRange(d));
      const totalTrips = [...g.values()].reduce((s, a) => s + a.trips, 0);
      const r = ranked(g, 'trips');
      const main = r.filter((e) => e.acc.trips / totalTrips >= 0.001)
        .map((e) => `${e.key} ${pct(e.acc.trips, totalTrips)}`).join(', ');
      const card = g.get('Credit card'), cash = g.get('Cash');
      const ratio = card && cash ? ` Card trips outnumber cash trips about ${(card.trips / cash.trips).toFixed(0)} to 1.` : '';
      return `${main}.${ratio} Because cash tips are not recorded, every tip figure on this page describes card payers only.`;
    },
  },
  {
    id: 'busiest-month',
    question: 'Which month has the most trips and which the fewest? Then compare trips per day, since months have different lengths.',
    hint: 'Use the From and To controls to select one month at a time and watch the Trips tile. Divide by the number of days.',
    view: { metric: 'trips' },
    answer: (d) => {
      const f = fullRange(d);
      const r = ranked(aggregate(d.time, timeMonth, (x) => x.month as string, f), 'trips');
      const days = new Map<string, Set<string>>();
      for (const row of d.daily) {
        const m = dayMonth(row);
        if (!days.has(m)) days.set(m, new Set());
        days.get(m)!.add(row.pickup_date as string);
      }
      const perDay = (e: { key: string; value: number }) => e.value / (days.get(e.key)?.size || 1);
      const hi = r[0], lo = r[r.length - 1];
      const pd = [...r].sort((a, b) => perDay(b) - perDay(a));
      const sameLow = pd[pd.length - 1].key === lo.key;
      return `Most: ${monthName(hi.key)} (${fmt('trips', hi.value)}, about ${int.format(Math.round(perDay(hi)))} per day). ` +
        `Fewest: ${monthName(lo.key)} (${fmt('trips', lo.value)}, about ${int.format(Math.round(perDay(lo)))} per day). ` +
        (sameLow
          ? `${monthName(lo.key)} is still the quietest after dividing by the number of days.`
          : `Per day, the quietest month is actually ${monthName(pd[pd.length - 1].key)}.`);
    },
  },
  {
    id: 'trip-duration',
    question: 'At which hours do trips take longest on average, and at which are they shortest? Does the longest hour match the busiest hour?',
    hint: 'Show Average duration and read the "by pickup hour" chart. Then switch back to Trips and compare.',
    view: { metric: 'avg_minutes' },
    answer: (d) => {
      const g = aggregate(d.time, timeMonth, (x) => x.pickup_hour as number, fullRange(d));
      const r = ranked(g, 'avg_minutes');
      const hi = r[0], lo = r[r.length - 1];
      const busiest = ranked(g, 'trips')[0];
      const busiestMin = g.get(busiest.key)!;
      return `Longest: ${hour(hi.key)} (${fmt('avg_minutes', hi.value)}). Shortest: ${hour(lo.key)} (${fmt('avg_minutes', lo.value)}). ` +
        `The busiest hour for pickups is ${hour(busiest.key)}, where trips average ${fmt('avg_minutes', METRICS.avg_minutes.value(busiestMin)!)}. ` +
        (hi.key === busiest.key ? 'They are the same hour.' : 'So the busiest hour and the slowest hour are not the same.');
    },
  },
  {
    id: 'quiet-days',
    question: 'Look at the "Trips per day" line. Which day has the most trips and which the three fewest? Check those dates on a calendar.',
    hint: 'Show Trips, find the highest peak and the deepest dips in the daily chart, and hover to read the dates.',
    view: { metric: 'trips' },
    answer: (d) => {
      const r = ranked(aggregate(d.daily, dayMonth, (x) => x.pickup_date as string, fullRange(d)), 'trips');
      const hi = r[0];
      const low = r.slice(-3).reverse();
      const tag = (iso: string) => { const h = holidayName(iso); return h ? ` - ${h}` : ''; };
      return `Busiest day: ${dateLabel(hi.key)} (${fmt('trips', hi.value)} trips)${tag(hi.key)}. ` +
        `Quietest three: ${low.map((e) => `${dateLabel(e.key)} (${fmt('trips', e.value)})${tag(e.key)}`).join('; ')}. ` +
        `The data shows when demand drops, not why: for the busiest day, look up what was happening in the city that week.`;
    },
  },
];
