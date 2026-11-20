export interface RuleCompare { rule: string; notebook: number; pipeline: number; pct: number; match: boolean }

/** One row per pipeline expectation: failures counted by the notebook version vs the declarative pipeline. */
export function compareRules(
  notebook: { rule: string; rows_failed: number | string; pct_of_all_rows: number }[],
  pipeline: { rule: string; failed: number; pct_failed: number }[],
): RuleCompare[] {
  const nb = new Map(notebook.map((r) => [r.rule, Number(r.rows_failed)]));
  return pipeline.map((p) => {
    // The notebook report leaves out rules that failed nothing, so a missing rule means 0 failures.
    const n = nb.get(p.rule) ?? 0;
    return { rule: p.rule, notebook: n, pipeline: Number(p.failed), pct: p.pct_failed, match: n === Number(p.failed) };
  });
}

export interface Engine { ok: boolean; median_s?: number; runs_s: number[]; error?: string }
export interface SizeRow { months: number; input_mb: number; rows: number | null; spark: Engine; duckdb: Engine; engines_agree: boolean }

export interface BenchLine { months: number; mb: number; rows: number | null; spark: string; duckdb: string; ratio: string; agree: boolean | null }

const fmt = (e: Engine) => (e.ok && e.median_s != null ? `${e.median_s.toFixed(1)} s` : 'failed');

/** Display rows. The ratio says which engine was faster and by how much, never a bare "x times". */
export function benchLines(sizes: SizeRow[]): BenchLine[] {
  return sizes.map((s) => {
    let ratio = '–';
    if (s.spark.ok && s.duckdb.ok && s.spark.median_s! > 0 && s.duckdb.median_s! > 0) {
      const a = s.spark.median_s!, b = s.duckdb.median_s!;
      ratio = b <= a ? `DuckDB ${(a / b).toFixed(1)}× faster` : `Spark ${(b / a).toFixed(1)}× faster`;
    }
    return { months: s.months, mb: s.input_mb, rows: s.rows, spark: fmt(s.spark), duckdb: fmt(s.duckdb), ratio, agree: s.spark.ok && s.duckdb.ok ? s.engines_agree : null };
  });
}

/** Timings are only worth showing when both engines agreed with each other at every size that ran. */
export function benchmarkIsTrustworthy(sizes: SizeRow[]): boolean {
  return sizes.length > 0 && sizes.every((s) => !(s.spark.ok && s.duckdb.ok) || s.engines_agree);
}
