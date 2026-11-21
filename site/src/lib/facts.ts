// Numbers quoted in the case studies. tests/facts.test.ts checks every one against the published data files,
// so the prose cannot drift from the data after a re-run.
export const FACTS = {
  bronzeRows: 41_169_720,
  duplicatesRemoved: 7,
  evaluatedRows: 41_169_713,
  cleanRows: 35_486_771,
  quarantinedRows: 5_682_942,
  quarantinePct: 13.8,
  passengerFailed: 4_492_869,
  passengerPct: 10.9,
  rulesCount: 10,
  cubeRows: 53_591,
  cubeMB: 1.3,
};
export const n = (x: number) => x.toLocaleString('en-US');
