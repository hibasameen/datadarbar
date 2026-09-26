# Census 2017 — plan to publication

Written 26 September 2026, against the draft in
`data_darbar_warehouse/census2017/draft-2026-09-26/`. This is a plan, not a
commitment to a date; the estimates are in working days of focused effort and the
uncertain ones say so.

## Where the work stands

| Stage | 2023 | 2017 | |
|---|---|---|---|
| 0 Acquisition | done | **done** | 5,746 files, locked, verified |
| 1 Extraction | done | **done** | 35 of 40 unit tables, plus table 2 and the four locality tables |
| 2 Geography register | mostly done | **not started** | the critical path |
| 3 Missingness reconciliation | done | **done** | 899,800 cells recovered |
| 4 Panel assembly | done | **partial** | built; 636 rows flagged |
| 5 Warehouse and catalogue | done | **nothing exists** | |
| 6 Site integration | not started | not started | shared with 2023 |
| 7 Release | machinery ready | **no machinery** | |

What is already publication-grade: the acquisition, the two-rendering
reconciliation (3.9 million cells compared directly, better evidenced than 2023's),
and reproducibility. What is not: half the tables, all of the geography, and every
piece of release apparatus.

## The decision that shaped everything else — settled: panel

**Does 2017 ship as a second cross-section, or as a panel?** Settled in favour of
the panel; recorded here because it is why Workstream C is on the critical path.

The two are very different pieces of work, and the answer changes the critical
path.

*A second cross-section* — 2017 published alongside 2023, no crosswalk — is
Workstreams A, B and D below, and is reachable in about three weeks. But it
invites the inference it does not support: a user downloads both years, joins on
district name, and computes a change over time that is wrong wherever a boundary
moved. Every district in Khyber Pakhtunkhwa changed between the two censuses.

*A panel* is the reason 2017 was acquired at all, and needs Workstream C, which
is the only part of this plan containing genuine research judgement rather than
engineering.

**Decided: the panel.** A cross-section release can still be cut from the same
work if the register proves harder than expected, but not the other way round —
publishing first and adding the crosswalk later leaves wrong numbers in
circulation in the meantime.

---

## Workstream A — finish the extraction

**Complete.** A1–A8 all done. Can run in parallel with C.

| Task | Days | Notes |
|---|---:|---|
| ~~A1. Islamabad~~ | done | Its 39 anchor-listed workbooks are now read as district workbooks. All 135 districts sum to 207,684,626, the published national total, and all six areas reconcile exactly |
| ~~A2. Locality tables 23–26~~ | done, with a caveat | 2,505,703 observations and 46,692 mauzas, through 2023's reader with one parameter added. Patwar-circle closure 99.9%. **But only 60 of 128 districts reconcile against published rural population** — the unsuffixed-subtotal problem 2023 solved and 2017 has not. See [census2017_localities.md](census2017_localities.md) |
| ~~A3. Table 2, urban locality list~~ | done | 589 named localities, 0 problems. Four areas sum exactly to their published urban population; table 2 is not expected to sum overall, as 2023's does not |
| ~~A4. Profile and declare the 16 undeclared tables~~ | done, with residue | All 16 declared and extracted: 35 of 40 tables, 4,397,361 observations. Five of the new ones are entirely clean (21, 22, 28, 31, 40); tables 29, 30, 32 and 35 hold most of the remaining ambiguity because **their category labels are bare numbers** — 1, 2, 3 rooms — indistinguishable from a column-number row, so the header collapses to its banner |
| ~~A5. Kohistan's table 4~~ | done | It labels the total row `ALL`, not `All Ages`. KP's 784,711 gap against its own published total is now zero |
| ~~A7. Resolve the locality hierarchy over-count~~ | done | Four defects: the check added a literacy percentage to a population; unsuffixed grouping rows were read as villages in ten districts; Malakand was missing from the layer entirely; and two tiers (`SECTION-N (PART)`, Gwadar's `CIRCLE`) were unrecognised. All 129 districts reconcile, and 2023 reaches 124 of 130 with the remaining six being PBS's own tables disagreeing. Nothing is flagged `hierarchy_unreliable` in either year |
| ~~A8. Bare-number column labels~~ | done | `txt` returned None for a non-string, so numeric header labels were dropped and every column collapsed onto its banner. Flagged rows fell from 109,778 to 31,963; it also fixed a 2023 column that had shipped mislabelled |
| ~~A6. Canonicalise indicator case~~ | done | 101 indicator groups merged. Raised table 35's flagged count by 3,798, which A8 covers |

**Exit test:** every table has every district that should have one, and the area
check finds a matching series for every comparable published figure.

## Workstream B — close out verification

**3–5 days.** Depends on A for the tables it adds.

| Task | Days | Notes |
|---|---:|---|
| ~~B1. The district×table pairs that do not align against their PDF~~ | done | Two causes, both found in Lahore's table 14. The section's stub/data boundary is one column for the whole section and a wide value in the first data column started to the left of it, so `10 AND ABOVE  535,956` read as label `10 AND ABOVE5` and value `35,956`; the label then matched nothing and every later row in the block paired against the wrong one. And three PDFs carry each other's table 5 in a cycle — Rawalpindi's holds Rahim Yar Khan, Rahim Yar Khan's holds Rajanpur, Rajanpur's holds Rawalpindi — which is a PBS binding error, now detected from the section's own printed banner and recorded rather than compared. **Differences 30,768 → 3,461** |
| ~~B2. The rows never compared~~ | done | The same boundary fix, plus banner rows no longer counted as failures: a unit, locality or sex heading carries no numbers, so there is nothing in the PDF for it to align to. **Unaligned 49,835 → 5,758**, with 65,947 label rows reported separately |
| ~~B3/B4. The area verification~~ | mostly | The area workbooks list every area down one sheet and the block cut did not recognise a province label, so each area's block ran to the end and every other area's figures were read as its own — Pakistan's table 1 was compared against Punjab's 109,989,655 among twelve others. A series the panel already flags `series_ambiguous`, or one the workbook prints more than once, is now reported as ambiguous rather than as a difference. **Differs 16,327 → 8,189; exact 88,700 → 89,932; 3,114 ambiguous recorded; no matching series 6,030 → 5,994** |

**Exit test:** no unexplained difference against an independently published
figure. Every remaining one is recorded with a reason.

**Not yet met.** 8,189 area differences and 5,994 series with no counterpart are
still unexplained, as are 3,461 cell differences against the PDFs and 5,758
unaligned rows. The residue is thin and spread — the worst district×table is now
449 rows, against 6,551 before — and much of the unaligned sits in tables 29, 30
and 32, whose category labels are bare numbers, which A4 already records.

## Workstream C — the geography register  ← critical path

**10–20 days, and the estimate is soft.** This is the only part with real
research judgement in it.

The problem is not name matching. It is that the administrative geography changed
fundamentally between the two censuses:

- **FATA was dissolved.** In 2017 it was a federal territory with 7 agencies and
  6 Frontier Regions; in 2018 all of it merged into Khyber Pakhtunkhwa. Its 13
  units have no 2023 counterpart under the same name or tier.
- **Districts were created and split.** KP had 25 districts in 2017 and has more
  now; the same is true elsewhere.
- **The tiers differ.** 2017 has sub-divisions and Frontier Regions; 2023 does
  not.

| Task | Days | Notes |
|---|---:|---|
| C1. District-level crosswalk | 3–5 | 134 × 2023's 135; most are one-to-one, the FATA merger and the splits are not |
| C2. Implement the FATA mapping | 1 | Now specified, not open — see decision 3. Seven one-to-one renames, six merges into adjoining settled districts |
| C3. Sub-district crosswalk | 4–8 | 667 units in 2017 against 591 in 2023. `build_crosswalk.py` already does layered resolution with evidence recorded per unit and candidates listed for anything withheld; adapting it is the cheap part, reviewing the residue is not |
| C4. Comparability flags | 1–2 | Per unit and per pair: exact, renamed, merged, split, no counterpart. Must distinguish the seven FATA renames from the six merges. Published alongside, not hidden |
| C5. Validation | 2–3 | A crosswalk is checkable: population under a mapping must be conserved within a stated tolerance, and the direction of any discrepancy must be explicable. The six merged KP districts get checked explicitly, since each must equal two 2017 units summed |

**Exit test:** every 2017 unit carries a documented relationship to 2023, or an
explicit statement that it has none — and the aggregate populations reconcile
under the mapping.

**Risk.** C3 is the task most likely to overrun. 2023's equivalent left 54 units
withheld after all automated matching, and that was within one census year. Across
a boundary reform the residue will be larger, and the honest response to a unit
that cannot be resolved is to withhold it, not to force it.

## Workstream D — warehouse, catalogue and release machinery

**4.5–6.5 days** (D0 done). Depends on A; C determines whether a crosswalk table ships with it. D0 should run first, and soon.

| Task | Days | Notes |
|---|---:|---|
| ~~D0. Rename `province` to `province_area`~~ | done | Both years rebuilt and verified data-neutral |
| D1. `build_warehouse_2017.py` | 2 | Parquet tables sized for the DuckDB-WASM range-read design, as 2023's five are |
| D2. Catalogue entries | 1 | The `notes` field is the only place a user is warned before publishing a number. For 2017 it must carry: `missing` is not zero and 26.8% of cells are missing; `unit_type` must be filtered or district and sub-district rows double-count; `is_rate` must never be averaged; `series_ambiguous` marks 636 rows whose key is not unique |
| D3. `build_all_2017.py` | 1 | One command, refuses to start if the tests fail, as 2023's does |
| D4. Input lock and release manifest | 0.5 | `lock_acquisition.py` exists; the release-level lock does not |
| D5. Build report and verification record | 1 | The `verification.json` equivalent |
| D6. Repeat-build gate in CI | 0.5 | `verify_2017.py` exists and passes; wire it so a non-deterministic build cannot be released |

**Exit test:** one command builds the release from the locked capture, byte-identically, and refuses if the tests fail.

## Workstream E — site integration

**Shared with 2023 and currently blocked on the same two open decisions**
(COD-AB geometry adoption; whether to aggregate the 43 new KP/Balochistan
tehsils). Adding a year multiplies the surface: a year selector, cross-year
indicator alignment, and — the part that needs care — **refusing to draw a change
over time where the crosswalk says the units are not comparable.**

Not estimated here; it needs its own plan once C lands.

---

## Sequence

```
Week 1–2    A (extraction)          ──┐
            C1–C2 (districts, FATA)   ├─ parallel
Week 3      B (verification)         ─┘
            C3 begins
Week 4–5    C3–C5 (sub-district crosswalk, validation)
Week 6      D (warehouse, release machinery)
            cut an internal release, review it
Week 7+     E (site), separately planned
```

**Roughly six weeks to a reviewable 2017 release**, with C3 the item most likely
to move that. A cross-section-only release could be cut at the end of week 3, and
I would advise against it.

## Decisions — settled 26 September 2026

**1. 2017 ships as a panel.** Not a second cross-section. Workstream C is
therefore on the critical path and a release does not go out without at least a
district-level crosswalk.

**2. The column is now `province_area`.** Done, both years. Read from "province/area": it must
cover both, since the values include FATA in 2017 and Islamabad in 2023, neither
of which is a province. Renamed in both census years before Workstream D builds a warehouse on it. Verified data-neutral: 4,599,521 rows in 2023 and 3,362,716 in 2017, zero rows changed when the column is renamed back, and repeat builds remain byte-identical. The capture manifest and the external mouza2020 crosswalk keep their own `province` field — they record where a file came from, not which area a figure belongs to.

**3. FATA is presented as merged districts.** This was the right call and better
founded than the alternative I had suggested — the 2023 counterparts do exist, so
holding FATA as its own flagged tier would have hidden a mapping that is available.
It resolves into two kinds, and the distinction matters for how a cross-year
figure may be computed:

| 2017 unit | 2023 district | Relationship |
|---|---|---|
| Bajaur Agency | Bajaur District | one-to-one |
| Khyber Agency | Khyber District | one-to-one |
| Kurram Agency | Kurram District | one-to-one |
| Mohmand Agency | Mohmand District | one-to-one |
| North Waziristan Agency | North Waziristan District | one-to-one |
| Orakzai Agency | Orakzai District | one-to-one |
| South Waziristan Agency | South Waziristan District | one-to-one |
| FR Bannu | Bannu District | **merged** — with 2017 Bannu |
| FR Kohat | Kohat District | **merged** — with 2017 Kohat |
| FR Peshawar | Peshawar District | **merged** — with 2017 Peshawar |
| FR Tank | Tank District | **merged** — with 2017 Tank |
| FR Lakki Marwat | Lakki Marwat District | **merged** — with 2017 Lakki Marwat |
| FR D.I.Khan | Dera Ismail Khan District | **merged** — with 2017 D.I.Khan |

The seven agencies became districts under the same name, so those are renames, not
merges, and a 2017 figure carries straight across. The six Frontier Regions were
absorbed into adjoining settled districts, so **each of those six 2023 districts
equals two 2017 units added together** — the settled district plus its FR. A
cross-year comparison that ignores this understates the 2017 side of six KP
districts. The comparability flag (C4) has to distinguish the two cases, and the
validation in C5 has to check the merged six specifically.

**4. The 16 `partial`-mapping tables ship**, flagged as 2017-only and excluded
from cross-year joins.

**5. Both years are republished together.** Verified: nothing from either build is
published — the live warehouse carries no `census2023_*` or `census2017_*` table,
only an unrelated `census_enrolment_5_16_by_sex.parquet`. So there is no live
schema to migrate and no published figure to withdraw.

## A risk outside the plan

**None of this work is in version control.** `etl/stage2`, `etl/census2017`,
`docs/stage1`, `docs/stage2` and the rest are untracked in git; the only tracked
change is a modification to `README.md`. Every line of the pipeline, every
declared table layout and every recorded defect exists solely as untracked files
in an iCloud folder. That is a larger exposure than anything in the workstreams
below, and committing is a few minutes' work.

## What would make me say stop

- If C3 leaves more than about 15% of sub-district units unresolved, the honest
  product is a district-level panel with sub-district data published per year and
  no cross-year join. That is a smaller claim but a true one.
- If the 46 unaligned district×table pairs turn out to be real PBS disagreements
  rather than alignment failures, B1 becomes a research task and the estimate
  changes materially.
