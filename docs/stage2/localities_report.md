# Locality tier — build report

Built 26 September 2026, release `stage2-4cc30d479fecaf22`, in the workspace at
`data_darbar_warehouse/stage2/localities-2026-09-26/`. **Nothing is published.**

Tables 31 to 34: individual rural mauzas and urban localities. These rows are *places*, not
published administrative units, so this is a separate release from the unit panel. Table 35
remains unavailable as Excel — all five links return 404 — and is not included.

## What was produced

| | |
|---|---:|
| Observations | **2,426,794** |
| Places | **71,615** |
| Rural mauzas | 46,671 |
| Named urban localities | 597 |
| Places carrying a hadbast or deh number | 30,807 (65% of rural) |
| Missing cells (printed dashes) | 438,837 |

### The hierarchy

Each level is published as a subtotal row and is included, tagged with `level`. **They must
never be summed with their children** — doing so roughly triples the national population.

| Level | Places | Where |
|---|---:|---|
| district | 279 | all |
| sub_district (tehsil / taluka) | 996 | all |
| qanungo_halqa | 1,110 | Punjab, KP, Balochistan |
| patwar_circle | 9,147 | Punjab, KP, Balochistan |
| supervisory_tapedar_circle | 272 | Sindh |
| tapedar_circle | 1,448 | Sindh |
| **mauza / deh** | **47,316** | all |
| locality (named urban) | 597 | all |
| charge | 1,643 | urban |
| circle | 8,791 | urban |

Sindh uses an entirely different vocabulary from the other provinces — the fourth time in
this project that a Sindh-specific naming convention has broken a rule that held elsewhere.

## The check that matters, and what it found

**Do a district's mauzas sum to its published rural population?** For **115 of 129**
districts, yes, within 1%.

| Province | Districts reconciling |
|---|---|
| Punjab | 33 of 35 |
| Khyber Pakhtunkhwa | 29 of 34 |
| Balochistan | 29 of 34 |
| Sindh | 24 of 25 |
| Islamabad | 0 of 1 |

It began at 96 of 129. The gap closed once three more grouping levels were recognised.

| District | Published rural | Mauza sum | Ratio |
|---|---:|---:|---:|
| Bajaur | 1,287,960 | 3,849,339 | 2.99× |
| Khyber | 1,051,560 | 3,154,212 | 3.00× |
| Lower Kohistan | 340,017 | 2,861,511 | 8.4× |
| South Waziristan | 888,675 | 2,657,368 | 2.99× |
| Mohmand | 553,933 | 1,661,799 | 3.00× |

Ratios that land on whole numbers mean the same people counted at several levels. The cause
was that the revenue hierarchy is not the same in every province, and three of its levels
were not being recognised:

- **`TRIBE` and `SECTION`** sit between the tehsil and the village throughout KP's ex-FATA
  districts. Bajaur's Bar Chamer Kand tehsil publishes `TARKANI TRIBE` then
  `MEHMOOD KHEL SECTION` then seven villages, each level repeating the same 3,574 — which is
  why the mauza level over-counted by exactly 2.99x.
- **`UC`** (union council) appears in 156 KP and 89 Balochistan blocks.

Adding those three took reconciliation from 96 districts to 114, and a further arithmetic
rule took it to 115.

`rural_reconciliation.csv` carries the verdict per district. **Do not sum the mauza level in
a district marked `hierarchy_unreliable`.** 14 remain: Lower Kohistan (5.45x),
Washuk, Panjgur, Kharan, Bajaur, Karachi West, Gwadar, Kolai Palas, Khyber, Okara,
Islamabad, Rajanpur, Quetta and Upper Kohistan. Each is now a distinct one-off shape rather
than a common pattern, and resolving them individually risks fitting to arithmetic
coincidence rather than to a hierarchy PBS has stated.

This was not caught by the internal checks. Patwar-circle closure passes at 99.6% and key
uniqueness at 99.76% *while the KP mauza level over-counts by 47%*, because both tests only
ask whether the data is internally consistent with the hierarchy as read. Only comparing
against an independently published total exposed it.

## Other verification

| Check | Result |
|---|---|
| **A village's population joins its housing** | **46,704 of 46,752 mauzas — 99.9%** |
| Mauzas sum to their patwar circle | 138,261 of 138,494 comparisons — 99.8% |
| Composite key unique, rural | 99.76% |
| Composite key unique, urban | **100%** |
| Duplicate keys | 237 of 71,599 |

The go/no-go for this tier was 98% key uniqueness. Both halves clear it.

Parents are identified **by their place in the hierarchy, not by name alone**. Dera Ismail
Khan's Paharpur tehsil publishes two different patwar circles both called
`WANDA KHAN MOHD PC`; keying children on the name merges them and their populations stop
reconciling. Each place gets a `DDL-` identifier derived from its path, its name, its hadbast
number and a sequence number to separate same-named siblings.

That identifier is deliberately **independent of which table a row came from**. Tables 31 and
32 describe the same villages — population and housing — but do not have the same row counts,
so a row-position identifier cannot join them. With a path-derived one, 46,704 of 46,752
mauzas carry both halves: Jhansa in Abbottabad has 4,952 people and 912 structures on one
key. Without that, the tier would be two disconnected tables.

## Defects found in the published census

**The revenue hierarchy differs by province, and three levels carry suffixes PBS does not
document together.** `TRIBE` and `SECTION` in ex-FATA KP, and `UC` in KP and Balochistan,
sit between the tehsil and the village. Anything keyed only on `QH`/`PC`/`STC`/`TC` counts
the same people two or three times over in those districts.

**PBS sometimes omits the suffix from a subtotal row entirely.** In Okara's Renala Khurd tehsil,
`CHAK NO 016/1-L` is printed as a subtotal — no suffix, no hadbast number — directly above
the four mauzas that sum to exactly its value (20,573). Read as a mauza it both
double-counts and breaks its parent's reconciliation.

Two shapes occur: a row equal to the *sum* of the villages beneath it (Okara), and a row
equal to the *single* row beneath it — Panjgur prints a bare `GICHK` between
`GICHK SUB-TEHSIL` and `GICHK UC`, all three 33,578, a chain of single-child levels with no
suffix in the middle. 137 such rows were reclassified, and **only
where the equality is exact**. That is arithmetic, not inference; anything that
did not add up was left alone. Each carries `relabelled = true`.

**Urban localities contain census operational levels.** Table 33 nests `CHARGE NO nn` and,
under that, `CIRCLE NO nn` inside each named locality. Their numbers restart in every
locality, so `CIRCLE NO 01` is meaningless without its parents — which is why the urban key
is only unique once charge and locality are included.

## The remaining 0.4%

494 patwar circles do not reconcile. The pattern is special entities that sit outside the
revenue hierarchy: in Abbottabad tehsil, `SIAL KOT PC` reports 5,194 while its seven listed
rows sum to 5,665 — the difference, 471, is exactly `GALYAT DEVELOPMENT AUTHORITY`, which
is printed inside the block but is not part of the circle.

These are reported rather than resolved. Placing them would mean asserting a hierarchy PBS
does not state.

## What this tier is, and is not

It **is** a queryable register of 71,599 Pakistani places with population, literacy,
educational attainment, religion, age structure, area and housing, nested inside tehsils
that already carry polygons — **usable at mauza level in the 115 districts that reconcile**,
which covers Punjab and Sindh almost completely.

It **is not** usable at mauza level in the 14 districts flagged
`hierarchy_unreliable` until the unmarked levels in those files are identified. Nor is it a mapped village layer or a cross-census panel. No public mauza polygons
exist; the only known complete set is the Sindh shapefile digitised from patwari maps by
Bin Khalid and Mattsson. And with 35% of rural places and all urban ones lacking a stable
identifier, linking to Census 2017 would be name-matching at a scale where the error rate
could not be characterised honestly.

The key is explicitly **census-specific**: `own_id` and `parent_id` encode table, province
and source row. They are stable across rebuilds of this release and nothing more.
