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

**Do a district's mauzas sum to its published rural population?** For **121 of 130**
districts, yes, within 1%.

| Province | Districts reconciling |
|---|---|
| Punjab | 34 of 35 |
| Khyber Pakhtunkhwa | 30 of 35 |
| Balochistan | 32 of 34 |
| Sindh | **25 of 25** |
| Islamabad | 0 of 1 |

It began at 96 of 129. The gap closed once three more grouping levels were recognised,
and again when the 2017 work on unsuffixed groupings was carried back into the shared
reader.

Only **45 of the 130 reconcile exactly**, against 123 of 129 in 2017. The 1% tolerance
is doing far more work in 2023 than in 2017, and that difference is not yet explained.
A district inside the tolerance is not evidence of an exact hierarchy here.

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

### Malakand was absent, and Lower Kohistan had swallowed it

Tables 5–23 print Malakand as `MALAKAND PROTECTED AREA`, a former Provincially
Administered Tribal Area whose name carries none of the words that mark a unit. The
locality reader never called the corpus-wide alias table — the unit reader did — so the
district's own row read as a village, and because a village does not reset the current
district, **every tehsil below it was attributed to Lower Kohistan**: Sam Rani Zai, Swat
Rani Zai and Thana Baizai, all Malakand's, appeared under a district that has neither.
That is the whole of Lower Kohistan's 445% over-count, and the reason Malakand appeared
in no locality row in either census year.

The alias `MALAKAND PROTECTED AREA → MALAKAND DISTRICT` already existed and was already
reviewed. Applying it in the locality reader adds Malakand as the 130th district and
takes Lower Kohistan from +445% to −11%.

### What remains

`rural_reconciliation.csv` carries the verdict per district. **Do not sum the mauza level in
a district marked `hierarchy_unreliable`.** Nine remain:

| District | Excess | Share |
|---|---:|---:|
| Bajaur | 590,855 | +45.88% |
| Kolai Palas Kohistan | −43,436 | −15.50% |
| Upper Kohistan | −48,764 | −11.53% |
| Gwadar | 16,172 | +11.07% |
| Lower Kohistan | −37,367 | −10.99% |
| Quetta | −50,512 | −4.90% |
| Rajanpur | −55,545 | −3.17% |
| Khyber | 26,273 | +2.50% |
| Islamabad | −14,747 | −1.18% |

Six of the nine are now **under**, which is a different defect from the over-counting
this page is mostly about: villages absent or unread, which relabelling cannot create.
Bajaur is the one large over-count left and its mechanism is understood but unfixed —
its tiers agree with each other (district, tribes and tehsils all 1,283,113) while the
village level runs 619,373 higher, which means unsuffixed `SECTION` rows read as
villages. The run-based tests cannot catch them because *consecutive* unsuffixed
groupings make every candidate run overshoot: the run past one section header takes in
the next section's villages too. That needs a prefix-sum test rather than a whole-run
one, and has not been attempted.

The hadbast column is the reason 2023 is harder than 2017. It is populated for 99.8% of
2017's village rows but only 0% of Sindh's, 34.8% of KP's and 64.6% of Balochistan's in
2023 — so the discriminator that separates a real village from an unsuffixed subtotal is
largely unavailable, and the arithmetic has to carry the whole argument.

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

Three shapes occur: a row equal to the *sum* of the villages beneath it (Okara), a row
equal to the *single* row beneath it — Panjgur prints a bare `GICHK` between
`GICHK SUB-TEHSIL` and `GICHK UC`, all three 33,578, a chain of single-child levels with no
suffix in the middle — and a row equal to the sum of a run of **groupings** rather than
villages, which sits a tier higher again. 183 such rows were reclassified, and **only
where the equality is exact**. That is arithmetic, not inference; anything that
did not add up was left alone. Each carries `relabelled = true` and a `relabel_rule`
naming the test that fired.

One rule was tried and **rejected**: matching a bare row against the value of the unit
*above* it. It reads plausibly — Balochistan really does restate a sub-division at village
depth — but a union council with a single village shares that village's population, so the
rule deleted the village. It accounted for 406 of 467 relabels in 2023 and removed 5.37
million people before being withdrawn. Requiring the children beneath to sum instead
catches the same Balochistan rows and none of the single-child councils.

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
that already carry polygons — **usable at mauza level in the 121 districts that reconcile**,
which covers Punjab and Sindh almost completely.

It **is not** usable at mauza level in the 14 districts flagged
`hierarchy_unreliable` until the unmarked levels in those files are identified. Nor is it a mapped village layer or a cross-census panel. No public mauza polygons
exist; the only known complete set is the Sindh shapefile digitised from patwari maps by
Bin Khalid and Mattsson. And with 35% of rural places and all urban ones lacking a stable
identifier, linking to Census 2017 would be name-matching at a scale where the error rate
could not be characterised honestly.

The key is explicitly **census-specific**: `own_id` and `parent_id` encode table, province
and source row. They are stable across rebuilds of this release and nothing more.
