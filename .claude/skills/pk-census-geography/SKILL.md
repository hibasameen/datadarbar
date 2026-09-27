---
name: pk-census-geography
description: How Pakistan's census geography is put together and how it changed between 2017 and 2023 — the administrative tiers, the rival boundary files and their join keys, the rural/urban classifications, and the aggregation rules that silently produce wrong totals. Use whenever a task touches Pakistani districts or tehsils, joins a dataset to a boundary file, compares 2017 with 2023, or aggregates census figures.
---

# Pakistan's census geography

Everything here was established against the data in this repository, not from
memory, and most of it was established the hard way. The rules in **Traps**
each correspond to a wrong number that was produced and caught.

## The tiers

Province/area → division → district → sub-district → (2017 only) locality.

**PBS's Digital Census 2023 boundary layer** is the frame to work on: 8
provinces/areas, 38 divisions, **157 districts**, **650 tehsils**. Of those,
**136 districts and 591 tehsils carry census data** (`in_census_2023`).

**What Data Darbar draws is 156 districts and 649 tehsils**, and the rule is
worth stating exactly because the counts look arbitrary otherwise:

- **Azad Jammu & Kashmir and Gilgit-Baltistan are always drawn** — 20 districts
  and 58 tehsils between them — whether or not the census reaches them. Neither
  census panel has a single row for either, so they render grey. They are
  greyed for want of data, not left off: a map of Pakistan that stops at the
  census frame makes a different claim about the country.
- **The one polygon PBS labels `OCCUPIED KASHMIR` is always excluded** — the
  part of Jammu & Kashmir not under Pakistani administration, district code
  `000`. It is the only thing dropped, which is why 157 becomes 156 and 650
  becomes 649.

A tehsil outside the census has no `dds_id`, so the map keys it on PBS's
`tehsil_code` instead (`PBS-<code>`) and marks it `nc`. Nothing joins to that
key, which is the point: the shape draws and stays grey.

**The census areas are six in 2017 and five in 2023.** 2017:
Punjab, Sindh, Khyber Pakhtunkhwa, Balochistan, **FATA**, Islamabad. FATA
merged into KP in 2018, so 2023 has five. A codebase that says "four provinces
plus ICT" is wrong about 2017 and will drop FATA's 12 units.

**GB and AJK are not in either census panel.** The PBS 2023 layer draws them
but flags them outside the census, and neither panel has a single row for them.
Anything covering GB or AJK comes from elsewhere.

**The sub-district tier has three names and they are not a hierarchy.** PBS
publishes some units as tehsils, some as sub-divisions, some as sub-tehsils,
and which word it uses is not consistent between censuses or even within one.

| | tehsil | sub-division | sub-tehsil | other | total |
|---|---:|---:|---:|---:|---:|
| 2017 | 426 | 53 | 57 | 1 | **537** |
| 2023 | 413 | 134 | 43 | 1 | **591** |

A unit published as a tehsil in 2017 is often a sub-division in 2023 and the
same place either way. PBS's own boundary file draws all 591 in one set, so
treat them as **one geography** and never as nested tiers. (The "other" is
DE-EXCLUDED AREA RAJANPUR, present in both and matching exactly.)

## The join keys

Five identifiers name sub-districts in this repository and they are not
interchangeable. This is the single most common source of quiet error.

| key | what it is | count | safe to join on? |
|---|---|---:|---|
| `dds_id` | Census 2023 unit id, `DDS-XX-NNNN` | 591 | **Yes** — one polygon per census unit |
| `dd_id` | geoBoundaries ADM3, 2017 vintage (a hash) | 553 | **No, not alone** — see below |
| `adm3_pcode` | COD-AB | 442 of 591 | partial coverage |
| PBS `tehsil_code` | PBS's own numeric code | 650, unique | yes, within the PBS frame |
| Mouza `tehsil_code` | Mouza Census 2020's own | 595 | needs its own crosswalk |

**`dd_id` is not unique against the 2023 frame.** 591 census sub-districts
share **471** `dd_id`s: 436 polygons carry one unit, 28 carry two, 5 carry
three and 2 carry four. Units were split after the 2017 boundary was drawn.
Joining on `dd_id` and picking one row silently discards the rest — this is
what once limited the tehsil map to 331 of 591 shapes.

`tehsil_id` in the satellite and night-lights tables **is** `dd_id` under
another name. `dk` / `district_key` is the app's district slug on a 2015 frame
of 147 districts.

**geoBoundaries and PBS are different digitisations of the same borders, not
the same lines.** Across 155 districts the median IoU is 0.896 and the 5th
percentile 0.616. The worst are Skardu (0.27), Ghizer (0.35), Kharmang (0.42),
Chaman (0.51), Killa Abdullah (0.54), Karachi West (0.57), Korangi (0.58),
Karachi South (0.60), Batagram (0.62), Kolai Palas Kohistan (0.63), Karachi
East (0.66), Malir (0.75). GB is on a different district frame entirely — PBS
draws 10 districts, COD-AB 14. For anything census-keyed, use PBS's own
polygons.

Measure it yourself:
`raw_data/pbs_insight_explorer/economic_2026-09-27/derived/cod_vs_pbs_district_iou.csv`.

## Rural and urban

Two unrelated classifications share the words. Do not mix them.

**In the census panels**, `locality` is `all`, `rural` or `urban`, and `all` is
the **total, not a third category**. Summing the three double-counts everyone.

Coverage is not universal, because a district can be wholly one or the other:

| | districts with a value | total |
|---|---:|---:|
| 2023, all | 136 | 241,499,431 |
| 2023, rural | 130 | 147,614,729 |
| 2023, urban | 123 | 93,884,702 |
| 2017 (restated), all | 136 | 207,684,626 |
| 2017 (restated), rural | 131 | 132,013,789 |
| 2017 (restated), urban | 123 | 75,670,837 |

Rural and urban sum to the total exactly in both years. A missing rural or
urban row means the district has none of that kind, not that data is absent.

**In the Mouza Census 2020** the classification is of *settlements*, not
people, and has five values: Rural, Urban, PartiallyUrban, Forest,
UnPopulated. "Urbanish" in this codebase means Urban + PartiallyUrban. A
Mouza urban share and a census urban share are different quantities about
different things.

## What changed between 2017 and 2023

**Districts: 135 → 136**, in 127 groups — 106 exact, 7 renamed, 6 merged,
6 split, 2 boundary transfer. Full list in
`references/boundary-changes-2017-2023.md`; the shape of it:

- **Renamed (7):** FATA's agencies became districts — Bajaur, Khyber, Kurram,
  Mohmand, Orakzai, North Waziristan, South Waziristan.
- **Merged (6):** each Frontier Region was absorbed by its host district — FR
  Bannu into Bannu, FR D.I.Khan into D.I.Khan, and so on for Kohat, Lakki
  Marwat, Peshawar and Tank.
- **Split (6):** Chitral, Kohistan, Kalat, Karachi West, Killa Abdullah,
  Loralai.
- **Boundary transfer (2):** territory moved between Jhang and Toba Tek Singh,
  and between Kachhi and Nasirabad. Both districts persist; their areas do not
  match across the two censuses.

**Sub-districts: 537 → 591**, in 510 groups — 393 exact, 78 renamed,
39 restructured. Of the 39, **18 are clean one-to-many splits** whose parent
can be drawn across its successors; the other 21 are many-to-many, where the
correspondence inside the group does not exist to be drawn.

**Outside the census frame**, going from a 2015 boundary set to PBS 2023 adds
more: Hunza Nagar became Hunza and Nagar, Skardu became Skardu, Kharmang and
Shigar. And AJK's **Jhelum Valley district is universally called Hattian**,
after Hattian Bala, its seat — no string similarity finds that pair, and the
best fuzzy candidate is Thatta, in a different province.

## The two censuses publish different indicators

Not just different boundaries - different tables. Of **4,039 district cell
definitions** (table x indicator x column heading), **34 exist in both
censuses**; 58 if table numbering is ignored. At tehsil level it is 15 of
1,094. The panels' own note says the same thing from the other end: only 50
indicator labels match verbatim.

**What this rules out:** a picker that treats the census year as a facet of a
chosen indicator. For about 99 per cent of them the year is part of what the
indicator *is*, because only one census published it. Offer the years a
definition actually has, and expect that to be one.

What *is* a real facet within a year: locality (all, rural, urban) and sex.
Most definitions carry three variants and many carry nine.

The curated survey indicators are the opposite case - they are built as
2017/2023 pairs with a difference, so a year control works there. The 2017-2023
comparison that always works is population, because PBS restates it itself.

## The oracle: check, do not argue

**Census 2023 table 1 prints a `POPULATION 2017` column** — PBS's own
restatement of the 2017 count on 2023 boundaries. It sums to **207,684,626**,
the published 2017 total to the person, at every tier.

This turns a boundary crosswalk from an argument into a test: for any group of
2017 and 2023 units claimed to cover the same ground, the 2017 units' published
population must equal the 2023 units' restated 2017 population. If it does not,
the claimed relation is wrong. Every district and sub-district group in this
repository balances under that test.

Table 1 also carries PBS's own 2017–2023 growth rate for all 136 districts at
all three locality levels. **Prefer both to anything computed from a
crosswalk.**

## Traps

Each of these produced a wrong number that was caught.

- **Never sum across `unit_type`.** Districts and sub-districts are nested;
  adding them double-counts.
- **Never sum across `locality` or `sex`.** `all` is the total.
- **In tables 1, 3, 21 and 25 the sex split is in the *indicator*, not the
  `sex` column,** which stays `all`. Filtering on `sex` there returns several
  series stacked. A "safe" district filter that looked right returned 678 rows
  for 136 districts because of this.
- **Count units, not shapes.** When a split parent is drawn across its
  successors, walking the map's shapes visits it more than once: the 2017
  district layer totalled 213.4M people against a published 207,684,626, and a
  ranking listed Chitral twice with the same number.
- **A dash means different things in the two years.** 2017 recovers printed
  dashes as real zeroes (899,800 of 901,438 rows); 2023 leaves them NULL.
- **`is_rate` is PBS's own flag in 2023 and inferred from the label in 2017.**
  Treat the 2017 one as a guide.
- **Do not compare the two panels on `table_id` or `indicator`.** The numbering
  differs and only 50 indicator labels match verbatim.
- **Internal consistency proves nothing.** A table summing to its own total
  says only that it was read consistently. Check against the published figure
  or the restatement.

## Where this lives

| | |
|---|---|
| Boundary layers | `app/data/districts_2023_geo.js` (157), `app/data/tehsils_2023_geo.js` (591) |
| Crosswalks | `etl/census2017/district_crosswalk_2017_2023.csv`, `subdistrict_crosswalk_2017_2023.csv` |
| Per-unit decisions | `etl/census2017/census_unit_map.csv` — which shape each unit is drawn on, and why |
| Non-census districts | `etl/places/place_map.py` |
| Written up | `docs/stage2/census2017_crosswalk.md` |
