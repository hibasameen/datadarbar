# The 2017 → 2023 geography crosswalk

Built 27 September 2026 by `etl/census2017/build_crosswalk_2017_2023.py`, with
the reviewed decisions in `etl/census2017/crosswalk_map.py`. **Nothing is
published.**

Every 2017 unit carries a documented relationship to 2023, or an explicit
statement that its correspondence is not resolved. Every group balances.

## The check is PBS's, not ours

Census 2023's table 1 prints a **POPULATION 2017** column beside the 2023
figure: PBS's own restatement of the 2017 count on 2023 boundaries. It sums
across the 136 districts to **207,684,626**, the 2017 total to the person, and
it exists at every tier — tehsil, sub-division and sub-tehsil sum to the same
figure.

That turns the crosswalk from an argument into a test. For each group of units
said to cover the same ground, the 2017 units' published population must equal
the 2023 units' restated population. A relation that does not balance is wrong.

This is why the workstream came in far under its 10–20 day estimate: the hard
part was expected to be judgement about correspondence, and the publisher had
already recorded it.

## Districts — 127 groups over 135 and 136 units

| Relation | Groups | |
|---|---:|---|
| exact | 106 | same name, same ground |
| renamed | 7 | the FATA agencies, now districts under the same name |
| merged | 6 | a Frontier Region absorbed into the district it adjoins |
| split | 6 | Chitral, Kohistan, and four districts that lost territory to a new neighbour |
| boundary transfer | 2 | two pairs that kept their names and exchanged territory |

**All 13 FATA units verify.** The seven agencies are one-to-one renames. The six
Frontier Regions were absorbed into the settled district each adjoins — FR Bannu
into Bannu, FR Peshawar into Peshawar — and in each case the two 2017 units sum
exactly to the 2023 district's restated count. Chitral's 447,625 is reproduced
exactly by Lower and Upper Chitral; Kohistan's 784,711 by its three successors.

### Two boundary transfers the check found

Jhang and Toba Tek Singh both kept their names and look like ordinary
identities. Jhang's restated 2017 count is **893 higher** than its published one
and Toba Tek Singh's is **893 lower**; Nasirabad and Kachhi differ by **1,932**
the same way. Each pair balances exactly together.

So those four districts are comparable **as two pairs**, and none of them is
comparable alone. Nothing in the names says so, and no name-matching crosswalk
would have caught it. This is the clearest argument for checking a crosswalk
against a published total rather than reviewing it by eye.

## Sub-districts — 510 groups over 536 and 591 units

| Relation | Groups | Matched by |
|---|---:|---|
| exact | 393 | name |
| renamed | 78 | name (63) or population (15) |
| restructured | 39 | — |

**Population alone settles 15 pairs.** Dera Bugti's `PHELAWAGH TEHSIL` and
`QADIRABAD SUB-DIVISION` are one place under two names, and only the 28,054 says
so. All five `TRIBAL AREA ADJ. …` units resolve the same way, to the named
sub-divisions that replaced them.

The tier word is dropped when comparing names: a place published as a tehsil in
2017 is often a sub-division in 2023 and the same place either way.

### What is deliberately not resolved

**65 units from 2017 and 120 from 2023 sit in 39 restructured groups.** The group
is declared to cover the same ground, and balances; the correspondence inside it
is left open rather than guessed.

- **Peshawar's one tehsil became six.** Badhber, Cham Kani, Mathra, Pishta
  Khara and Shah Alam were carved out of it, and the remnant kept the name.
- **Lahore City and Model Town both kept their names while exchanging
  territory**, so neither pairs with its namesake.

A name pair whose populations disagree is pulled into the restructured group
rather than reported as a match. That is the difference between a crosswalk that
is honest about its residue and one that looks complete.

## Resolving the restructured groups with geometry

The census tables cannot separate those 39 groups — the populations balance
under every pairing — so the question moved to boundaries. Three layers were
tried and only one answers it:

| Layer | Vintage | Verdict |
|---|---|---|
| geoBoundaries ADM3 | **2017** (their current Pakistan release) | this is the 2017 side |
| COD-AB, OCHA/HDX | boundaries created 2 Sep 2022 | predates the census and uses a different sub-district scheme; has none of Peshawar's new tehsils, and one `Gujranwala` polygon where the census has City and Saddar |
| **PBS Digital Census 2023** | 2023 | 591 polygons mapping one-to-one onto the census units by `dds_id` |

The PBS layer is the piece nothing else had: it is the census's own frame, so a
2023 unit and its polygon are the same object rather than two things matched by
name.

`resolve_restructured.py` measures each 2023 unit against **all 554** of the 2017
polygons, not a name-filtered shortlist — a new tehsil need not carry any part of
its parent's name, and Quetta's Panjpai scores 1% against the polygons whose
names contain "Quetta" while sitting almost entirely in one that does not.

| Verdict | Units |
|---|---:|
| settled — at least 75% inside one 2017 parent | **74** |
| divided, with shares reported | 26 |
| unreliable — the 2017 layer never drew a group member | 21 |

**17 of the 39 groups are settled outright.** Hassan Khel reads 91% inside FR
Peshawar, independently confirming a pairing the population had already made.

Three things the method is careful about:

- **Share is summed per parent**, not taken from the largest single polygon. The
  2017 layer splits Peshawar into four, so Cham Kani reads 67% in Peshawar IV and
  27% in Peshawar II while lying 94% inside the one census tehsil both belong to.
- **A missing polygon is not a parent.** The 2017 layer has no Model Town, so
  Model Town's ground sits inside LAHORE CANTT; crediting that share to Lahore
  City would describe the gap in the layer, not the history of the territory.
  Eight groups are in this position and are reported rather than answered.
- **A divided unit stays divided.** A tehsil drawing a quarter of its area from a
  second parent is not comparable to either alone. That is a finding.

## Exit test

> Every 2017 unit carries a documented relationship to 2023, or an explicit
> statement that it has none — and the aggregate populations reconcile under the
> mapping.

**Met at both tiers.** 127 district groups and 510 sub-district groups, zero
unbalanced, complete coverage of both sides.

Within-group correspondence is now given for 17 of the 39 restructured cases and
partly for the rest, from boundary geometry rather than arithmetic. What remains
is 26 units genuinely divided between two 2017 parents, and 21 in eight groups
where the 2017 layer never drew one of the units involved. The first is a fact
about the territory; the second would need a 2017 boundary set more complete
than geoBoundaries', which is the next thing to look for.
