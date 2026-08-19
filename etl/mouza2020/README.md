# Mouza Census 2020 — rural facilities layer

Builds the `Rural Facilities` layer on the Poverty Metrics page from PBS's
Mouza Census 2020, a hundred-per-cent count of 48,738 revenue villages.

```
python3 build_crosswalk.py   # PBS tehsil -> ADM3 dd_id, writes the crosswalk CSV
python3 build_payload.py     # aggregates counts onto polygons -> app/data/mouza_data.js
```

`pk-mouza-2020-tehsil.csv` mirrors `Aadaad/data/pk-mouza-2020`, which holds the
scraper and the full documentation of the source's quirks.

Two things to know before changing anything here.

**The join is many-to-one and that is deliberate.** PBS enumerates 595 tehsils
against the boundary file's 553, because it carries sub-tehsils created after the
polygons were drawn — the ex-FATA tehsils, the newer Balochistan sub-tehsils, the
three Cholistan units. Every figure is a count of mouzas, so summing several PBS
tehsils into one polygon is exactly right. `mouza2020_tehsil_crosswalk.csv` records
how each one was matched: `exact_name`, `variant` (same place, different spelling),
`parent` (a sub-tehsil folded into the unit it was carved from), `approx` (no polygon
exists for the area and it was placed in a neighbour — 24 polygons are affected and
the detail panel says so), and so on. Nothing is dropped: all 48,738 mouzas land
somewhere. Judgement calls live in `manual_map.py`, one line each with a comment.

**There is no published denominator.** PBS reports numerators only, and its own
indicator blocks disagree about how many mouzas answered — just 33 of 544 enumerated
tehsils give a single consistent base. Each exclusive block is therefore divided by
its own row sum, multiple-response blocks by the modal base (so they can exceed 100%),
and the spread between blocks is carried through as `bsp` and surfaced in the UI when
it exceeds 5%.
