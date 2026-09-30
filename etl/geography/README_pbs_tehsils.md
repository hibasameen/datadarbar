# The tehsil geography: PBS Census-2023 polygons

Since 27 September 2026 every tehsil-level layer on the map draws on one boundary file,
`app/data/tehsils_geo.js` (`window.DD_GEO_T`): PBS's own Digital Census 2023 tehsil layer
(economic.data.gov.pk `th.geojson`, cleaned copy in
`raw_data/geospatial/boundaries/pbs-census2023-2026-09-27/`). 649 polygons — the 591 Census-2023
sub-district units plus 32 AJK and 26 GB tehsils; the disputed-territory polygon is not drawn.

Two keys ride on each feature. `dd_id` is the PBS tehsil code (a three-digit string), the key the
poverty, school, health and Mouza Census groups are built on; `dds_id` is the census unit id, the key
the census panel uses. The `tehsil` and `tehsil2023` geographies in app.js are the same file read by
one key or the other. `dk` is the district polygon's key (via `etl/build_dataset.apply_crosswalk`),
`prov` the province as the app spells it, `merged` marks the seven ex-FATA districts.

It replaces geoBoundaries ADM3 (2017, 553 shapes), on which 51 census units shared 23 polygons and
55 had none, and it retires the separate `tehsils_2023_geo.js` that briefly served the census layer.

## Rebuilding

```
python3 etl/geography/build_tehsils_geo_pbs.py . ../raw_data/geospatial/boundaries/pbs-census2023-2026-09-27/pbs_tehsils_2023.geojson
python3 etl/geography/build_tehsil_satellite_pbs.py --tehsils <full-precision geojson> --pop <WorldPop tif> \
        --rwi <ind_pak_relative_wealth_index.csv> --viirs <viirs_pak_clips/> --out etl/geography/tehsil_satellite_pbs2023.csv
python3 etl/geography/build_poverty_payload_pbs.py .            # DD_POV.tehsils
python3 etl/schools/build_tehsil_access.py ... --tehsils app/data/tehsils_geo.js --out <dir>
python3 etl/schools/build_map_payload.py --stats ... --counts ... --pov app/data/poverty_data.js --out etl/schools/school_access_tehsil.csv
python3 etl/health_access/build_tehsils_pbs.py --tehsils app/data/tehsils_geo.js --index etl/geography/pbs_tehsil_index.csv ...
python3 etl/health_access/build_map_payload.py
python3 etl/mouza2020/build_crosswalk_pbs.py && python3 etl/mouza2020/build_payload.py
python3 etl/build_web_warehouse.py                              # warehouse tables keyed on the new dd_id
```

The satellite and access layers are recomputed on the new polygons, not re-keyed: RWI is the
population-weighted mean of Meta's cells (3 km WorldPop window as weight), WorldPop is a zonal sum,
night-lights are the filtered sum of lights per km² (1 nW floor, persistent flares masked), school
distance and travel time to care run the same code as the Adaad pieces with the new zones. National
aggregates reproduce the old build: WorldPop 220.2M (220.7M before, the difference is the part of Kashmir
the old layer drew); girls'/boys' middle-school distance 2.76/2.28 km in both; motorised travel time to
care 22.01 v 22.10 minutes; every Mouza Census national share identical to one decimal. The Mouza
crosswalk now joins 551 of 595 PBS tehsils by code; 24 "approx" placements on the old frame fall to one.

`etl/geography/pbs_tehsil_index.csv` lists every feature with its keys and names;
`census2023_units_to_pbs_geojson.csv` maps the 591 census units to their polygons (1:1 by name).

## Digital Census 2023 units and emigration layers

`build_dc_diaspora_payload.py` writes three more DD_POV tables from the PBS Insight Explorer pulls of
2026-09-27 (`raw_data/pbs_insight_explorer/`): `dc_tehsils` and `dc_districts` (23 unit types from the
Digital Census 2023 building enumeration, counts and per 1,000 people) and `diaspora_districts` (Bureau of
Emigration registrations by district of origin 2011–2024). Rates use the 2023 census population from the
Stage-2 warehouse; GB/AJK fall back to WorldPop 2020 (`pop_src`). District rows sum PBS districts onto the
district polygon's key, so Hunza+Nagar, Kharmang+Shigar+Skardu and the like are one row each. The map
groups are `dcCommerce`, `dcCommerceT`, `dcSocial`, `dcSocialT` (topic Economic Activity) and `diaspora`
(topic Emigration).
