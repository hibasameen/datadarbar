# Travel time to care (release 2026-09)

The two tables here are the data behind Adaad's *The unequal road to care*
(September 2026 issue) and its method notebook *From maps to minutes*:
modelled travel time from every populated 1 km cell in Pakistan to the nearest
mapped health facility, on the Malaria Atlas Project's motorised (2019) and
walking-only (2020) accessibility surfaces (Weiss et al., *Nature Medicine* 26,
2020), weighted by WorldPop 2020, aggregated to Data Darbar's 147 districts and
553 tehsils. `build_web_warehouse.py` reads them into the public warehouse as
`health_access_district` and `health_access_tehsil`; `build_map_payload.py`
writes them into `window.DD_POV` (`health_districts`, `health_tehsils`) in
`app/data/poverty_data.js` for the map's Health → Travel Time to Care groups.

| file | rows | what |
|---|---:|---|
| `travel_time_districts_2026-09.csv` | 147 | population-weighted medians and means, unweighted medians and means, shares beyond 30/60/120 minutes; the piece's `district_access_mpi.csv` without its MPI columns (join `mpi_districts`) and with shares as per cent |
| `travel_time_tehsils_2026-09.csv` | 553 | the same by tehsil, means only; the notebook's `travel_time_tehsils.csv` as published |

Both were built by `build.py` and `build_tehsils.py` in Adaad's
`data/pk-health-access` and frozen with the issue on 9 September 2026; the
frozen copies with full source notes are at adaad.org/datasets/. What the
measure is and is not: the facility set is OpenStreetMap and Google Maps
hospitals and clinics, public and private together, with nothing on staffing,
opening hours or quality, so this is geographic access to a mapped point, not
to a working service; motorised assumes a vehicle, walking-only assumes none;
WorldPop understates Gilgit-Baltistan by about a third against the 2023
census. The tehsil grid holds 4,671 fewer cells than the district grid because
the two boundary files rasterise differently, so tehsil rows recombine to 22.10
motorised minutes nationally against the district build's 22.13. Manora
Cantonment has no valid cells and is blank.

```
python3 etl/health_access/build_map_payload.py   # DD_POV.health_* tables
python3 etl/build_web_warehouse.py               # the two warehouse tables
```
