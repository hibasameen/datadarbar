# Local census comparison preview

This is a separate, offline preview. Its source is outside `app/`, which is the only directory deployed by the existing website workflow. The builder refuses to write inside `app/` or the frozen warehouse releases. No public website update is part of this work.

Open `local_preview/census/index.html` in the Data Darbar workspace. It works directly from disk: its data, map renderer, styles, logo, downloads and archived source PDFs are local. A server and internet connection are not required. The explicit PBS publication links open the original websites only when selected.

## Included

- All 127 common statistical comparison areas and 4,572 observations from `geography-8cee002c8d474fd6`.
- Population in 2017 and 2023, absolute change and percentage change; total, male, female and transgender counts.
- Province filters, search by comparison area or member district, keyboard-selectable map shapes, a table, area details and CSV exports.
- Education counts and rates as separate year snapshots. Cross-year education changes are deliberately unavailable because questionnaire equivalence remains uncertified. Rates use summed numerators and denominators, not averages of district percentages.
- Links to all 280 checksum-verified source PDFs, alongside the original publication links and source locators.
- Full release downloads, a boundary audit, and separate published population baselines for the four recently reviewed districts.

## Geography scope

`boundary_mapping.json` explicitly maps 130 existing website shapes into 127 comparison areas. Jhang + Toba Tek Singh, Kachhi + Nasirabad, and Karachi West + Keamari use dissolved outlines. The remaining older combined-area outlines are retained as illustrative geometry. None is certified as a census polygon.

Six areas with former frontier regions—Bannu, Lakki Marwat, Dera Ismail Khan, Tank, Kohat and Peshawar—are hatched, with numerical shading withheld. The boundary file does not separately identify those regions; their inclusion and full extent cannot be verified from that file. This is an unresolved extent check, **not proof that all frontier-region land is missing**. The complete statistical areas remain in the table, summary totals and downloads.

Seven invalid input shapes are repaired for display with Shapely `make_valid`; non-polygon remnants are omitted. The boundary audit records each repair. This does not certify geographical accuracy. AJK and Gilgit-Baltistan appear only as context outlines, outside this census release.

## Build and check

Run from the Data Darbar workspace root using the installed Python 3.11.5 / Shapely 2.0.5 environment:

```sh
python3 datadarbar/preview/census/build_preview.py
python3 -m unittest discover -s datadarbar/preview/census -p 'test_*.py' -v
node --test datadarbar/preview/census/preview.test.js
```

The UI tests use the existing repository's `jsdom` dependency and execute the actual preview JavaScript and D3 renderer. They test interactions, totals, map selection, missing-value propagation, weighted rates, source links, download initiation and the education-change restriction. They are not a substitute for a visual browser review.

The Python tests check the pinned release, unique assignments, population conservation, joint-area dissolution, archived-source hashes, withheld boundary extents and an independent byte-for-byte rebuild. `preview_build.json` records the input hashes, build code, renderer output hashes and geometry runtime. No timestamps or machine-specific paths enter the generated files.

To build another independent copy:

```sh
python3 datadarbar/preview/census/build_preview.py --out /private/tmp/datadarbar-census-preview-repeat
```

## Before public integration

1. Complete visual desktop/mobile and browser download checks. Automated browser access and the optional loopback server were blocked in this session because automatic approval review failed with an authentication error (401); neither action ran.
2. Verify the map extents against suitable census-vintage boundary evidence. All current outlines are explicitly illustrative; the six frontier-region extents are withheld from shading.
3. Review education questionnaires and universes before enabling education change calculations.
4. Integrate the reviewed preview into the site's normal navigation and data build, then publish only when requested.

The existing public application files and both frozen census/geography releases remain unchanged.
