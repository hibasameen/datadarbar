# Stage 1: source inventory and reproducible census build

Delivered 25 September 2026. This implements the first two steps of the Stage 1 direction: register the project's sources and rebuild population and educational attainment from the original census tables. The public application remains at baseline `df174cf0e4c56db1162bb6c942db4c649dbd55c4` for all 40 checked data assets.

## Start here

- [Source inventory](inventory/README.md): 35 dataset families, all 25 published warehouse tables, all 40 published data assets, and 4,169 declared dependency files.
- [Detailed inventory](inventory/source_inventory.csv): periods, geography, population covered, source URLs, input locations, transformations, outputs, evidence, reuse conditions and blockers.
- [Build findings](census_build_report.md): source corrections, verification results and outstanding geographic decisions.
- [Input lock](inventory/census_inputs.lock.json): identities and SHA-256 checksums for all 294 required source/reference files, plus five baseline assets and the legacy mapping script.
- [Reproducibility verification](reproducibility_check.json): independent rebuild comparison.
- [Package identity](reproduction_package.json) and [public baseline check](baseline_integrity_check.json): archive checksum and unchanged-asset evidence.

The release is in the workspace at `data_darbar_warehouse/stage1/census-2026-09-25-release/`. Its identity is `census-audit-07f818302704d872`. The adjacent `census-2026-09-25-reproduction.tar.gz` contains the selected raw inputs, source code, baseline comparison inputs, reference output and instructions for a fresh-directory rebuild. These files are local artifacts, not a published dataset.

## What the census build covers

| Module | Original source | Source units | Population covered |
|---|---|---:|---|
| Population 2017 | Table 1; 135 district/agency/frontier-region PDFs | 135 | All ages, all sexes; separate sex counts |
| Education 2017 | Table 15; 135 PDFs | 135 | Age 5+, all sexes, overall locality |
| Population 2023 | Table 1; five provincial/ICT PDFs | 136 | All ages, including headcount-only enumeration |
| Education 2023 | Table 13; five provincial/ICT PDFs | 136 | Age 5+, all sexes, detailed enumeration, all localities |

The resulting 5,422 observations include source counts and derived attainment groups/rates. A unit is the administrative entity in that publication. The two counts above are not counts on one common district map. This package excludes other census modules, survey estimates and a national village panel.

Each observation records its original name, source identifier, dataset/year, population universe, raw cell, file, PDF page/text-line location, source URL, derivation numerator/denominator, missing-value status and arithmetic validation status. The 2017 identifiers are publication codes; the 2023 identifiers are generated source-name keys. Neither is a longitudinal geographic identifier.

## Rebuild from the workspace

Commands below start in the **Data Darbar workspace**, one level above the `datadarbar` application repository. They use `python3` from your chosen environment; this machine's working interpreter is `/Users/hibasameen/anaconda3/bin/python3`.

Requirements: Python 3.11, DuckDB 1.5.5, and Poppler's `pdftotext` on PATH. The verified runtime was Python 3.11.5 and Poppler 21.11.0. Another Poppler/Python version may change extracted text, Parquet bytes or the release fingerprint; compare outputs before accepting it.

Check `python3 --version` and `pdftotext -v` in the environment used for the build. This machine has multiple Poppler versions; the verified one is `/Users/hibasameen/anaconda3/bin/pdftotext`. Activating that environment, or placing its `bin` directory first on PATH, selects the recorded runtime. A fresh-directory run using Poppler 26.09.0 produced the same data files but correctly received a different runtime fingerprint. The strict byte-equality verification used 21.11.0.

```sh
python3 -m pip install -r datadarbar/etl/stage1/requirements.txt
python3 -m unittest discover -s datadarbar/etl/stage1 -p 'test_*.py'
python3 datadarbar/etl/stage1/build_census.py --out data_darbar_warehouse/stage1/census-new-run
```

Choose an output directory that does not exist. The offline build checks every locked input and the baseline before parsing. Missing or changed inputs stop the build. It writes to a temporary directory, verifies the Parquet files by reading them back, then exposes the completed release. It refuses existing output directories and output locations inside the application repository or source-input tree.

To check repeatability:

```sh
python3 datadarbar/etl/stage1/build_census.py --out data_darbar_warehouse/stage1/census-repeat-run
python3 datadarbar/etl/stage1/verify_release.py --release data_darbar_warehouse/stage1/census-new-run --repeat data_darbar_warehouse/stage1/census-repeat-run
```

All recorded artifacts and `build_manifest.json` must be byte-identical. Only `run.json`, which records execution time and absolute paths, is excluded. The verifier also checks recorded file hashes and the current code fingerprint.

For another source location, use `--source-root /path/to/pbs`. `--input-lock`, `--baseline-root` and `--legacy-crosswalk` are also configurable. Source paths inside the lock are relative to the PBS root; historical absolute locations inside the 2017 manifests are not used to open files.

## Inventory and source updates

`python3 datadarbar/etl/stage1/inventory.py` regenerates the inventory **and refreshes the census input lock** from the current local files. Run it only when deliberately reviewing source changes; do not use it to bypass an unexpected checksum failure. Review its file and checksum differences before a new release.

The reviewed registry is `etl/stage1/source_registry.py`. Non-census files are inventoried by name, size and role; their historical pipelines have not all been rerun. Every published asset has an inventory association, including an explicit unresolved-lineage record for the legacy PBS payload. Association is not proof of a complete transformation history. Five declared input paths are absent: the DHS raw directory and four NEPRA locations documented in the separate Adaad project. Other Adaad dependencies are recorded where explicitly referenced.

The 2023 source PDFs were captured from the [official PBS census page](https://www.pbs.gov.pk/census/) on 25 September 2026. Exact retrieval times, URLs and checksums are in `raw_data/pbs/stage1_sources/2026-09-25-official/retrieval_manifest.json`; the source page is archived alongside them. `fetch_census_sources.py` captures a fresh, separate snapshot. It is never called by the build. Adopting a later snapshot requires reviewing the registry/lock and comparing the release; downloading again does not replace the existing inputs. The 2017 PDFs use the archived local manifests and their source URLs; their original retrieval dates have not been independently established.

## Output guide

| File | Use |
|---|---|
| `source_observations.csv`, `.parquet` | Main source-native census data; retain status and universe columns |
| `source_units.csv` | Unit/module roster; original labels and identifier types |
| `indicator_dictionary.csv` | Units, universes, formulas, aggregation and comparability notes |
| `validation_checks.csv`, `issues.csv` | Category reconciliation and explicit source-symbol flags |
| `legacy_extract_comparison.csv` | Cell-by-cell check of 2023 historical CSVs against original PDFs |
| `legacy_map_crosswalk.csv` | Existing map-name mappings, all unreviewed; unresolved units retained |
| `legacy_map_candidates.csv`, `.parquet` | Diagnostic aggregation using the legacy mapping |
| `baseline_comparison.csv` | Candidate-to-public-data comparison; rates allow baseline rounding |
| `validation_report.json` | Coverage, validation and comparison counts; release caveats |
| `input_lock.json`, `build_manifest.json` | Exact input, code, runtime and output identities |
| `run.json` | Execution-specific time and paths |

Source dashes and blanks remain missing, distinct from printed zero. Education rates use the table's own age-5+ total. Counts are combined before computing rates; percentages are not averaged. If an education total fails reconciliation, derived values are withheld and source cells are retained with a discrepancy flag.

`legacy_map_*` files are diagnostic. Do not use them as a certified cross-year panel or publish their differences automatically. The next step is a dated geography registry and reviewed crosswalk, followed by an indicator-definition comparison. The release computes no 2017–2023 changes and assigns no inherited 2017 values to Keamari.
