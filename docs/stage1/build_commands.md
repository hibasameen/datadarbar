# Recorded build and inspection commands

Recorded for traceability on 25 September 2026. **None of the data build commands
below was executed in this task.** Commands are taken from project documentation
and deployment configuration at commit
`df174cf0e4c56db1162bb6c942db4c649dbd55c4`; their inputs and portability remain to
be audited in Step 2.

| Purpose | Working directory | Documented command | Source | Side effect / dependency |
|---|---|---|---|---|
| Census and survey dataset build | `etl/` | `python3 build_dataset.py` | `README.md` | Writes district-map outputs; requires the documented raw inputs. |
| Web warehouse build | Repository root | `python3 etl/build_web_warehouse.py --src /path/to/data_darbar_warehouse` | `WAREHOUSE.md` | Reads app JSON/JS, Mouza CSVs, indicator labels and a local warehouse; writes Parquet and catalogue. The path is a placeholder, not an available directory here. |
| Mouza crosswalk | `etl/mouza2020/` | `python3 build_crosswalk.py` | `etl/mouza2020/README.md` | Rewrites crosswalk CSV. |
| Mouza map payload | `etl/mouza2020/` | `python3 build_payload.py` | `etl/mouza2020/README.md` | Writes `app/data/mouza_data.js`. |
| Search-page generation | Repository root | `python3 scripts/build_seo.py` | `.github/workflows/deploy.yml` | Runs during deployment; writes search pages. |
| Search-page checks | Repository root | `python3 scripts/check_seo.py` | `.github/workflows/deploy.yml` | Runs during deployment; this is not a census/warehouse data audit. |
| Local website preview | `app/` | `python3 -m http.server 8000` | `WAREHOUSE.md` | Serves local files; does not rebuild datasets. |

The pinned deployment workflow builds and checks search pages before publishing
`app/`. It does not invoke the census or warehouse build commands. This establishes
what the workflow does, not that the deployed data are incorrect or out of date.

## Complete the byte archive from a local clone

The capture utility reads committed Git objects and writes to a **new folder
outside the checkout**. It does not change branches, rebuild data, execute
notebooks, commit files, push, or deploy. Any uncommitted local edits are left
untouched and are not used as baseline source data.

From the downloaded package directory, with the existing repository at
`../datadarbar`:

```bash
python3 scripts/capture_baseline.py \
  --repo ../datadarbar \
  --out ../datadarbar-baseline-df174cf \
  --live-base-url https://darbar.adaad.org
```

The utility defaults to the full baseline commit above. Omit `--live-base-url` for
an offline Git snapshot. A Git commit must already exist in the local clone; the
utility deliberately does not fetch or clone. Choose a different output folder
for each subsequent capture rather than overwriting evidence.

Outputs are `repository.zip`, `capture_manifest.json`, and (when requested)
`live/`. The live capture covers pinned data files, any additional files named by
the live catalogue, and selected application pages and scripts; it is not a
complete offline mirror of the website. It records individual failures and
byte-level matches to the pinned Git objects. It does not test data semantics.

Git LFS payloads, submodule contents, ignored files, untracked files and the local
raw warehouse need separate preservation. LFS pointers and submodule references
are flagged rather than silently called complete source-data backups.
