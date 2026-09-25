# Data Darbar Core — Stage 1 scope

**Version:** 0.1 · **Scope date:** 25 September 2026  
**Project lead:** Hiba Sameen  
**Stage:** Audit and prototype  
**Current task:** Step 1 — fix the scope and preserve a baseline

## Objective

Establish a traceable geographic foundation and prove it on a small, reproducible
census-based data package. An independent analyst should eventually be able to
follow a published number from its source geography, through any aggregation,
to the website and research download.

Step 1 records our starting position. It does not certify the data, correct any
historical issue, or complete the wider Stage 1 audit.

## Scope boundaries

| Area | Included in Stage 1 | Deferred |
|---|---|---|
| Geography | Inventory the geographic frames currently used; draft a district register covering the 2017 census, 2023 census and current map; document relationships and audit the existing tehsil crosswalk. | A new national boundary collection or national village-level panel. |
| Core prototype | Selected census population and education counts and rates. Compare 2017 and 2023 only where source definitions, coverage and geographic equivalence support it. | Migration of every census indicator and every warehouse table. |
| Additional social indicators | A small selection of PSLM district indicators, conditional on source and geographic checks. Keep observation periods explicit. | New small-area estimation and automatic inclusion of HIES, LFS or PDHS district estimates in the default research view. |
| Lower-level feasibility | Inspect available Mouza files, identifiers and documentation; establish access and reuse requirements. | A commitment to settlement-level outputs before access and linkage have been demonstrated. |
| Existing products | Preserve the current interface and hosting. Propose targeted corrections where evidence justifies them. | Website redesign, hosted API, new macroeconomic/trade modules or platform migration. |
| Release discipline | Baseline identification, checksums, documented inputs, validation rules and one reproducible example. | A claim that this prototype is a complete national research release. |

The administrative coverage of each source is retained as published. No fixed
national district count is adopted at this point, and missing regions or records
are not silently filled from another source.

## Working rules

1. Preserve original observations, names, codes, geographic levels, periods and
   covered populations. A value's display polygon is not evidence of its source
   geography.
2. Record source units, boundary versions and consistent analytical areas as
   distinct concepts. Do not declare a boundary reference year from a filename
   or file creation timestamp.
3. Treat name matching, exact geographic equivalence, verified aggregation and
   estimated allocation as different relationships. Leave unresolved cases
   explicit rather than forcing a match.
4. For aggregation, retain numerators and denominators where available and
   recompute rates. Do not average percentages without a justified weighting rule.
5. Preserve uncertainty and coverage restrictions in the downloadable data, not
   just in map labels. Exact numerical equality is not, by itself, proof of an
   inherited or duplicated observation.
6. Treat existing correction notes as test cases to verify against the baseline;
   do not assume that a documented historical issue remains live.
7. Do not rebuild production data while establishing the baseline. Subsequent
   fixes require before/after comparisons and separate review.

## Six Stage 1 deliverables

| Step | Deliverable | Acceptance condition |
|---|---|---|
| 1. Scope and baseline | This specification, a fixed repository reference, file identities, preservation status and recorded build commands. | We can distinguish what has been preserved as bytes, what is identified only by Git object, and what remains unavailable or unverified. |
| 2. Source inventory | Source-to-output inventory and dependency table. | Every prototype dataset has a traceable path or an explicit missing-input blocker. |
| 3. Geographic foundation | Draft district register, source crosswalks, boundary registry and analytical-area membership. | Every prototype source unit is linked through documented evidence or marked unresolved. |
| 4. Targeted audit | Issue register, evidence and regression tests. | Each issue is verified resolved, fixed, excluded, or outstanding with consequences recorded. |
| 5. End-to-end prototype | Selected source-linked and harmonised indicators, validation report and worked analysis. | A reviewer reproduces the example without undocumented manual joins. |
| 6. Mouza feasibility | Recommendation on a mouza pilot, tehsil panel or published aggregates. | Recommendation follows inspected identifiers, linkage evidence and access/reuse conditions. This work does not block the district prototype. |

## Baseline protocol and current status

The repository baseline is commit
`df174cf0e4c56db1162bb6c942db4c649dbd55c4`, observed on `main` on
25 September 2026. Its recorded committer timestamp is 13 September 2026,
23:54:14 UTC. The working branch is `stage1/scope-baseline-2026-09-25`.
The fixed commit, rather than the moving branch name, defines the baseline.

The repository reference identifies all committed code and data at that commit.
The accompanying package records all 39 file identities beneath `app/data/`,
including 25 Parquet files and `catalog.json`. Those identities are Git blob
hashes, not locally calculated SHA-256 hashes of downloaded data files.

Fourteen available conversation/project uploads have been copied without
alteration and SHA-256 verified in the accompanying package. They remain
separate from the repository baseline: their membership in the current build has
not been established. These source-file copies are not uploaded to the public
working branch.

**The offline baseline is partial.** Complete repository byte downloads and live
JSON/Parquet capture were unavailable in this runtime. The source version has
been pinned, but deployed data equality, a full offline archive and access to the
local raw warehouse have not been verified. The supplied capture utility can
archive the pinned Git objects from an existing local clone and optionally save
live data with their actual retrieval times.

A successful later capture is a new dated evidence record; it must not be
backdated as if it were obtained during this session. No pipeline, uploaded
notebook, or production rebuild has been run in Step 1.

## Ownership and changes

Hiba Sameen owns the analytical scope and decisions about comparability. Data
engineering work implements and tests those decisions. An independent analyst
review is required before moving from the prototype to the wider release; no
reviewer has yet been assigned.

Scope changes should be recorded as a new version, stating the added work,
dependencies and effects on the acceptance conditions. Keep corrections and
production deployment separate from this baseline branch.

## Next work item

Build the source inventory from this baseline, beginning with the census
population and education pipeline. The first audit should trace source keys,
intermediate tables, map JSON/JavaScript and warehouse Parquet, including the
historical district-split cases recorded in `CENSUS_2023_SPLIT_DISTRICTS.md`.

## Evidence behind the starting position

Repository evidence is fixed at the baseline commit: `README.md`, `WAREHOUSE.md`,
`.github/workflows/deploy.yml`, the `app/` tree, and the complete `app/data/` and
`app/data/warehouse/` tree responses. The capture package names the associated
Git objects and records its local checksum checks. Scope rules and deliverables
above are project decisions for this stage, not claims that those checks have
already been completed.
