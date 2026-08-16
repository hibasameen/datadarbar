# HIES 2024-25 at district level: what is defensible and what is not

Deep dive, 16 August 2026. Follows on from `HIES_CROSSWALK_BUG.md`, which fixed the
crosswalk. This note is about the problem the fix *revealed* rather than the one it
solved: HIES 2024-25 does not identify districts for urban households, so every
district figure Data Darbar can build from it is a rural figure.

All numbers below are computed from the microdata in
`PBS data/Microdata/HEIS/` and validated against PBS's published tables.

---

## 0. The most urgent thing, first

**The live site is currently in the worst of the three possible states.**

Commit `2956347` shipped the corrected ETL *and* the "rural households only" UI
labels, but did not regenerate `app/data/districts.json`. That file was last
touched on 9 August, by `f5e66f9`. So the site is serving the **pre-fix,
misassigned** numbers with **post-fix provenance labels** attached to them.

Multan's panel says "Rural households only. HIES 2024-25 records no district
identifier for urban households" over a figure that is actually rural Vehari's.

How different the live numbers are from the same indicator computed correctly,
across the 84 districts where both exist:

| Indicator | median abs. difference | 90th pct | max | share of districts off by >5pp |
|---|---:|---:|---:|---:|
| Electricity access | 4.9 pp | 43.5 pp | 74.0 pp | 49% |
| Piped water | 7.3 pp | 35.1 pp | 53.6 pp | 57% |
| Food insecurity | 5.8 pp | 15.0 pp | 26.8 pp | 51% |

Correlation between live and correct: 0.55 for electricity, **0.20 for piped
water**, 0.62 for food insecurity. Piped water as published is close to
statistically unrelated to piped water as measured.

Worst individual cases: Tor Ghar shows 99.0% electricity, correct figure 25.0%.
Kurram 74.2% against 6.2%. Jacobabad 92.9% against 39.4%.

Regenerating `districts.json` is a prerequisite for everything else in this note,
and should happen before any further design work. A label that dignifies wrong
data is worse than no label.

---

## 1. The main problem, stated precisely

It is three nested problems, and conflating them is what makes the discussion
hard. In order of severity:

### 1a. HIES 2024-25 has no urban district identifier — structural, not fixable

Verified directly on all 30,123 households in the release. The P-code is
`province(1) · division(1) · district(1) · stratum(1) · PSU(4)`. Cross-tabulating
the district digit against the stratum digit:

| district digit | stratum 1 (rural) | stratum 2 (urban) |
|---|---:|---:|
| 0 | 0 | **941 PSUs** |
| 1–9 | 1,212 PSUs | 0 |

Every urban PSU in the country carries district digit `0`. There are **127 rural
strata, each one district**, and **31 urban strata, each one entire division**.
The microdata's own `region` variable confirms the split exactly: 19,163 rural
households, 10,960 urban, matching PBS's published Table 2 household for
household.

So `2702` is not a district. It is every urban household in Multan division —
Vehari, Multan, Lodhran and Khanewal pooled, with nothing in the file
distinguishing them. No crosswalk, however carefully derived, recovers a district
that was never recorded.

**This is by design, and PBS says so.** HIES stratifies by province; district
representativeness is PSLM's job, not HIES's. HIES 2024-25 is the ninth round of
the *Provincial-level* HIES series. Districts appear nowhere in its stated scope.

The affected mass is large: **10,960 households, 36.4% of the sample, 38.7% of
weighted population.** (Sanity check: census 2023 puts Pakistan at 38.9% urban
across the same districts. The survey weights are consistent with the census, which
matters for §3b below.)

### 1b. A rural-only figure is not a district figure

This is the part that makes "just label it rural" insufficient on its own. Using
census 2023 urban shares to reconstruct what a whole-district figure would be —
rural stratum for the rural share, the household's own division's urban stratum
for the urban share — the rural-only number is off by:

| Indicator | median abs. bias | 90th pct | max | pop-weighted signed bias |
|---|---:|---:|---:|---:|
| Per-capita consumption | Rs 929 | Rs 3,393 | Rs 11,027 | **+Rs 1,771** |
| Electricity | 0.8 pp | 13.4 pp | 25.8 pp | +3.1 pp |
| Piped water | 3.3 pp | 9.9 pp | 30.3 pp | +3.2 pp |
| Food insecurity | 1.5 pp | 5.4 pp | 12.0 pp | −1.8 pp |
| Owner-occupied | 2.7 pp | 6.5 pp | 19.6 pp | −3.4 pp |

The bias is **systematic, not random** — every signed mean points the same way,
because urban households are richer and better served everywhere. Rural-only
understates district welfare, and understates it most exactly where readers care
most.

The worst cases are the places people look up first:

| District | urban % | rural-only per-capita | whole-district | understated by |
|---|---:|---:|---:|---:|
| Hyderabad | 83.1 | Rs 8,576 | Rs 17,268 | **101%** |
| Karachi West | 90.7 | Rs 11,024 | Rs 22,051 | **100%** |
| Larkana | 44.7 | Rs 12,303 | Rs 17,186 | 40% |
| Jamshoro | 46.7 | Rs 10,561 | Rs 14,516 | 37% |
| Bahawalpur | 37.8 | Rs 9,623 | Rs 12,844 | 34% |
| Faisalabad | 48.4 | Rs 13,391 | Rs 17,465 | 30% |
| Rawalpindi | 68.8 | Rs 15,864 | Rs 19,732 | 24% |

**For rankings this is disqualifying in specific cases.** On per-capita
consumption across 89 districts, the mean district moves 9.3 places when urban
households are put back; 24 districts move more than 10 places and 6 move more
than 20. Hyderabad moves **60 places**, from 75th to 15th. Anyone using Data
Darbar to say "Hyderabad is among Pakistan's poorest districts" would be reporting
an artefact of the sampling frame.

The consolation is that the *overall* ordering survives — Spearman correlation
between rural-only and whole-district is 0.87 for consumption and 0.90–0.99 for
the rest. Rural-only is a defensible measure of rural conditions and a rough proxy
for district ordering. It is not a district level, and it is badly wrong for the
29 districts (40.7% of the population) that are more than 35% urban.

### 1c. Sampling precision — the smaller problem

Worth checking, and it turns out to be the *least* of the three. Design-based
standard errors, linearised and clustered on PSU:

| Indicator | true between-district SD | sampling SD | **reliability** | median 95% CI |
|---|---:|---:|---:|---:|
| Per-capita consumption | Rs 3,020 | Rs 1,245 | 0.86 | ± Rs 1,613 |
| Electricity | 23.7 pp | 9.2 pp | 0.87 | ± 9.1 pp |
| Piped water | 16.2 pp | 7.8 pp | 0.81 | ± 8.8 pp |
| Food insecurity | 11.2 pp | 4.4 pp | 0.87 | ± 7.0 pp |
| Owner-occupied | 10.1 pp | 5.2 pp | 0.79 | ± 7.2 pp |

Reliability of 0.79–0.87 means roughly 80–87% of the observed variation across
districts is real signal. That is respectable — comparable to what published
district indicators usually carry. The rural stratum estimates are statistically
meaningful *as rural estimates*.

The exception is the thin tail. The median rural district has 12 PSUs; **34 of 90
have fewer than 12**, and Kurram (16 households, 1 PSU) and Orakzai (15, 1 PSU)
have no usable variance estimate at all. The current `n < 30` suppression rule
counts households, not PSUs, so it lets single-cluster districts through. That is
the wrong threshold: 16 households from one village is not a district sample.

**So the headline is: this is a coverage-bias problem, not a precision problem.**
Which is good news, because coverage bias is addressable and thin samples are not.

---

## 2. Two secondary issues found while checking

**Post-stratification calibrates rural households to total district population.**
`load_hies()` sets the calibration factor to `census_pop[district]["pop_total"] /
weighted_rural_pop`. Every current HIES field is a ratio, so this scalar cancels
and nothing published today is affected. But it is a live trap: the first count or
total ever added to the HIES block will silently inflate rural counts to
whole-district population. The denominator should be the district's *rural*
census population.

**Islamabad's urban stratum is district-identifying and is being discarded.**
Code `6102` has exactly one district in its roster. It is the only urban stratum
in Pakistan that resolves to a district, and the current patch drops it with the
other 30, throwing away 91 perfectly attributable households.

---

## 3. Solutions

Five options, assessed. They are not mutually exclusive; the recommendation is a
combination.

### 3a. Publish rural-only, labelled, and rename the indicator — *necessary, do first*

Rename the fields and the UI from "Multan — piped water" to "Multan (rural) —
piped water", so the reader sees the domain in the *value*, not in a footnote
below it. Carry `hies_coverage: "rural_only"` in the data. Suppress the 12
districts above 50% urban entirely, where a rural-only figure describes under half
the population and is actively misleading.

- **Defensibility:** high. It is exactly what the survey measured.
- **Cost:** ETL rerun plus label changes. Mostly already written.
- **Limitation:** does not give the reader a district figure, and the map still
  invites cross-district comparison of quantities that aren't comparable.

### 3b. Add a division-level urban layer — *recommended, high value, low risk*

The 10,960 urban households are perfectly good data at the level they were
sampled. Emit them as **division records**, not district records: "Multan
division, urban, 429 households". Every one of the 31 urban strata clears any
sensible sample threshold. Fold Islamabad's into its district record per §2.

- **Defensibility:** high. Estimates published at exactly the domain sampled.
- **Cost:** a new `divisions_urban.json` and a panel; roughly 1,200 populated
  cells currently discarded.
- **Benefit:** Lahore, the Karachi districts and Faisalabad have something honest
  to show instead of a blank, and the reader can see the urban–rural gap that is
  driving §1b rather than having it hidden.

### 3c. Synthetic census-weighted district estimates — *offer as a clearly-flagged modelled layer*

`district = (1 − urban_share) × rural_district + urban_share × urban_division`,
with the urban share from census 2023. This is a standard synthetic small-area
estimator, and the ingredients are unusually clean here: the census urban share is
measured not modelled, and HIES's own weighted urban share (38.7%) matches the
census (38.9%) almost exactly, so the two frames agree.

- **Defensibility:** moderate, *conditional on labelling*. The assumption is that
  a district's urban households resemble its division's urban households. That is
  reasonable for Multan or Peshawar and poor for Karachi division, where the
  largest district is 23% of the urban stratum.
- **Do not present these as survey estimates.** Separate field prefix
  (`hies_syn_*`), separate map layer, visible flag, and publish the division-share
  weight alongside so the reader can see how much is modelled.
- **Value:** it is the only route to a whole-district figure from this survey, and
  it is what the §1b table is built from — so if we know enough to say rural-only
  is biased, we know enough to publish the correction, provided it is labelled.
- **Guardrail:** suppress where the district is under 40% of its division's urban
  population. That kills the Karachi cases, which is correct.

### 3d. Move district welfare to PSLM — *the structurally right answer*

PSLM 2019-20 is a *district* round. Its microdata carries an explicit `district`
variable with 126 districts and a `region` variable, across 160,654 persons and
5,673 PSUs — urban and rural, district-identified. It already supplies 31 fields
to 124 districts on the site, and the district MPI is built from it.

For every indicator where the two overlap — piped water, toilet, mobile,
internet — **PSLM should be the district-level series and HIES should not appear
at district level at all.** HIES's role becomes the provincial and urban–division
layer, plus the indicators PSLM lacks (consumption, FIES).

- **Defensibility:** highest available. This is the survey designed for the
  question.
- **Cost:** low, the loader exists.
- **Limitation:** 2019-20, so five years stale, and it has no consumption module.
  Worth checking whether a district PSLM round has been published since.

### 3e. Full small-area estimation (Elbers–Lanjouw–Lanjouw) — *not now*

Model consumption on census-observable covariates in HIES, predict into census
2023. This is what the World Bank poverty maps do and it is the proper solution to
the urban attribution problem.

- **Defensibility:** high in the literature, but only with correct MSE estimation
  and a documented model.
- **Cost:** high. Needs census microdata Data Darbar does not have, plus real
  validation work.
- **Verdict:** the right eventual answer for a poverty map, not a fix for this
  bug. Park it.

---

## 3f. Can urban households be attributed to districts at all? — four methods tested, three fail

Asked directly, and worth recording because the negative results are what license
falling back on §3b.

**Method 1 — find another variable.** Inventoried all 26 microdata files. The only
geography anywhere in the release is `province`, `region`, and the P-code. No
tehsil, city, enumeration-block or frame identifier. All 941 urban PSUs carry
district digit `0`. **Nothing to work with.**

**Method 2 — fieldwork-date fingerprinting.** `section_info.dta` carries the
enumeration date, and PBS field teams move geographically, so PSUs enumerated
together might share a district. Tested on the rural stratum where the district is
known:

| date gap between two PSUs in the same division | P(same district) |
|---|---:|
| same day | 0.264 |
| 1 day | 0.464 |
| 2–3 days | 0.415 |
| 8–14 days | 0.191 |
| *baseline* | *0.232* |

There is a real signal — consecutive-day pairs are twice as likely to share a
district — but it peaks at 0.46 accuracy. **Not remotely identifying.**

**Method 3 — frame order in the PSU serial.** The PSU code decomposes as
`quarter × 1000 + frame index`, so the sample is a systematic draw from an ordered
frame. If PBS implicitly stratified by district before selection, frame index would
be district order. Tested by autocorrelation of PSU means against index distance:
0.09–0.21 across piped water, electricity and consumption, with the only elevated
value at lag 4, which is the quarterly rotation, not geography. **The frame index
carries no usable district structure.**

**Method 4 — borrow the district pattern from PSLM 2019-20.** The most promising
idea: PSLM has district × urban samples for 103 districts (mean 397 urban
households each), so use PSLM's *within-division* pattern to split HIES's division
mean. This can be validated honestly, because HIES's rural stratum has known
districts — so predict HIES rural district values from the HIES rural division mean
plus PSLM's rural within-division pattern, and check.

| Predictor of HIES 2024-25 rural district piped water | RMSE | MAE | corr |
|---|---:|---:|---:|
| Division mean alone (§3c) | **10.17 pp** | 6.54 | 0.82 |
| PSLM ratio transfer | 10.90 pp | 6.84 | 0.80 |
| PSLM additive transfer | 11.64 pp | 7.18 | 0.76 |
| PSLM 2019-20 district value directly | 12.76 pp | 8.49 | 0.74 |

**Every transfer is worse than doing nothing.** PSLM explains 4% of the
within-division variation (r = 0.21, optimal shrinkage slope 0.26); applying that
shrinkage improves RMSE from 10.17 to 9.95 pp, which is not worth the complexity or
the explanatory burden. Five years of staleness plus PSLM's own district sampling
noise destroy the signal.

**Method 5 — sharp bounds.** What *is* rigorous: given the division's urban PSU
distribution and the district's census share of division urban population, the
district's urban mean is bounded by the average of the worst and best fraction of
PSUs. This is assumption-free and correct. It is also usually uninformative — the
median bound is 95% of the division mean wide, and only 4 of 82 districts come in
under ±35%.

But it identifies exactly where the division figure *can* be published as a
district figure:

| District | share of division urban pop | division urban per-capita | sharp bound | width |
|---|---:|---:|---|---:|
| Islamabad | 100% | Rs 29,597 | exact | 0% |
| Dera Ismail Khan | 88% | Rs 11,111 | Rs 10,294 – 11,483 | 11% |
| Swat | 78% | Rs 15,828 | Rs 14,537 – 16,943 | 15% |
| Abbottabad | 51% | Rs 21,266 | Rs 18,411 – 24,107 | 27% |

Districts below 30% of their division's urban population have a median bound width
of **141%** — Karachi's cores, where the figure is meaningless.

**Method 6 — ask PBS.** The district is in their sampling frame; it was dropped
from the public release, not lost. This is the only route to genuine district-level
urban data from HIES 2024-25, and it costs an email. Worth doing regardless of what
else is built.

## 3g. Older rounds and the PBS website — what the survey series actually offers

Checked pbs.gov.pk directly rather than reasoning from the 2024-25 microdata alone.
This changes the picture in one important way and confirms it in another.

### PBS states the design in its own words

From the HIES 2024-25 sample design page, which settles §1a without needing the
microdata at all:

> **Urban Domain:** For urban domain, each administrative **division** for all four
> provinces has been considered as an independent stratum.
>
> **Rural Domain:** For rural domain, each administrative **district** in Punjab,
> Sindh and Khyber Pakhtunkhawa and each administrative **division** in
> Balochistan, has been considered as an independent stratum.

and on scope:

> …expected to produce reliable results at **provincial level with urban and rural
> break down**.

The coding-scheme write-up adds the rule that generates the `0` district digit:

> At position III, zero (0) processing code has been assigned if stratum is
> administrative **division**, and one digit other than zero has been assigned if
> stratum is administrative **district**.

This is an exact, independent confirmation of everything in §1a, including that
Balochistan's *rural* strata are division-level too.

### The series alternates, and the district rounds DO code urban by district

Seven PSLM District Level Surveys have been completed — **2004-05, 2006-07,
2008-09, 2010-11, 2012-13, 2014-15, 2019-20**. The 2010-11 coding scheme is the
decisive document, because it describes both designs side by side in one file:

> **a. District Level Survey** — A two-digit code at positions II and III has been
> assigned to indicate district (stratum) in each province. In urban sub-universe,
> big city and other urban areas of an admn **district** has been considered as
> independent strata…
>
> **b. Quarterly Provincial Level Survey (HIES Part)** — Big cities and other urban
> areas of an admn. **Division** has been considered as stratum. In rural areas,
> each district in Punjab, Sindh and Khyber/P.K provinces has been treated as
> stratum.

It even ships two separate annexures: *"Coding scheme for other urban areas
(district level)"*, listing every district with an urban code, and *"Coding scheme
for other urban areas (admin division HIES part)"*, listing 31 divisions. The
2006-07 scheme is district-coded for urban throughout.

**So the answer to "does an older HIES round help" is: not for consumption.** The
district-urban coding belongs to the *district survey* arm, and the consumption
module has always ridden on the *provincial* arm. HIICS 2015-16 uses language
identical to 2024-25 — "zero at position III if stratum is administrative
division". Every HIES round with a consumption module has division-level urban
strata. There is no year to fall back on for per-capita expenditure.

**But it helps a great deal for everything else**, because the district rounds
carry the social, housing, sanitation and ICT modules with urban district-coded.

### What is already sitting unused on disk

PSLM 2019-20 — already in `PBS data/Microdata/PSLM 2019-20/` — has an explicit
`district` variable *and* a `region` variable:

| | |
|---|---:|
| Districts | 126 |
| Urban households | 49,982 |
| Rural households | 110,672 |
| Districts with urban n ≥ 30 | 103 |
| Districts with urban n ≥ 100 | 84 |

And critically, **the site's existing `pslm_*` fields are already whole-district,
urban-and-rural combined.** I verified this by recomputing from microdata: the
published `pslm_pct_piped_water` correlates 0.9988 with the urban+rural
recomputation (mean absolute difference 0.13 pp) and only 0.913 with a rural-only
version. So Data Darbar *already* has defensible whole-district figures for piped
water, toilets, literacy, enrolment, mobile, internet and health — for 124
districts. It simply isn't leading with them, and is instead leading with a
rural-only HIES series for overlapping indicators. That is the cheapest fix
available and it requires no new data.

### An eighth district round is in the field right now

> Field operation of the PSLM District Level Survey 2026 has been started from
> **15th July, 2026**. The sample for PSLM Survey 2026 at the district level
> comprises **6,300 PSUs**, including 4,027 rural and 2,273 urban, and 25
> households enumerated from each selected block.

That is roughly 157,500 households, district-representative, urban included, and
it is being collected now. PBS also describes PSLM as the "only data source to
estimate Multidimensional Poverty at District level" — which is what the site's MPI
already uses. Worth planning the pipeline for the 2026 release rather than
retrofitting later.

### Also on the PBS estate

`https://pslm-sdgs.data.gov.pk/districtlevel` is a PBS district-level dashboard
carrying education/ICT, health, housing & sanitation, **FIES**, and migration, with
per-district downloads and a time series spanning 2006-07 to 2019-20. FIES matters
because `hies_food_insecurity_pct` is currently rural-only; the portal appears to
carry a district-level FIES series. The download endpoints are JavaScript-rendered
and will need a browser session rather than a plain fetch.

### The reframing that matters

Before concluding the urban gap is fatal, it is worth asking how much district
detail is actually at stake. Decomposing the variance of HIES's *rural* district
estimates — where the truth is known — into between- and within-division parts:

| Indicator | between-division | within-division |
|---|---:|---:|
| Per-capita consumption | 70% | 30% |
| FIES score | 64% | 36% |
| Electricity | 63% | 37% |
| Piped water | 60% | 40% |
| Food insecurity | 60% | 40% |
| Owner-occupied | 41% | 59% |

**Roughly two-thirds of the spatial variation in these indicators is between
divisions, not between districts within a division.** On that reading, publishing
the urban stratum at division level looks like a modest loss.

**Correction: that decomposition is measured on the rural stratum, and the urban
stratum behaves differently.** PSLM 2019-20 codes urban by district (§3g), so this
can be checked directly rather than assumed:

| Indicator | domain | between-division | within-division |
|---|---|---:|---:|
| Piped water | rural | 79% | 21% |
| Piped water | **urban** | 59% | **41%** |
| Flush toilet | rural | 76% | 24% |
| Flush toilet | **urban** | 48% | **52%** |

**Urban districts inside a division are roughly twice as heterogeneous as rural
ones.** Cities in the same division differ from each other far more than their
surrounding countryside does. So a division-level urban figure discards 40–50% of
the district-level variation, not the 20–25% the rural numbers implied. I had this
too optimistic in the first draft.

That sharpens rather than reverses the conclusion. The honest answer to "can it be
attributed" is: **no — not for consumption, in any round PBS has ever published —
and the cost is larger than it first appears.** Which is precisely why the
recommendation is not to lean on a division-level urban layer as a substitute for
district data, but to move the overlapping indicators onto PSLM, where genuine
district-level urban data already exists and is already on disk.

---

## 4. Recommendation

1. **Regenerate `districts.json` today.** Everything else is secondary to the fact
   that the site is currently serving 9 August's misassigned numbers under
   corrected labels.
2. **Ship 3a + 3b together.** Rural-only district fields, renamed so the domain is
   in the label, plus a division urban layer. Both are pure descriptions of what
   PBS sampled; nothing is inferred. Suppress the 12 districts above 50% urban.
   Restore Islamabad.
3. **Change the suppression rule from `n_households < 30` to `n_PSU < 8`**, and
   show sample size in the tooltip. This is what removes Kurram and Orakzai, which
   the current rule lets through.
4. **Then add 3c as a separate, visibly-modelled layer**, with the division-share
   guardrail. Not as a replacement for 3a.
4b. **Publish Islamabad, D.I. Khan, Swat and Abbottabad's urban figures at district
   level with their bounds** (§3f, method 5). These four are identified up to a
   width of 0–27%, and Islamabad exactly. Do not extend this below ~50% share.
4c. **Email PBS for the urban district variable.** It exists in their frame. This
   is the only route to real district-level urban HIES data and costs nothing.
5. **Move overlapping district indicators to PSLM (3d) — and promote this above
   step 2 in priority.** PSLM 2019-20 is already on disk, already loaded, and its
   `pslm_*` fields are already whole-district urban+rural (verified, §3g). For
   piped water, toilets, literacy, enrolment, mobile, internet and health, the site
   should lead with PSLM and drop the rural-only HIES version entirely rather than
   showing both. This is the largest defensibility gain available for the least
   work, and it needs no new data.
5b. **Plan for PSLM District Level Survey 2026**, in the field since 15 July 2026 —
   6,300 PSUs, ~157,500 households, district-representative with urban included.
   Building the loader against 2019-20 now means the 2026 release drops in.
5c. **Pull the PBS district FIES series** from `pslm-sdgs.data.gov.pk/districtlevel`
   to replace the rural-only `hies_food_insecurity_pct`. Needs a browser session;
   the endpoints are JavaScript-rendered.
6. **Fix the calibration denominator** (§2) before any HIES count field is added.
7. **Leave `food_share` withheld.** The item-to-block recall mapping is unresolved
   for 108 of 285 item codes and the share swings between 6% and 48% on the
   treatment; publishing a number that lands on PBS's 36.72% would be fitting to
   the answer.

---

## 5. What was validated

Every aggregate below reproduces PBS's published HIES 2024-25 tables from the
microdata, which is what licenses the district-level arithmetic above.

| Quantity | Computed | PBS published | Difference |
|---|---:|---:|---:|
| National monthly consumption per capita | Rs 13,300 | Rs 13,240 | +0.5% |
| National monthly consumption per household | Rs 79,785 | Rs 79,150 | +0.8% |
| Urban monthly per household | Rs 96,806 | Rs 95,533 | +1.3% |
| Rural monthly per household | Rs 68,091 | Rs 67,894 | +0.3% |
| Average household size | 6.00 | 5.98 | +0.3% |
| Rural households | 19,163 | 19,163 | exact |
| Urban households | 10,960 | 10,960 | exact |

Standard errors are design-based: linearised residuals aggregated to PSU, with a
finite-population correction omitted (conservative). Reliability is
`(observed variance − mean sampling variance) / observed variance`, the standard
shrinkage diagnostic.

---

## Sources

- PBS, [Household Integrated Economic Survey (HIES) 2024-25 report](https://www.pbs.gov.pk/wp-content/uploads/2020/07/HIES-2024-25-Report-Final-1.pdf)
- PBS, [HIES 2024-25 landing page](https://www.pbs.gov.pk/household-integrated-economic-survey-hies-2024-25/)
- PBS, [Sample Design](https://www.pbs.gov.pk/sample-design/)
- PBS, [PSLM 2010-11 Round VI coding scheme (District/HIES Survey)](https://www.pbs.gov.pk/wp-content/uploads/2020/07/Coding-scheme-for-Round-vi.pdf) — the document showing both designs side by side
- PBS, [PSLM 2006-07 Round III coding scheme](https://www.pbs.gov.pk/wp-content/uploads/2020/07/coding-scheme-2006-07-Round-III.pdf) — district-coded urban
- PBS, [HIICS 2015-16 coding scheme](https://www.pbs.gov.pk/wp-content/uploads/2020/07/CODING-HIICS-2015-16.pdf) — same division-urban rule as 2024-25
- PBS, [PSLM district-level SDG dashboard](https://pslm-sdgs.data.gov.pk/districtlevel)
- Microdata: `PBS data/Microdata/HEIS/` (weight, roster, sec_6a, sec_05m1–m4)
- `PBS data/Microdata/PSLM 2019-20/stata data/`
- Prior note: `HIES_CROSSWALK_BUG.md`
