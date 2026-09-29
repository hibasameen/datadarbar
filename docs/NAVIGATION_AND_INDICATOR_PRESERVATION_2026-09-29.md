# Data Darbar: simpler navigation with the full indicator collection

Design notes prepared 29 September 2026, following a comparison of the local update at http://localhost:8793/ with https://darbar.adaad.org/.

**Objective:** keep the breadth of Data Darbar's indicators while making it easy to find, understand and change a measure. A reader should get a useful map or chart immediately, then reveal the detail they need. Every existing valid indicator should retain an accessible route.

These are recommendations for the next revision. Suggested defaults require a check of definition, coverage and data quality before implementation. The usability observations below come from desktop inspection; the mobile requirements are proposed acceptance criteria.

## 1. What is going wrong

The update exposes too much of the classification system at once. Topic, subtopic, indicator family, source, indicator and metric can all appear as separate dropdowns. Readers must understand the distinctions between those labels before they can choose a measure.

The live site benefits from recognisable destinations, shorter indicator lists and deliberate starting views. Its simplicity should be carried forward into the larger collection.

| Observed behaviour | Why it matters | Design response |
|---|---|---|
| The local Places page opens on “0–4,” with a metric called “All.” | The subject and unit are unclear without examining the controls. | Open on Total population, with the year, unit and coverage stated plainly. |
| Education on the live map immediately shows total literacy. Education & schools in the update opens on “% Below Primary,” with six dropdowns and 78 indicator choices in that state. | Selecting a familiar subject does not reliably produce its most useful overview. | Give each topic an explicit starting measure and a short set of featured measures. |
| Searching “literacy” reports 395 matching district indicators, beneath the existing controls. | Search inherits the complexity of the catalogue and is hard to discover. | Put search first and group related results, while preserving access to every matching series. |
| Economy replaces visible topic links with a dropdown, then adds a dataset selector and chart list above the year and sector controls. | Readers cannot scan the available subjects, and useful controls move down the sidebar. | Restore visible topic navigation; place controls beside the chart they affect. |
| State displays a disabled dataset selector containing `fbr_tax_collection`, repeated beneath chart choices. | Implementation names occupy space without helping most readers choose. | Show readable source information with the selected chart; make exact table identifiers available in details. |

References: [live map](https://darbar.adaad.org/map.html), [live GDP and budget page](https://darbar.adaad.org/finance.html), [live Trade Atlas](https://darbar.adaad.org/trade.html), [local Places](http://localhost:8793/places.html), [local Economy](http://localhost:8793/finance.html), [local State](http://localhost:8793/state.html).

## 2. What “keeping the indicators” must mean

Preservation has three parts: keeping the values, keeping their definitions, and keeping a route by which someone can find them. Leaving a measure in a download while removing its ordinary browsing route would weaken the product.

Adopt the following rules:

- Retain existing valid indicators, years, supported geographies, breakdowns and source references.
- Keep both the published wording and a clearer display label. Both should be searchable.
- Retain totals, denominators and reference populations. They may move into a detailed table view, but remain findable and downloadable.
- Preserve distinct definitions even when their labels sound similar.
- Preserve original identifiers or a documented mapping from old identifiers to new ones, so saved links continue to resolve.
- Keep unusual and specialist measures accessible without requiring SQL.
- Keep questionable source values and unresolved definitions documented in the catalogue. Their presence there does not require presenting them as validated map values.
- Any removal from the public chart or map must have an explicit reason, such as an invalid value, unsupported geography or unresolved definition. Record the reason and retain the source route.

The main screen can show a small selection without limiting the collection. “Featured” must describe the presentation priority, never the boundary of what is available.

## 3. Use three connected routes into the same collection

| Route | Reader's need | What appears |
|---|---|---|
| **Explore a topic** | “Show me education in Pakistan.” | A useful default map or chart, followed by roughly 6–10 featured measures. |
| **Browse all indicators** | “I need a particular education measure.” | The complete topic library, organised under readable subheadings, with search and optional refinements. |
| **Inspect a measure** | “Show female literacy, or the survey estimate.” | Relevant breakdowns, alternative source versions, years, definition, coverage and downloads. |

These should share one selection state. Opening the full library must not discard the selected district, year or other valid choices. A search result should open the same view that browsing reaches.

Keep a visible “Browse all education indicators” link next to the featured list. Avoid making the full collection look like a separate technical product or an obscure advanced mode.

## 4. Clarify the site's destinations

Places, Economy and State can remain useful broad groupings. They need explicit destinations beneath them. The homepage should offer direct links to familiar subjects such as District & Tehsil Map, GDP & Industry, Trade Atlas, Money & Prices, Government Budget & Tax, and Courts & Public Services.

If the three existing homepage cards remain, put these named links inside them. A visitor looking for exports should be able to recognise “Trade Atlas” immediately.

On desktop, provide a compact, grouped topic list within Economy and State. Opening one group should reveal its topics without expanding every topic in the site. On a phone, a “Browse topics” sheet can serve the same purpose.

Use these proposed groupings:

| Area | Visible topic groups |
|---|---|
| Places | Population; Education & schools; Work & local economy; Health & disability; Women & gender; Housing; Utilities & infrastructure; Digital access; Poverty & living standards; Agriculture; Migration; Environment & satellite measures. |
| Economy | GDP & growth; Industry; Trade; Prices & interest rates; Money & banking; Exchange rates & external balance; Remittances. |
| State | Budget & tax; Courts & judges; Crime & policing; Energy; Public services & disasters. |

These labels are a proposed browsing vocabulary, not an instruction to discard the current taxonomy. Some can be grouped further after a navigation test. Migration and environment must retain named entry points even if they sit within a broader group.

Give each subject one principal home, with useful cross-links elsewhere. The federal budget should have its principal page under State; Economy can link to it. School access can be reached from Education and Public services. Multiple entry points can resolve to the same measure.

Keep Catalogue, Compare, Query and Methods readily available as secondary links. Compare should also be available beside a selected chart, so readers can enter with their first series already chosen.

## 5. Redesign the Places picker

The initial Places view should show a meaningful map without requiring any setup.

**Suggested first view:** Total population; district geography; the latest appropriate census; all people; all residences; no facility overlay selected. State the actual source year and coverage.

Use the left panel for finding a measure:

1. Search indicators.
2. A visible topic list or a compact topic chooser.
3. Featured measures for the selected topic.
4. “Browse all indicators in this topic.”

The active measure should be shown clearly above or beside the map. Put its year control close to the legend. Keep place search and the district/tehsil switch easy to find.

Keep province narrowing available near place search or in a clearly labelled geography control. Put school points and other overlays under “Map layers.” Geographic filtering and point overlays should not interrupt indicator selection.

Remove Subtopic, Indicator family and Source as simultaneous steps in the default selection path. Subtopics and families are valuable as headings in the full library. Source choice belongs to the selected measure or an optional library filter.

Search and the full library should open in a panel large enough to scan. In the inspected 1280 × 720 view, the control stack already pushes search toward the bottom; adding more fields would make this worse.

## 6. Separate measures from their breakdowns

A measure states what is being measured. A breakdown states whose value is shown. The interface should communicate both clearly.

For example, “Literacy rate” is the measure. Sex, age group and residence may be breakdowns, where the source supports them. A different survey definition is an alternative version, requiring its own source and definition label.

| Current collection contains | Preferred browsing presentation | What must remain accessible |
|---|---|---|
| Total, male and female literacy rates | Literacy rate, with sex choices where definitions align | Each original series, definition, year and source |
| Census and survey literacy estimates | One literacy family with separately labelled source versions | Both versions; neither silently replaces the other |
| Age-specific population counts | Population by age, with a labelled age selector or age profile | Every published band, including overlapping bands |
| Crop area, production and yield across many crops | Crop selection followed by Area / Production / Yield | Every crop and measure, with its own coverage |
| Imports and exports by product and partner | Trade overview, with drill-down by product and country | Full product detail and partner series |
| Court cases instituted, disposed and pending | Case flows, with explicit measure choices | All flows, stocks, court tiers and categories |

Do not group solely by matching text. “Under 5” and “0–4” require an equivalence check. Literacy among people aged 10+ is different from literacy among adults aged 15+. Attendance, enrolment and attainment are different measures. Rates and counts should share a display switch only when their populations and definitions align.

Where equivalence is uncertain, keep separate entries under a common heading and show the distinguishing definition. Cosmetic similarity must never create a false time series or erase a source distinction.

## 7. Show only relevant controls, with important breakdowns easy to reach

After a reader selects a measure, expose its useful choices. For literacy, sex is likely important enough to show directly. For crops, crop and measure are central. For trade, imports/exports and year belong in the main view.

Move less frequently used options into “More breakdowns” or “Refine view.” Do not hide every demographic choice by default: the placement should follow the subject and common tasks.

Use a summary such as “2023 · Age 10+ · Women · All residences · Census” beside the title. It must remain visible when the controls are collapsed.

| Choice | Placement rule |
|---|---|
| Year or period | Visible when there is a meaningful choice; otherwise display the single available date as text. |
| Sex | Directly visible for measures where the comparison is central; otherwise available in breakdowns. |
| Age group | Available within measures that support it; include the selected age in the title or summary. |
| Urban/rural | Available where published and meaningful; show the active selection. |
| Count/rate/share | Offer only mathematically valid presentations; name the denominator and unit. |
| Source version | Show the selected source and a “Compare sources” or “Other sources” route when alternatives exist. |
| Technical table identifier | Definition/source details and exports. |

Do not display disabled single-option selectors. Use readable text for fixed attributes. An unavailable comparison can have a short explanation, such as “Only published for 2023.”

Preserve compatible selections when switching measures. If a selected breakdown becomes unavailable, explain the reset: “This measure is available for all people only.” Never silently switch a person from female to total values or from tehsils to districts.

## 8. Build a complete, browsable indicator library

The library is essential to preserving breadth. A reader who does not know the exact search term must still be able to discover a specialist measure.

Within Education, for example, show headings for Literacy, School participation, Educational attainment, Gender gaps, School availability and Distance to schools. Show meaningful measures within each heading. Detailed source tables and reference populations can sit in a clearly named subsection.

The full library should support:

- Browsing by topic and family, with headings that can be expanded.
- Search across the whole collection or within the current topic, with the scope explicitly labelled.
- Optional filters for geography, source and year, available when requested.
- A source/table view for readers who know the published material.
- An “Other published measures” route for entries still awaiting classification.
- Result counts that distinguish measure families from detailed series.
- Pagination or “Load more” through every result.

The current search renderer limits the visible result list to 200 and asks the reader to keep typing for the remainder. The revised library should allow all matches to be browsed, even when the reader cannot supply a more specific phrase.

Avoid blank subtopics and invisible leftovers. An unclassified entry should still appear through source browsing and search until it receives a good public label.

## 9. Improve search without suppressing detail

Search should return a manageable set of meaningful groups, with the full matching detail underneath.

For “literacy,” the first results should make total literacy, female literacy and the main source versions easy to recognise. Detailed age bands should appear under an expandable literacy group. A query for “female literacy 2023” should resolve more specifically than the generic query.

Rank exact names and specific intent ahead of broad topic matches. Curated defaults can help order a generic query, but must not outrank a precise request for a specialist source or age group. Geography coverage can break ties; it must not bury a relevant regional measure simply because it covers fewer districts.

Search should recognise plain-language aliases and retain source terminology. Examples include literacy/literate, out-of-school/OOSC, electricity/power connection, and mother tongue/language. Check ambiguous terms such as “schooling” against several families rather than pretending they identify one exact measure.

Each result needs a short name, a distinguishing definition where necessary, source, available dates and geography. Prefer “136 districts with data” to “136 shapes.” Keep long source notes out of the result row and available after selection.

If a result exists only at district level while the map shows tehsils, display that fact and offer “Open district view.” Do not let the geography filter make the measure appear nonexistent.

## 10. Protect these subject areas explicitly

The following is a preservation checklist, not a closed list of featured indicators. It covers measures represented in the inspected inventory. Actual availability varies by source, place and year.

| Subject | Measures and distinctions to retain |
|---|---|
| Population & households | Population, density, growth, household size, urban/rural composition, age, sex, marital status, household relationships, language, religion, nationality, registration and homelessness. Keep detailed census cross-tabulations and reference populations. |
| Education & schools | Literacy, literate and illiterate populations, attendance, enrolment, out-of-school children, attainment, gender gaps, school availability and distance by school sex/level. Preserve census and survey versions. |
| Work & local economy | Participation, employment, unemployment where held, sector and status of employment, youth work/study, non-participation, establishments, workforce, markets and access to credit. |
| Health & disability | Disability and functional difficulty, education/employment among people with disabilities, illness and treatment, maternal care, immunisation, nutrition, child protection, and access to care. |
| Women & gender | Decision-making, barriers to work, reproductive health, fertility, tested literacy, early marriage and attitudes to violence. Also provide cross-links to female education and employment measures. |
| Poverty & living standards | MPI incidence, intensity, index and component deprivations; food insecurity; household welfare; relative wealth estimates with method labels. |
| Housing & utilities | Tenure, rooms/crowding, materials, structure type, construction period, housing-service combinations, water, sanitation, electricity, cooking fuel, waste, roads and village facilities. |
| Digital access | Devices, internet, digital finance, information sources and village connectivity. |
| Agriculture | Every available crop's area, production and yield, with full fiscal-year coverage. |
| Migration | Census migration and reasons, persons living abroad, registered emigrants by district, destination, skill and occupation, plus related remittance series. Keep migrant stocks and registered departure flows distinct. |
| Environment & satellite measures | Night-time lights, satellite population estimates, relative wealth, and available hazard exposure measures. Label measurement methods and dates. |
| GDP & industry | GDP, GVA, GNI, income per person, growth, sector shares/contributions, annual and quarterly activities, manufacturing output, manufacturing censuses and input-output relationships. |
| Trade | Imports, exports, balance, products through full available HS detail, partners, commodity groups, monthly/fiscal-year views and reconciliation notes. |
| Money & external balance | Inflation and price components, interest rates, money supply, banking, exchange rates, reserves, current account and remittances. Preserve frequency, units and source distinctions. |
| Budget & tax | Receipts, expenditure, detailed budget lines, tax heads/subheads, nominal amounts and valid derived shares. Keep estimates, revised estimates and actuals distinguishable where present. |
| Courts, policing, energy & disasters | Case flows, pendency, staffing and vacancies; reported offences and FIRs; plants, fuel, capacity and distribution performance; disaster events and separate impact measures. |

Cross-listing can improve discovery without copying the underlying series. School access belongs under Education and service access. Female employment belongs under Work and Women & gender. Satellite wealth should be reachable from Poverty and satellite methods.

## 11. Choose defaults deliberately

Use an explicit default for every topic. Do not select whichever entry sorts first. The defaults should have clear definitions, useful coverage and an immediately understandable unit.

| Topic | Candidate first view | Other featured choices |
|---|---|---|
| Population | Total population | Density, growth, household size, urban share, age structure |
| Education | Literacy rate, with age definition shown | Female literacy, out-of-school children, attainment, school access |
| Work | Labour-force participation, where coverage supports it | Employment, unemployment, female participation, sector of work |
| Health & disability | A reviewed overview chosen after checking coverage | Disability, maternal care, immunisation, nutrition, access to care |
| Poverty | Multidimensional poverty incidence | Intensity, index, deprivations, food insecurity, relative wealth |
| Utilities | A clearly defined electricity-access measure | Water, sanitation, cooking fuel, waste and connectivity |
| Agriculture | An explicitly selected major crop and production measure | Area, yield and “All crops” browsing |
| Trade | Export composition | Imports, partners, trade balance, changes over time |
| Money & prices | National inflation, with rate definition stated | Food/core inflation, policy rate, rupee and reserves |
| State | A clear topic choice such as tax collection as a share of GDP | Budget, courts, crime, energy and disaster impacts |

These are starting recommendations. There is no reason to force all Health datasets into one supposed national headline when their coverage differs.

The initial view should state its reference period. An older, well-defined survey measure remains valuable; it should display its actual date rather than inherit a newer date from another source.

## 12. Simplify Economy and State around questions

Use visible topic links such as “What Pakistan exports,” “How GDP has changed,” “What the government collects,” and “Court backlogs.” Keep shorter familiar labels where those are easier to scan.

Each topic should provide a title, a short explanation, a primary chart, relevant controls and source information. Variants such as Share of GDP / Share of total / Nominal rupees can appear as labelled choices close to the chart when they show the same underlying subject.

Related charts should be visible as named links or previews. A small companion chart can remain on screen when it helps explain the primary chart. Avoid an inflexible one-chart rule that separates information readers need together.

Remove the general dataset selector from everyday topic navigation. Show alternative datasets only when a reader has a meaningful choice between estimates, definitions or coverage. Use names such as “Quarterly national accounts,” with the table identifier in details.

Keep the good parts of the new State charts: distinct presentations for case flows, staffing, crime, energy and disaster impacts. Simplifying navigation should preserve the added analytical choices.

Replace the long “Getting around” banner with short guidance at the relevant interaction, such as “Select a sector to see its products.”

## 13. Keep the statistics honest as controls are simplified

Several details are essential to interpretation and must remain visible or one step away:

- **Reference population and denominator.** A percentage of children aged 5–16 differs from a percentage of the whole population. Name it.
- **Source and date.** Census, survey, administrative records and satellite estimates can measure different things.
- **Geography and boundary treatment.** The same name or displayed polygon does not by itself make two years comparable.
- **Missingness.** Distinguish unavailable, not collected, not mapped and zero. Do not fill a map with zeros for missing areas.
- **Change.** Offer it only for a verified comparable pair. Use percentage points for differences between rates when appropriate, and label percentage growth separately.
- **Aggregation.** Recompute rates from compatible numerators and denominators; do not average district rates into a national rate. Avoid double-counting shared geographic units.
- **Coverage.** “136 of 156 districts have data” is different from “Pakistan total.” Label partial totals accordingly.
- **Reported versus derived.** Identify calculated rates, indexed series, interpolations or splices and explain the method.

School counts, school locations and distance-to-school estimates are different products. Likewise, health-facility travel times do not imply a point layer of health facilities. Preserve these distinctions in both navigation and chart titles.

Important comparability notes should stay next to the figure. Longer methodology can sit behind “Definition & sources.”

## 14. Design mobile and accessible behaviour at the same time

On a phone, keep the selected measure, its date, the map/chart and a clear “Change indicator” button in the main view. Open topic browsing and search in a full-height panel. Keep the active selection summary visible after the panel closes.

Put details and rankings below the map or in a labelled panel. Avoid permanently showing a long filter form above the visualisation. Closing the picker should return to the chosen measure and preserve the map position when possible.

Provide keyboard access to the picker, groups, search results and chart controls. Return focus to the invoking control after a panel closes. Announce loading, changed results and empty states. Use explicit labels and visible focus indicators.

Offer a data table alongside each map/chart so selecting small shapes or hovering is not the only way to read values. Use colour with labels or patterns where necessary, and ensure controls remain usable at increased text sizes.

Search results should load progressively. The interface need not load every series' values to make the full collection discoverable.

## 15. Make preservation verifiable

Before changing navigation, record the current inventory using stable source identities. Count indicator definitions, geography-specific entries and observation series separately.

The checked-in Places index inspected for these notes contains **5,635 entries: 4,480 district entries and 1,155 tehsil entries**. These are index rows, not necessarily 5,635 unique statistical concepts. The warehouse catalogue lists **61 tables**. The earlier inspected homepage displayed a different indicator total; reconcile display counts against the release being served before using a public count as an acceptance target.

The index also contains **463 rows without the newer `h_topic` hierarchy field**, although they retain older topic labels. That does not prove that they are inaccessible. It does mean the new browser needs an explicit classification or fallback rule for those rows.

Create a migration ledger with one record per existing identity, mapping it to its new family, source version, supported breakdowns, browsing route and old-link alias. Include entries that will be source-table-only and the reason they cannot be visualised safely.

The release checks should establish:

1. Every valid original indicator has a destination in the new interface or a documented source-table route where visualisation is unsuitable.
2. No year, geography, supported breakdown or original source reference disappears through grouping.
3. Every entry is reachable through search, and every entry has a browsable topic or source-table route.
4. Search pagination exposes all matches, including those beyond the current 200-result display limit.
5. Old shared URLs open an equivalent selection or explain why that selection is unavailable.
6. Grouped variants retain the correct source, definition, values and export behaviour.
7. Measures with different definitions remain distinct, even when displayed under a shared family.

Counts alone are insufficient: merging two legitimate definitions can lower a count while still returning plausible-looking results. Check identity mappings and representative values as well.

## 16. Use real journeys as acceptance tests

The action counts below are design targets for testing, not measurements already achieved.

| Task | Target behaviour |
|---|---|
| Open the map | Population is already displayed with a meaningful title, date and coverage. |
| Choose Education | Literacy appears immediately; the reader need not choose a source table first. |
| Show female literacy | One obvious action from the default literacy view, where supported. |
| Find a rare attainment or housing cross-tabulation | Reach it through the complete library within a few clearly labelled browsing steps, without SQL or an exact internal name. |
| Compare census and survey literacy | Both versions are discoverable; their definitions and dates are visibly distinguished. |
| Find a specialist result beyond the first search page | “Load more” or pagination reaches it without requiring a narrower query. |
| Change district to tehsil | Preserve the indicator if available; otherwise explain availability and offer an appropriate route. |
| Select 2017–2023 change | Calculate only a verified comparison and expose boundary/definition limitations. |
| Find crop yield or an 8-digit trade product | Reveal detailed selection within the subject while retaining a clear path back to the overview. |
| Open a source total or denominator | Find it through the source-table or detailed-measure route and download it. |
| Reopen a shared link | Restore the measure, source, year, geography and breakdowns. |
| Repeat the main tasks on a phone and with a keyboard | Complete them without hover, hidden controls or inaccessible map-only interactions. |

Test with a general reader and a researcher. Measure whether they reach the right definition, the number of unnecessary choices, and whether they can explain what the displayed figure means.

## 17. Implement in stages

**Stage 1 — improve the first screen.** Set explicit defaults; put search first; restore visible topic navigation; remove disabled single-option selectors; make familiar destinations visible on the homepage. Record the inventory baseline before changing routes.

**Stage 2 — add the complete library.** Introduce featured measures plus a full browsable collection; reuse existing family labels as headings; add all-result pagination and fallback classification. Keep current detailed identities intact during this stage.

**Stage 3 — group verified variants.** Convert genuinely equivalent sex, age, residence and presentation variants into contextual controls. Map every original identity to the grouped view. Keep uncertain matches separate until reviewed.

**Stage 4 — improve search and continuity.** Add aliases, intent-aware ranking, grouped results, visible selection summaries, compatible state preservation and shared-link migration.

**Stage 5 — verify coverage and usability.** Run the preservation checks, the user journeys, mobile and keyboard checks, and targeted statistical checks for changed selectors and grouped variants.

This order lets navigation improve before a full semantic reclassification is complete. Unreviewed material remains in the complete library throughout.

## 18. Relationship to the existing design work

The existing `REDESIGN_IA_2026-09.md` already calls for a searchable indicator field, a browsable topic list and facets chosen after selecting a measure. Its Economy proposal also describes a visible topic tree. Those principles remain useful.

These notes sharpen the preservation requirements and recommend using the existing hierarchy as the structure of a browsable library. Implementing every hierarchy level as a visible dropdown is the main behaviour to change.

Relevant local inputs inspected: `app/data/places_index.js`, `app/data/warehouse/catalog.json`, `app/assets/js/places.js`, `app/assets/js/explorer.js`, and `docs/REDESIGN_IA_2026-09.md`. The catalogue inventory identifies what is held; it is not proof that every table is already represented by a working public visualisation.

**Definition of success:** a new visitor can reach a useful answer with very little setup, while a researcher can still find the precise source, indicator, period and breakdown they came for.
