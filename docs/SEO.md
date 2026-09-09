# Search discovery

python3 scripts/build_seo.py builds 19 initial district profiles and 14 documented dataset pages, plus their hubs, standalone About/Methodology pages, canonical metadata, robots.txt and sitemap.xml. python3 scripts/check_seo.py checks rendered metadata, structured data and downloadable links. Both run in the deployment workflow.

Profiles draw directly from app/data/districts.json and preserve 2023 district units, age groups and missing values. The initial set has population/literacy coverage and no explicit boundary-change flag; the absence of a flag is not an independent boundary audit. CSVs are generated from the same records. Dataset pages read warehouse catalog definitions and point to existing Parquet files. Full About/Methodology content is extracted from the maintained modal source; unsupported JavaScript interpolation fails the build.

The generated HTML/CSVs are committed for inspection and direct static serving. Regenerate after updating source data. Do not edit generated pages by hand. Analytics runs only on darbar.adaad.org and still labels engagement events separately from page views.

After publication, submit https://darbar.adaad.org/sitemap.xml in Search Console. Inspect a district and a dataset URL. Track non-brand query impressions/clicks, organic landing pages and downloads. Evaluate page visits separately from GoatCounter events; legacy referrals remain in historical reports.
