# Queries for the Pakistan Bureau of Statistics

Drafted 26 September 2026. Not sent. Each item was found while building the Census 2023
district and tehsil panel documented in [build_report.md](build_report.md), and each is
reproducible from the published files alone.

Suggested route: the PBS data request form at
<https://www.pbs.gov.pk/data-information-request-form/>, copied to the Geography/GIS Section
for items 6 and 7.

---

## 1. Two published renderings of Table 14 disagree on one series, everywhere

In Table 14 (population and employment), the row **"Not L.F & Stud (15 to 24)"** differs
between the Excel release at `/result-excel/` and the PDF release at `/census/`, for every
administrative unit in the country — all 727 rows, in all five regions.

Abbottabad District, all localities:

| | All sexes | Male | Female |
|---|---:|---:|---:|
| Excel `table_14_kp_districts.xlsx` | 97,801 | 27,817 | 69,959 |
| PDF `table_14_kp_districts.pdf` | 112,505 | 33,294 | 79,186 |

Every other row in the same block — Population, Employed, Paid Employee, Own Account,
Employer, Unpaid Family Helper, Unemployed — is identical in both files. It is the only
indicator in Table 14 affected.

**Which figure is correct, and should one of the two releases be superseded?**

## 2. Which rendering is authoritative where the two disagree?

Beyond item 1, 25,525 individual cells differ between the Excel and PDF releases,
concentrated in the intermediate and graduate enrolment columns of Tables 12 and 13(a). A
representative case in Table 13(a) for Abbottabad has eight of ten columns agreeing to the
digit and two differing by exactly seven.

**Is one release a later revision of the other? If so, which, and as of what date?**

## 3. Topi Tehsil's rural population

`table_1_kp_districts.xlsx` gives Topi Tehsil (Swabi) a rural population of **382,562**,
which equals its total population. The PDF gives **307,695**. The difference, 74,867, is
exactly Topi's published urban figure, and rural plus urban reconciles to the total only
with the PDF value. Swabi district does not sum correctly using the Excel figure.

**Can the Excel file be corrected?**

## 4. Six place names in Table 1 are missing the letters "ALL"

In Table 1 only, and in no other table:

| Table 1 prints | Every other table prints |
|---|---|
| AI TEHSIL | ALLAI TEHSIL |
| KAR KAHAR TEHSIL | KALLAR KAHAR TEHSIL |
| KAR SAYADDAN TEHSIL | KALLAR SAYADDAN TEHSIL |
| TANDO AHYAR DISTRICT | TANDO ALLAHYAR DISTRICT |
| TANDO AHYAR TALUKA | TANDO ALLAHYAR TALUKA |
| KAG SUB-TEHSIL | KALLAG SUB-TEHSIL |

The deletion is exactly the string `ALL` in every case, which is what removing the word
"ALL" from locality labels would do to any place name containing those three letters.

**Can Table 1's unit names be corrected?**

## 5. Table 6's "KP — District Wise" file contains the whole country

Both the census page and the Excel page link `table_6_kp_districts.xlsx` under "KP —
District Wise". That file contains all 135 districts of Pakistan, ending with Surab,
Washuk, Zhob, Ziarat and Islamabad. There appears to be no KP-only Table 6, so KP's figures
exist only inside the national file.

**Is a KP-specific Table 6 available?**

## 6. Table 10's "KP — District Wise" link points at Table 1

On both pages, the Table 10 row's "KP — District Wise" link resolves to
`table_1_kp_districts`. The genuine files exist at the predictable URLs
(`table_10_kp_districts.pdf` and `.xlsx`) and were retrieved successfully.

**Can the links be corrected — and are there other slots with the same error?**

## 7. Tables 6, 7, 8, 10 and 13 have no sub-district breakdown

These five are published under a "District Wise" heading but contain district rows only,
while the other 23 carry the full district → sub-division → tehsil hierarchy.

**Is that deliberate — a disclosure-control or data-quality decision — or an oversight? If
tehsil-level figures exist for these tables, they would complete the sub-district panel.**

## 8. What does the printed dash mean?

In several tables the Excel release records `0` where the PDF prints `-`; in others the dash
is preserved; in a few, both conventions appear within one file. Reconciling the two
releases recovered 1,120,488 cells where the Excel had replaced a printed dash with zero.

**Is the dash "not applicable", "nil", or "not available"?** The three have different
consequences for every rate computed from those columns, and the distinction cannot be
recovered from the Excel alone.

## 9. Table 35's Excel files

All five Table 35 links on `/result-excel/` return 404, and no filename variant resolves.
**Are the workbooks recoverable?**

## 10. Are Tables 27 to 30 planned?

The published series runs 1–26 and then 31–35.

## 11. The mauza boundary layer

The GIS page states that PBS holds ArcGIS layers down to Mauza, and that mussavis were
scanned for accurate mauza boundaries in Khyber Pakhtunkhwa and Punjab.

The Data Dissemination Policy places block maps, coordinates and shapefiles in Tier 4, on
the stated ground that enumeration blocks "are operational sampling units rather than stable
administrative boundaries". The same table says public dissemination "should normally use
stable administrative areas such as Mouza/Deh, Patwar Circle, Tehsil/Taluka and District".

**Can the mauza-level boundary layer be supplied for research use** — explicitly excluding
census blocks and any coordinates — on the basis that mauza is among the stable
administrative areas the policy identifies as suitable for public dissemination?

---

### Attribution

Everything above is derived from files published by PBS and is reproducible from them. If it
is useful, the comparison code and the full cell-level difference logs can be shared.
