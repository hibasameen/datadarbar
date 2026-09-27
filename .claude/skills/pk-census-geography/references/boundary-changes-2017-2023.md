# Every boundary change between Census 2017 and Census 2023

Generated from the crosswalks in `etl/census2017/`, which are themselves
checked against PBS's restated 2017 population: for every group below, the
2017 units' published population equals the 2023 units' restated figure.
Regenerate with `.claude/skills/pk-census-geography/references/regenerate.py`.

## Districts

135 units in 2017, 136 in 2023, in 127 groups: 2 boundary transfer, 106 exact, 6 merged, 7 renamed, 6 split.

### Renamed (7)

FATA's agencies became districts.

| 2017 | 2023 | 2017 population |
|---|---|---:|
| Bajaur Agency | Bajaur District | 1,090,987 |
| Khyber Agency | Khyber District | 984,246 |
| Kurram Agency | Kurram District | 615,372 |
| Mohmand Agency | Mohmand District | 474,345 |
| North Waziristan Agency | North Waziristan District | 540,546 |
| Orakzai Agency | Orakzai District | 254,303 |
| South Waziristan Agency | South Waziristan District | 675,215 |

### Merged (6)

Each Frontier Region was absorbed by its host district. Two 2017 units share one 2023 shape, so their figures add — or, for a rate, average on 2017 population.

| 2017 | 2023 | 2017 population |
|---|---|---:|
| Bannu District + FR Bannu | Bannu District | 1,210,183 |
| Dera Ismail Khan District + FR D.I.KHAN | Dera Ismail Khan District | 1,693,594 |
| FR Kohat + Kohat District | Kohat District | 1,111,266 |
| FR Lakki Marwat + Lakki Marwat District | Lakki Marwat District | 902,138 |
| FR Peshawar + Peshawar District | Peshawar District | 4,331,959 |
| FR Tank + Tank District | Tank District | 427,044 |

### Split (6)

One 2017 unit became several. Its count cannot be divided between them; drawn across all of them instead.

| 2017 | 2023 | 2017 population |
|---|---|---:|
| Chitral District | Lower Chitral District + Upper Chitral District | 447,625 |
| Kalat District | Kalat District + Surab District | 412,058 |
| Karachi West District | Karachi West District + Keamari District | 3,907,065 |
| Killa Abdullah District | Chaman District + Killa Abdullah District | 758,354 |
| Kohistan District | Kolai Palas Kohistan District + Lower Kohistan District + Upper Kohistan District | 784,711 |
| Loralai District | Duki District + Loralai District | 397,423 |

### Boundary transfer (2)

Both districts persist, but territory moved, so the two years cover slightly different ground.

| 2017 | 2023 | 2017 population |
|---|---|---:|
| Jhang District + Toba TEK Singh District | Jhang District + Toba TEK Singh District | 4,934,128 |
| Kachhi District + Nasirabad District | Kachhi District + Nasirabad District | 797,779 |

## Sub-districts

537 units in 2017, 591 in 2023, in 510 groups: 393 exact, 78 renamed, 39 restructured.

Renamed pairs are matched by name inside a district group, or — where the
names give nothing — by a population that is identical and uniquely so
within the group. Dera Bugti's Phelawagh Tehsil and Qadirabad Sub-Division
are one place under two names, and only the 28,054 says so.

### Restructured, resolved as clean splits (18)

One 2017 unit became several 2023 ones, so the parent is drawn across its
successors. The remaining restructured groups are many-to-many and stay
undrawn: the correspondence inside them does not exist to be drawn.

| 2017 unit | drawn on |
|---|---:|
| Alpuri Tehsil | 3 shapes |
| Bannu Tehsil | 4 shapes |
| Chaman Tehsil | 2 shapes |
| Chitral Sub-Division | 2 shapes |
| Dalbandin Tehsil | 2 shapes |
| DIR Sub-Division | 2 shapes |
| Duki Tehsil | 4 shapes |
| Haripur Tehsil | 2 shapes |
| Jhal JAO Sub-Tehsil | 2 shapes |
| Kohat Tehsil | 2 shapes |
| KOT Addu Tehsil | 2 shapes |
| Ladha Tehsil | 2 shapes |
| Lakki Marwat Tehsil | 2 shapes |
| NAL Sub-Tehsil | 2 shapes |
| Nushki Tehsil | 2 shapes |
| Panjgur Tehsil | 2 shapes |
| Peshawar Tehsil | 6 shapes |
| Sanhri Tehsil | 4 shapes |

### Restructured, left undrawn (47 units)

Several 2017 units became several 2023 ones. The group balances; which
unit corresponds to which does not follow from that, and is not guessed.

| district | units |
|---|---:|
| Abbottabad District | 2 |
| Bahawalnagar District | 2 |
| Buner District | 2 |
| Chakwal District | 2 |
| Charsadda District | 2 |
| Dera Ismail Khan District | 3 |
| Gujranwala District | 2 |
| Jhang District | 1 |
| Kachhi District | 1 |
| Kalat District | 2 |
| Kharan District | 2 |
| Kohistan District | 4 |
| Lahore District | 2 |
| Mansehra District | 2 |
| Mardan District | 2 |
| Musakhel District | 2 |
| Nasirabad District | 3 |
| Pishin District | 2 |
| Quetta District | 2 |
| Rawalpindi District | 2 |
| Sheikhupura District | 2 |
| Toba TEK Singh District | 1 |
| Torghar District | 2 |

