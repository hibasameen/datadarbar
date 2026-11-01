# Economy and State on the Places pattern

Built 23–24 October 2026 from the mockups on the redesign canvas (row 6,
"Economy and State on the Places pattern") and the "Chart palette" artboard.

## The template (explorer.js, explorer.css)

Both pages now work the way Places does: one view at a time.

- **Rail.** Groups, with only the current group open; the others show a count
  and a chevron. An **Overview** entry at the top; a link at the bottom to the
  other explorer. `DDExplorer.mount({nav: true, tabsEl, overview, links})`.
- **Overview.** Each page opens here (`state.html` with no query,
  `finance.html#t=overview`): one tile per group with a headline read off the
  payload, and the group's topics. Clicking a tile opens its first topic.
- **Head.** Eyebrow (group) · title (topic) · a two-line dek with *Read more* ·
  Share and CSV. The chart starts about 250–300px from the top, against
  300–360 before.
- **Tabs.** A topic's charts are tabs above one chart (`DDExplorer.tabs`).
  Coverage is a single line under the tabs, not a boxed strip.
- **Controls.** Choices that drive the whole topic sit in one row above the
  chart (`.topic-ctl`); view toggles stay on the chart.
- **Linked view.** A chart's drill-down sits in a panel on the right
  (`.pane-side`), the way Places shows a district: the year's sub-sector mix
  beside the structure chart, the year's breakdown beside the growth chart,
  the partner's profile beside the partners chart. Below 1180px it drops
  under the chart.
- **Phone.** The rail collapses behind a "Topic · <current>" bar on both
  pages (`DDExplorer.mobileToggle`); tabs scroll sideways.

## The boundary (agreed 2026-10-23)

| Economy (`finance.html`) | State (`state.html`) |
|---|---|
| Output & growth: size and shape · what drove growth · how sectors feed each other · factories (LSM, QIM, weights, censuses) | Public money: what the state collects · the federal budget · **what the state owes** (fiscal series and debt since 1950, moved from Economy) · oil and gas money to provinces |
| Trade: over time · what Pakistan trades · partners · growing and shrinking | Justice: case flows and pendency · judges and vacancies |
| Prices & money: inflation · interest rates · money and bad loans · the rupee | Crime & policing: reported offences · **Sindh police records** (crime tables + FIRs merged) |
| External: reserves and the balance of payments · **remittances and migration** | Power & gas: plants · distribution · load shedding · rooftop solar · gas |
| Land & resources: one topic (mining, crop prices, emissions as tabs) | Water & disasters: reservoirs · **floods and monsoon** (2022 floods + monsoon impacts merged) · disaster alerts |
| The long view: since 1950 (GDP, money, prices) · peers | |

Old links keep working: `finance.html#t=budget`, `#t=longrun&at=sec-fiscal`
and `#at=sec-debt` redirect to State; `state.html?t=sindhCrime`, `?t=firs`,
`?t=impacts` resolve to the merged topics (`REMAP` in state.js, which is a
no-op once `etl/economy/build_state_payload.py` — already updated — rebuilds
the payload).

## How Economy does it without reopening its modules

`economy-nav.js` replaces `economy-rail.js` and `sidebar-mobile.js`.
finance.js, money.js and trade.js are untouched: they still own the hash
(`t=` is *their* topic, `at=` a card), hide other topics' cards and draw on
`hashchange` and `resize`. The nav speaks to them only through the hash: a
tab click writes `t=<the card's module topic>&at=<card>`. The page's own
topics (`PAGE` in economy-nav.js) are a grouping over cards and need not match
the modules' — "Factories" is finance.js's `industry` and `censuses`,
"Remittances and migration" is half of money.js's `external`. The modules
rewrite the hash without `at=` once applied, so the card on screen is
remembered in the nav, not read back from the URL.

The budget, fiscal and debt cards stay in finance.html, always hidden
(`GONE`), because finance.js and money.js initialise against their ids.

One gotcha found on the way: the modules hide every `[data-topic]` outside
their topic, so nothing in the rail may carry a `data-topic` attribute
(`data-k` instead).

## The palette (palette.js)

Eight categorical slots in a fixed order, validated for lightness, chroma,
protan/deutan/tritan separation and 3:1 contrast on cream:
pine `#1e6b3e` · gold `#b8860b` · blue `#2f6fb0` · brick `#b5402a` · plum
`#7a4f9c` · teal `#0f9aab` · orange `#d96a26` · mauve `#8c4a7e`.
Only the first three are safe in every pairing (maps, scatter, small
multiples); from slot four on, assign in order and fold a ninth series into
"Other". Fixed meanings (`DDPalette.meaning`): agriculture `#9dbfa9`,
industry `#d4a017`, services `#0c3a1e`; inflows pine, outflows gold; State's
single series `#0f6e78`; reference `#9aa39c`; era shading `#e9ece6`.
Diverging: brick ↔ grey `#d9d8d0` ↔ green (`DDPalette.diverging`). The green
and gold sequential ramps are unchanged. State's hard-coded colours (fuel
families, hazards, judges, Sindh categories) were moved onto the slots.

## Not done

- Economy's topic deks are written in economy-nav.js; the modules' own
  `TOPICS[].desc` are no longer shown.
- The `.foot` source notes under the Economy cards are hidden in the
  one-chart view (each card has its provenance panel); the text is still in
  finance.html.
- The three Economy modules still draw every card of a topic on each hash
  change, including the ones that are off-screen; harmless, but a module
  that drew only the `at=` card would be faster.
