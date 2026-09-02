/* ==========================================================================
   Data Darbar — shared About / Methodology / Contact pop-outs
   Self-contained: injects its own CSS + modal markup and wires all behaviour.
   Works on every page (index, map, trade, finance) and under file:// —
   no fetch(), no dependency on styles.css or app.js.
   Open a modal with any element carrying  data-modal="about|methodology|contact".
   ========================================================================== */
(function () {
  if (window.__ddModalsInit) return;
  window.__ddModalsInit = true;
  window.__ddModalOpen = false;

  /* ---- styles (literal colours so it renders regardless of page CSS) ---- */
  var CSS = `
  .dd-modal-overlay{position:fixed;inset:0;background:rgba(12,58,30,.55);
    backdrop-filter:blur(3px);display:flex;align-items:center;justify-content:center;
    z-index:2000;padding:24px;animation:ddFade .15s ease}
  .dd-modal-overlay.dd-hidden{display:none}
  .dd-modal{background:#fff;border-radius:16px;box-shadow:0 24px 60px rgba(12,58,30,.32);
    max-width:640px;width:100%;max-height:84vh;display:flex;flex-direction:column;
    overflow:hidden;animation:ddSlide .2s ease}
  .dd-modal-header{display:flex;align-items:center;justify-content:space-between;
    padding:18px 24px 14px;border-bottom:1px solid #e2e5ea}
  .dd-modal-header h2{font-size:19px;font-weight:800;color:#0c3a1e;letter-spacing:-.01em;margin:0}
  .dd-modal-close{background:none;border:none;font-size:24px;color:#9aa2ad;cursor:pointer;
    padding:2px 10px;border-radius:8px;transition:.15s;line-height:1}
  .dd-modal-close:hover{color:#3d424d;background:#f1f2f5}
  .dd-modal-body{padding:20px 24px 26px;overflow-y:auto;font-size:14px;line-height:1.7;color:#3d424d}
  .dd-modal-body h3{font-size:12.5px;font-weight:700;color:#145228;text-transform:uppercase;
    letter-spacing:.06em;margin:20px 0 8px}
  .dd-modal-body h3:first-child{margin-top:0}
  .dd-modal-body h4{font-size:13.5px;font-weight:700;color:#17301f;margin:14px 0 4px}
  .dd-modal-body p{margin:0 0 10px}
  .dd-modal-body ul{margin:0 0 12px 20px;padding:0}
  .dd-modal-body li{margin-bottom:5px}
  .dd-modal-body a{color:#1e6b3e;font-weight:600}
  .dd-modal-body a:hover{color:#145228}
  .dd-modal-body code{background:#f1f2f5;padding:1px 5px;border-radius:4px;font-size:12.5px;
    font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
  .dd-modal-body strong{color:#17301f}
  .dd-tag{display:inline-block;font-size:10.5px;font-weight:700;letter-spacing:.05em;
    text-transform:uppercase;padding:2px 8px;border-radius:20px;margin-bottom:2px}
  .dd-about-links{display:flex;gap:10px;margin-top:14px;flex-wrap:wrap}
  .dd-about-link{display:inline-flex;align-items:center;gap:6px;padding:8px 16px;border-radius:8px;
    background:#eef4ee;color:#145228;font-size:13px;font-weight:600;border:1px solid #d9e6db;
    text-decoration:none;transition:.15s;cursor:pointer}
  .dd-about-link:hover{background:#e2efe4;border-color:#1e6b3e}
  .dd-form{display:flex;flex-direction:column;gap:13px;margin-top:12px}
  .dd-fg{display:flex;flex-direction:column;gap:4px}
  .dd-fg label{font-size:11px;font-weight:700;color:#145228;text-transform:uppercase;letter-spacing:.06em}
  .dd-fg input,.dd-fg textarea{padding:10px 12px;border:1.5px solid #e2e5ea;border-radius:8px;
    background:#faf9f5;font-size:14px;color:#17301f;font-family:inherit;transition:.15s;resize:vertical}
  .dd-fg input:focus,.dd-fg textarea:focus{outline:none;border-color:#1e6b3e;
    box-shadow:0 0 0 3px rgba(34,128,74,.12);background:#fff}
  .dd-send{padding:10px 20px;border-radius:8px;border:none;background:#145228;color:#fff;
    font-size:14px;font-weight:600;cursor:pointer;font-family:inherit;display:inline-flex;
    align-items:center;gap:8px;transition:.15s;align-self:flex-start}
  .dd-send:hover{background:#0c3a1e}
  .dd-send:disabled{opacity:.5;cursor:default}
  .dd-status{font-size:13px;font-weight:600;margin-top:2px}
  .dd-mz-lede{font-size:14.5px;line-height:1.62;color:#505662;margin:0 0 16px;
    padding-bottom:14px;border-bottom:1px solid #e2e5ea}
  .dd-ladder{margin:14px 0 6px;padding:14px 16px;background:#f8f9fb;border:1px solid #e2e5ea;
    border-radius:8px}
  .dd-ladder-row{display:grid;grid-template-columns:62px 1fr;grid-template-areas:"n l" "n b";
    column-gap:12px;padding:4px 0;align-items:center}
  .dd-ladder-n{grid-area:n;text-align:right;font-variant-numeric:tabular-nums;font-weight:700;
    font-size:13px;color:#6b7280}
  .dd-ladder-l{grid-area:l;font-size:12px;color:#505662}
  .dd-ladder-l em{font-style:normal;color:#9ca3af}
  .dd-ladder-bar{grid-area:b;height:5px;border-radius:3px;background:#c5cad3;margin-top:3px}
  .dd-ladder-row.is-here .dd-ladder-n,.dd-ladder-row.is-here .dd-ladder-l{color:#b8941a}
  .dd-ladder-row.is-here .dd-ladder-bar{background:#e8b92e}
  .dd-ladder-row.is-unit .dd-ladder-n,.dd-ladder-row.is-unit .dd-ladder-l{color:#145228;font-weight:700}
  .dd-ladder-row.is-unit .dd-ladder-bar{background:#1e6b3e;height:7px}
  .dd-mz-cards{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:12px 0 6px}
  .dd-mz-card{background:#f0f9f4;border:1px solid #e6f4ec;border-radius:8px;padding:12px 14px}
  .dd-mz-card h5{margin:0 0 5px;font-size:11px;font-weight:700;color:#1a5632;
    text-transform:uppercase;letter-spacing:.06em}
  .dd-mz-card p{margin:0;font-size:12.5px;line-height:1.6}
  @media(max-width:560px){.dd-mz-cards{grid-template-columns:1fr}}
  .dd-status.ok{color:#1e6b3e}.dd-status.err{color:#c0392b}
  @keyframes ddFade{from{opacity:0}to{opacity:1}}
  @keyframes ddSlide{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:none}}
  @media(max-width:640px){.dd-modal{max-height:92vh;border-radius:12px}
    .dd-modal-body{padding:16px 18px 20px}.dd-modal-header{padding:14px 18px 10px}
    .dd-modal-header h2{font-size:16px}}
  `;

  /* ---- content ---- */
  var ABOUT = `
    <h3>The project</h3>
    <p>Data Darbar is an open explorer of Pakistan's official statistics. It brings together the
      population census, household and labour-force surveys, 8-digit external-trade data, the national
      accounts, the federal budget and the central bank's monetary and external-sector series — sources published by the
      <a href="https://www.pbs.gov.pk/" target="_blank" rel="noopener">Pakistan Bureau of Statistics (PBS)</a>,
      the <a href="https://www.finance.gov.pk/" target="_blank" rel="noopener">Finance Division</a> and the
      <a href="https://easydata.sbp.org.pk/" target="_blank" rel="noopener">State Bank of Pakistan</a> —
      into a single set of interactive views. The name is a nod to the shrine in Lahore, reimagined here
      as a place of gathering for Pakistan's data.</p>
    <p>Every figure is traceable to its published source, and the cleaned data behind the site is held in
      a queryable DuckDB + Parquet warehouse rather than locked inside PDFs.</p>

    <h3>The five views</h3>
    <ul>
      <li><strong>District &amp; Tehsil Map</strong> — census, PSLM, labour-force, household and DHS health indicators
        across all 141 districts, with a 2017 vs 2023 comparison, plus the Mouza Census 2020 at tehsil level.</li>
      <li><strong>Trade Atlas</strong> — every 8-digit HS commodity as an Atlas-style treemap grouped by
        sector, with drill-down, top partners and change over time (2015–2024).</li>
      <li><strong>GDP &amp; Budget</strong> — GDP by sector and real growth, the 2015-16 input-output
        flows, and the federal budget's receipts and expenditure as a detailed line-item treemap.</li>
      <li><strong>Monetary &amp; External</strong> — <a href="https://easydata.sbp.org.pk/" target="_blank" rel="noopener">State Bank of Pakistan</a> series: the rupee since 1947, the policy
        rate since 1956, inflation, the interbank curve, reserves, the current account broken down to individual
        commodities, remittances by source, the money supply and bad loans.</li>
      <li><strong>Poverty & Wealth</strong> — a multidimensional poverty index built from household
        microdata, alongside satellite measures of relative wealth, population and night-time lights,
        mapped down to all 554 tehsils, alongside the Mouza Census inventory of what rural villages actually have.</li>
    </ul>
    <p>A sixth page, <strong>Query the Data</strong>, opens the whole warehouse — every table behind the five
      views — to SQL in the browser, with nothing sent to a server.</p>

    <h3>Built by</h3>
    <p><strong>Hiba Sameen</strong> is an economist and data scientist based in London. She holds a PhD in
      Economics and has worked across academia, government and think tanks on economic policy, with an
      interest in data science and engineering focused on making public data more accessible. Data Darbar
      grew out of the frustration of comparing Pakistani statistics that are scattered across PDF tables
      and separate reports.</p>
    <p class="dd-about-links">
      <a href="https://github.com/hibasameen" target="_blank" rel="noopener" class="dd-about-link">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12 0C5.37 0 0 5.37 0 12c0 5.3 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61-.546-1.385-1.335-1.755-1.335-1.755-1.087-.744.084-.729.084-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.694.825.576C20.565 21.795 24 17.295 24 12c0-6.63-5.37-12-12-12"/></svg>
        GitHub</a>
      <a href="https://www.linkedin.com/in/hiba-sameen-86750819/" target="_blank" rel="noopener" class="dd-about-link">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433a2.062 2.062 0 01-2.063-2.065 2.064 2.064 0 112.063 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/></svg>
        LinkedIn</a>
      <a class="dd-about-link" data-modal="contact">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 4h16v16H4z" fill="none"/><polyline points="22,6 12,13 2,6"/></svg>
        Contact</a>
    </p>

    <h3>Sources</h3>
    <ul>
      <li><strong>Population &amp; Housing Census 2017 and 2023</strong> — district demographic, education
        and employment tables.</li>
      <li><strong>PSLM, LFS &amp; HIES</strong> — social/living-standards, labour-force and household
        income &amp; expenditure surveys. PSLM 2019-20 is a district-level round and carries urban and
        rural households alike; HIES 2024-25 is a provincial round whose urban stratum is the division,
        so its district figures are rural-only (see Methodology).</li>
      <li><strong>PSLM district FIES</strong> — district-level food insecurity (moderate or severe) as
        published by PBS on the <a href="https://pslm-sdgs.data.gov.pk/districtlevel" target="_blank"
        rel="noopener">PSLM district dashboard</a>, computed on FAO's FIES methodology.</li>
      <li><strong>PDHS 2017-18</strong> — Pakistan Demographic and Health Survey microdata, published by the
        <a href="https://www.nips.org.pk/" target="_blank" rel="noopener">National Institute of Population
        Studies (NIPS)</a>: family planning, fertility, maternal &amp; child health, and child nutrition, at
        district level. Also fills the AJK &amp; Gilgit-Baltistan districts the census tables omit.</li>
      <li><strong>External Trade Statistics</strong> — 8-digit HS imports and exports by commodity and
        partner country, 2015–2024.</li>
      <li><strong>National Accounts (2015-16 base)</strong> — GDP by sector, real growth and the
        supply-use / input-output table.</li>
      <li><strong>Federal Budget "Budget in Brief"</strong> — receipts and expenditure line items,
        2009–2027.</li>
      <li><strong>Industry Statistics (QIM &amp; CMI)</strong> — Quantum Index of large-scale Manufacturing
        (monthly and annual, 2005-06 and 2015-16 bases) and the Census of Manufacturing Industries.</li>
      <li><strong>State Bank of Pakistan, <a href="https://easydata.sbp.org.pk/" target="_blank" rel="noopener">EasyData</a></strong> — 1,336 monthly, quarterly, daily and annual series
        from 33 datasets, pulled through SBP's REST API: exchange rates (from Aug-1947), the policy rate (from
        Jan-1956), the CPI/SPI/WPI (2015-16 base), KIBOR, lending and deposit rates, reserves (from Jun-1948),
        the BPM6 balance of payments, export receipts and import payments by commodity, services trade,
        country-wise workers' remittances (from Jul-1972), repatriation of profits by sector, monetary
        aggregates and non-performing loans.</li>
      <li><strong>Poverty &amp; satellite</strong> — Alkire–Foster Multidimensional Poverty Index from PSLM
        microdata, plus Meta's Relative Wealth Index, WorldPop population, VIIRS night-lights and the PBS Mouza Census 2020 facility inventory at tehsil level.</li>
    </ul>
    <p>All sources are public publications of the Government of Pakistan and the <a href="https://easydata.sbp.org.pk/" target="_blank" rel="noopener">State Bank of Pakistan</a>, except the
       Relative Wealth Index (Meta / Data for Good) and VIIRS night-lights (NASA/NOAA).</p>
  `;

  var CONTACT = `
    <p>Have a question, a suggestion, or spotted a data issue? Send a note and it reaches Hiba directly.</p>
    <form id="ddContactForm" class="dd-form">
      <div class="dd-fg"><label for="ddcName">Your name</label>
        <input type="text" id="ddcName" required placeholder="Name"/></div>
      <div class="dd-fg"><label for="ddcEmail">Your email</label>
        <input type="email" id="ddcEmail" required placeholder="you@example.com"/></div>
      <div class="dd-fg"><label for="ddcSubject">Subject</label>
        <input type="text" id="ddcSubject" placeholder="Optional"/></div>
      <div class="dd-fg"><label for="ddcMessage">Message</label>
        <textarea id="ddcMessage" rows="5" required placeholder="Your message…"></textarea></div>
      <button type="submit" class="dd-send" id="ddcSubmit">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
        Send message</button>
      <p id="ddcStatus" class="dd-status"></p>
    </form>
  `;

  var METH = `
    <p>Data Darbar is built from a reproducible pipeline: raw PBS and Finance Division publications
      (PDF tables parsed programmatically, CSV releases and survey microdata) and State Bank series fetched
      from its API are cleaned and normalised
      into a <strong>DuckDB + Parquet</strong> warehouse, then exported as the compact data files the site
      loads. The notes below cover each view in turn.</p>

    <h3><span class="dd-tag" style="background:#e4f0e8;color:#1e6b3e">District &amp; Tehsil Map</span></h3>

    <h4>District-name matching &amp; crosswalk</h4>
    <p>A core challenge in Pakistani administrative data is inconsistent district naming — the same place
      appears as "D.G. Khan", "Dera Ghazi Khan" or "DG Khan" across publications. Names are lowercased,
      stripped of punctuation and mapped through a manually curated crosswalk of name variants to a single
      canonical name that matches the GeoJSON boundary file (e.g. "Abbotabad" → "Abbottabad",
      "Naushero Feroze" → "Naushahro Firoz").</p>

    <h4>Multi-district aggregation</h4>
    <p>Several districts are reported at sub-district level but appear as one polygon in the boundaries.
      These are aggregated automatically: <strong>Kohistan</strong> (Upper + Lower) and
      <strong>Chitral</strong> (Upper + Lower). Count indicators are summed; rate indicators (literacy,
      unemployment) are recomputed from aggregated numerator and denominator counts rather than averaging
      percentages, which would be incorrect.</p>
    <h4>Karachi, and districts younger than the surveys</h4>
    <p><strong>Karachi</strong> was previously aggregated into one polygon but is now shown as its seven
      separate districts — Karachi Central, East, South, West, Korangi, Malir and Keamari. That creates a
      mismatch with sources whose geography predates the split, and two rules handle it. Both publish the
      geography that was actually measured and label it, rather than discarding real data or implying a
      district estimate the source never made.</p>
    <ul>
      <li><strong>Karachi city-wide.</strong> The PDHS 2017-18 sampling frame predates the split, so the
        survey carries a single Karachi category (1,016 women in the family-planning module). This is
        carried across all seven districts flagged <code>karachi_citywide</code>. Without it, roughly 20
        million people would have no health indicators at all — and until this build they did, because the
        single "Karachi" row silently failed to match any of the seven keys.</li>
      <li><strong>Keamari and Karachi West, 2017 vs 2023.</strong> Keamari did not exist at the 2017
        census, and Karachi West's 2017 population of 3.91 million covers the area that is now both
        districts. Differencing that against Karachi West's 2023 figure alone produced an apparent loss of
        1.23 million people and 559&nbsp;km² — which was the largest population decline anywhere on the
        site, and was purely the boundary moving. Census 2023 does measure the two separately and those
        figures stand; only the 2017 row and the change are treated as belonging to the combined area, so
        both districts now show <strong>+21.5%</strong> and carry a boundary-change note. The 2017 figure
        is one number displayed on both, so the two 2017 columns must not be added together.</li>
      <li><strong>Keamari inherits from Karachi West.</strong> Keamari was carved out of Karachi West in
        2020 and appears in Census 2023 with 2.07 million people, but in none of the survey frames — PSLM
        2019-20, both Labour Force Surveys and HIES 2024-25 all predate it or still count it as part of
        Karachi West. It is the only district on the site with a census population and no survey data
        whatsoever. Survey families it has nothing of its own for are filled from Karachi West and flagged
        <code>&lt;source&gt;_inherited_from</code>. Its census figures are untouched, because the census
        does count it separately.</li>
    </ul>
    <p>In both cases sample sizes travel unchanged and describe the parent geography — the city, or Karachi
      West — which is what the flags are there to signal. The pipeline also checks every incoming row
      against the boundary file and reports any that fail to match, so this class of silent loss surfaces
      at build time rather than as an unexplained gap on the map.</p>

    <h4>Choropleth &amp; change</h4>
    <p>Values are mapped to fill colours using quantile breaks (5 classes) via
      <a href="https://gka.github.io/chroma.js/" target="_blank" rel="noopener">Chroma.js</a>, so each class
      holds roughly the same number of districts — useful for skewed distributions. The "Change" view uses
      a diverging scale centred at zero (red = decline, green = growth) and computes a simple difference
      <code>2023 − 2017</code>, expressed in percentage points for rate indicators.</p>

    <h4>Survey adjustments (LFS &amp; HIES)</h4>
    <p>The Labour Force Survey and HIES are designed to be representative at the <em>provincial</em>, not
      district, level. Two adjustments are applied: a minimum sample-size filter (districts with
      <em>n</em> &lt; 30 observations are suppressed and flagged), and post-stratification of survey weights
      to Census 2023 population totals (a sex-ratio reweighting for LFS microdata; a population calibration
      factor for HIES households). These improve plausibility but do not remove the limits of provincial
      surveys at fine geographies — district survey estimates should be read as approximate.</p>

    <h4>Why HIES 2024-25 figures are labelled "rural only"</h4>
    <p>This is the single most important caveat on the household panels, so it is worth stating plainly.
      HIES 2024-25 <strong>does not record which district an urban household lives in</strong>. In PBS's
      own words, "for urban domain, each administrative <em>division</em> for all four provinces has been
      considered as an independent stratum", while the rural domain uses "each administrative
      <em>district</em> in Punjab, Sindh and Khyber Pakhtunkhwa and each administrative <em>division</em>
      in Balochistan". We verified this directly in the microdata: all 941 urban sampling units carry a
      district code of zero, against 1,212 rural units that carry a real one. There are 127 rural district
      strata and 31 urban <em>division</em> strata.</p>
    <p>The consequence is that <strong>every HIES district figure on this site covers rural households
      only</strong> — 19,163 of the 30,123 sampled households. The 10,960 urban households, 39% of the
      weighted population, cannot be assigned to a district by any method, and no crosswalk can recover
      what was never recorded. Districts with no rural sample at all — Lahore, most of Karachi, urban
      Islamabad — therefore carry no HIES figures rather than a guessed one.</p>
    <p>This matters most where it is least obvious. Because urban households are better off almost
      everywhere, a rural-only figure understates district welfare systematically, and by more in more
      urban districts. Reconstructing whole-district values using Census 2023 urban shares suggests
      rural-only per-capita consumption is understated by around 8% in the median district but by
      roughly 100% in Hyderabad and Karachi West. On a 89-district consumption ranking, the typical
      district moves about 9 places once urban households are restored, and Hyderabad moves 60.
      <strong>HIES district figures are a sound description of rural conditions and should not be read as
      district totals.</strong></p>

    <h4>Food insecurity: two different series</h4>
    <p>Two food-insecurity measures appear on the site and they are not interchangeable.</p>
    <ul>
      <li><strong>Food Security — PSLM 2019-20 (whole district)</strong> is the series to quote for a
        district. PSLM is a <em>district-level</em> survey round: it codes urban households by district, so
        the figure covers urban and rural together. These are PBS's own published estimates, which follow
        FAO's FIES methodology (a Rasch item-response model), covering 111 districts including all of
        Karachi, which HIES cannot reach at all.</li>
      <li><strong>Consumption &amp; Food Security — HIES 2024-25 (rural only)</strong> is five years more
        recent but rural-only, and is computed here as a raw FIES score of 4 or more out of 8 rather than
        by the Rasch model. Recomputing the PSLM series the same way reproduces PBS's district ranking
        closely (Spearman 0.91 across 112 districts) but sits about a third lower in level. So the two
        numbers should be compared for <em>pattern</em>, never for <em>level</em>.</li>
    </ul>
    <p>The same principle orders the other panels: where a whole-district PSLM series and a rural-only HIES
      series measure the same thing — piped water, sanitation, mobile and internet access — PSLM is listed
      first and HIES is offered as the more recent rural supplement. Piped water is a good illustration of
      why: in Hyderabad the whole-district PSLM figure is 68%, while the rural-only HIES figure is 23%.</p>

    <h4>Health &amp; demographic indicators (PDHS 2017-18)</h4>
    <p>The five health layers — Family Planning, Fertility &amp; Child Survival, Maternal Health, Child
      Immunisation and Child Nutrition — are computed from the <strong>Pakistan Demographic and Health Survey
      (PDHS) 2017-18</strong> microdata, published by the <a href="https://www.nips.org.pk/" target="_blank"
      rel="noopener">National Institute of Population Studies (NIPS)</a>, using the survey's sampling weights.
      District estimates are validated against the published national PDHS figures — e.g. contraceptive
      prevalence 33.7% (published 34.2%), modern method 24.7% (25.0%), unmet need 18.4% (17.3%), stunting
      37.2% (37.6%), full immunisation 66.0% (66%).</p>
    <p><strong>Important caveat:</strong> the PDHS is designed to be representative at the national, provincial
      and regional level, <em>not</em> the district level (about 15,000 women across ~130 districts), so
      district figures are <em>indicative</em>. Each layer carries its own denominator and unweighted sample
      size, and the same n&lt;30 suppression rule applies — so reliable coverage varies sharply by indicator:
      family planning and fertility are estimable in ~122 of 133 sampled districts, maternal health in ~98,
      child nutrition in ~47, and child immunisation in only ~16 (the 12–23-month denominator is very small
      per district). Suppressed districts are greyed out with their sample size shown in the tooltip.
      Gilgit-Baltistan and Azad Jammu &amp; Kashmir — excluded from the national weight — use the survey's
      combined weight, so their within-district estimates are valid; the PDHS is what now fills the 17 AJK/GB
      districts the PBS census tables don't cover, giving at least some data for all 141 districts.</p>

    <h3><span class="dd-tag" style="background:#f6ecd0;color:#a67c0a">Trade Atlas</span></h3>

    <h4>Source &amp; parsing</h4>
    <p>Imports and exports come from PBS
      <a href="https://www.pbs.gov.pk/external-trade-statistics/" target="_blank" rel="noopener">External Trade
      Statistics</a> at the <strong>8-digit HS</strong> commodity level (roughly 1,050 distinct products per
      direction). The published PDFs are converted with <code>pdftotext -layout</code> and parsed with a
      stateful reader that pops trailing value columns, repairs HS codes whose leading zeros were dropped and
      became glued to the commodity name, and re-joins long product names that wrapped onto a second line and
      pushed their totals down. Each year is validated against the printed <code>GRAND TOTAL</code>:
      reconstructed exports reconcile to 100% and imports to 93–100% of the published total.</p>

    <h4>Sector grouping &amp; the treemap</h4>
    <p>Every 8-digit line is assigned to its <strong>HS section</strong> (the 21 top-level groupings —
      textiles, mineral fuels, vegetable products, machinery, and so on) and its 2-digit chapter. The Atlas
      treemap follows the logic of Harvard's Atlas of Economic Complexity: area is proportional to trade
      value and colour encodes the HS section, so the composition of what Pakistan buys and sells is legible
      at a glance. Clicking a sector zooms to its chapters and products; a breadcrumb tracks the drill-down.
      Values are the customs values as published (nominal Pakistan Rupees), not inflation- or exchange-rate
      adjusted, and represent recorded formal trade only.</p>
    <p><strong>Gap years.</strong> PBS did not publish 8-digit data for exports 2017-18 or imports 2018-19 and
      2019-20. Exports 2017-18 are filled from UN Comtrade (Pakistan's calendar-2018 exports, shown at
      HS-section level only, valued in Rs at the fiscal-year average rate); the two import gaps are filled at
      the total level only from the Pakistan Economic Survey (Pakistan stopped reporting to UN Comtrade after
      2018, so no commodity breakdown is available). These years are flagged in the source note.</p>

    <h3><span class="dd-tag" style="background:#f3dedb;color:#b23b2c">GDP &amp; Budget</span></h3>

    <h4>National accounts (GDP by sector &amp; growth)</h4>
    <p>GDP is taken from the national accounts on the <strong>2015-16 base year</strong>. The "economy by
      sector" view shows value added by the standard sector split (agriculture, industry, services and their
      sub-sectors); "real GDP growth" is the year-on-year change in constant-price GDP. Long historical
      series are shown where PBS publishes a consistent constant-price series; a rebasing (e.g. from the older
      2005-06 base) introduces a level break, so series are presented on the latest base rather than spliced.</p>

    <h4>Input-output flows (2015-16)</h4>
    <p>The chord view is built from the PBS <strong>2015-16 supply-use / input-output table</strong>. The
      published matrix (68 industries) is aggregated to 12 broad sectors, and inter-industry flows are drawn
      as directed ribbons from supplying to using sector. Table values are published in Rupees <em>million</em>
      and are converted to billions (÷1,000) for display. The snapshot is structural — it shows how much of
      each sector's output is used as intermediate input by every other sector in that year — and is not a
      time series.</p>

    <h4>Federal budget treemap</h4>
    <p><strong>Receipts</strong> are built from the Finance Division's <strong>Explanatory Memorandum on
      Federal Receipts</strong> — the most granular receipts source — for every year <strong>2009-10 to
      2026-27</strong>. The memorandum classifies each receipt by Pakistan's NAM chart-of-accounts codes
      (B = tax, C = non-tax), which give a clean hierarchy: Tax Revenue splits into Direct Taxes (Income Tax)
      and Indirect Taxes (Customs, Sales Tax, Federal Excise), and Non-Tax Revenue into Income from Property,
      Receipts from Civil Administration, Miscellaneous Receipts and Levies &amp; Fees. The own-year budget
      estimate is the last numeric column. Because table numbering and which leaves carry codes vary across
      the 18 years, each parent's captured leaves are reconciled to its printed control subtotal and any gap
      is placed in an explicit "Other …" child — so every node sums exactly to the published Tax Revenue,
      Non-Tax Revenue and total-revenue figures.</p>
    <p><strong>Expenditure</strong> is the current expenditure by function from the <strong>"Budget in
      Brief"</strong> (debt servicing, defence, running of civil government, subsidies &amp; grants, etc.),
      broken into detailed sub-items. Both sides are rendered as a treemap where area is proportional to the
      Rupee amount; a toggle switches between Expenditure and Receipts and a slider moves across budget years.
      All figures are nominal budget estimates as published; presentation of some line items changes across
      years, so cross-year comparison of a single narrow item should be read with the source in mind.</p>

    <h3><span class="dd-tag" style="background:#ece4f2;color:#7a5195">Monetary &amp; External</span></h3>

    <h4>Source &amp; extraction</h4>
    <p>Every series comes from the State Bank's <a href="https://easydata.sbp.org.pk/" target="_blank"
      rel="noopener">EasyData</a> portal through its REST API, one request per series with full history.
      The portal serves 226 datasets and about 23,000 series; the warehouse holds a curated 1,336 of them
      (the headline and "core" tiers of a published selection file), with the full catalogue indexed so
      any other series can be added. Fetching is checkpointed and rate-limited (SBP's binding cap is
      250 requests an hour), and two flavours of malformed JSON in the portal's responses — raw control
      characters and unescaped quotes inside strings — are repaired before parsing. The page itself ships
      only the ~50 series it draws, as a plain script file, so it loads instantly and works offline.</p>

    <h4>Fiscal years, partial years and scales</h4>
    <p>Monthly flows are summed to Pakistan's July–June fiscal year. The newest year is nearly always
      partial, so the year picker defaults to the last <em>complete</em> year and labels partial ones with
      their month count; a one-month "year" shown beside a twelve-month one reads as a collapse that never
      happened. The rupee chart uses a log scale by default: on a series that runs from 3 to 280 per dollar
      only a log axis shows equal percentage moves as equal distances. Daily KIBOR is thinned to month-end
      for the chart (the SQL console keeps every day).</p>

    <h4>The current-account treemap</h4>
    <p>"Dollars in" and "dollars out" are the credit and debit sides of the BPM6 current account — goods,
      services, primary income and secondary income — drawn to scale, so the deficit is the extra width
      of the right-hand box. The build refuses to publish unless credits minus debits equals SBP's
      reported current-account balance to the dollar, every year. Clicking a box opens its breakdown:
      services by type and transfers by kind come straight from the BPM6 tables; remittances by source use
      the country-wise table; goods use SBP's export receipts and import payments by commodity; income paid
      abroad splits into repatriated profits by sector and a residual (interest and other investment
      income) equal to the primary-income debit less the repatriation table.</p>
    <p>Two of those tables have no published hierarchy: the 147 commodity series and the 50 sector series
      are listed in document order with no level marker, yet many are sub-totals of their neighbours
      (Transport Group ⊃ Road Motor Vehicles ⊃ CKD ⊃ Motor Cars; Power ⊃ Thermal, Hydel, Coal). Summing
      everything overstates goods and repatriation totals by up to 28%. The build recovers the tree
      arithmetically — a series is a parent when the top-level items after it sum to it in every fiscal
      year — and checks the result against the published totals. The commodity tables are on a
      "through banks" basis; the balance-of-payments figure deducts freight and adds other flows, so the
      zoomed header shows both numbers rather than rescaling tiles to force them to match.</p>

    <h4>Remittances</h4>
    <p>SBP's country series are nested: U.A.E. already contains Dubai, Abu Dhabi and Sharjah; "Other GCC"
      contains Bahrain, Kuwait, Oman and Qatar; "ten European countries" contains Belgium through Sweden.
      Adding every series overstates the total by 42%. The site uses the fifteen-source partition that
      reconciles exactly to SBP's published total, and the build fails if it ever stops doing so.</p>

    <h3><span class="dd-tag" style="background:#dceef0;color:#0f6e78">Poverty &amp; Wealth</span></h3>
    <h4>Multidimensional Poverty Index (district)</h4>
    <p>An <strong>Alkire–Foster adjusted headcount</strong>, M<sub>0</sub> = H &times; A, computed directly
      from <strong>PSLM 2019-20 household microdata</strong> — from the joint distribution of deprivations
      across households, not from district averages. Two equally weighted dimensions: <em>education</em>
      (years of schooling; child school attendance, ¼ each) and <em>living standards</em> (electricity,
      cooking fuel, sanitation, drinking water, housing materials, 1/10 each). The poverty cutoff is
      k = 1/3, the global-MPI standard, and estimates are person-weighted using PSLM survey weights.
      The health dimension is deliberately omitted: MPI health indicators require DHS microdata that
      cannot be linked to PSLM households, so this is best read as a <em>living-conditions</em> MPI.
      It covers 119 of 141 districts — the rest were not sampled by PSLM. The national aggregate
      (M<sub>0</sub> = 0.222, headcount 39.2%) sits on top of the official Pakistan MPI headcount.</p>

    <h4>Satellite layers (tehsil)</h4>
    <p><strong>Relative Wealth Index</strong> is Meta's machine-learning estimate on a ~2.4 km grid,
      aggregated to tehsils as a population-weighted mean. <strong>Population</strong> is WorldPop 2020
      (UN-adjusted, 1 km), zonal-summed; it totals 220.7 million, matching the UN estimate.
      <strong>Night-time lights</strong> are VIIRS DNB June composites for 2020–2026. Two corrections
      matter and are applied: a <strong>1 nW background floor</strong> (over half the raw radiance total
      is diffuse haze from airglow and moonlit snow or sand), and a persistent-<strong>gas-flare</strong>
      mask. Tehsils below one person per km² are flagged and withheld, because over empty terrain the
      signal measures surface albedo rather than activity. Note that night-lights are themselves an
      <em>input</em> to the Relative Wealth Index, so those two layers are not independent of each other;
      the survey-based MPI is the one independent benchmark.</p>

    <h4>Rural facilities (tehsil)</h4>
    <p>PBS's <strong>Mouza Census 2020</strong>, a hundred-per-cent count of 48,738 revenue villages
      carried out with the provincial revenue departments. For each mouza an enumerator records what
      exists in it — a girls' primary school, a basic health unit, a metalled street, mains
      electricity — so every figure here is a <em>share of mouzas</em>, not of people. A mouza of
      12,000 counts the same as a mouza of 300, and the frame is rural by construction: cities are
      not revenue villages, so urban Karachi and the city tehsils sit outside it entirely.</p>
    <p>PBS does not publish the denominator, and its own indicator blocks disagree about how many
      mouzas answered — only 33 of 544 enumerated tehsils give a single consistent base. Each block
      is therefore divided by its own row sum, and where the blocks diverge by more than 5% the
      detail panel says so. PBS enumerates 595 tehsils against the boundary file's 553, mostly
      because it carries sub-tehsils created after the polygons were drawn, so those are pooled into
      the unit they were carved from; the crosswalk is published alongside the code. Azad Jammu and
      Kashmir and Gilgit-Baltistan were not enumerated in this round. Neither were Mand and Tump in
      Kech or Kallag in Panjgur, though every neighbouring sub-tehsil reports normally.</p>

    <h3>Limitations</h3>
    <ul>
      <li>Survey-based district indicators (PSLM, LFS, HIES) are sample estimates and carry sampling error,
        especially in smaller districts.</li>
      <li>The MPI omits the health dimension and reflects 2019-20 conditions; the Relative Wealth Index is
        modelled rather than measured, and is benchmarked against a pooled India–Pakistan baseline.</li>
      <li>Night-time lights proxy activity and electrification, not GDP; June-only monthly composites carry
        more year-to-year noise than annual products and a seasonal signature.</li>
      <li>Trade figures are nominal recorded customs values and exclude informal/unrecorded trade; leading-zero
        and name-wrap repairs in PDF parsing are validated in aggregate but individual rare lines may vary.</li>
      <li>National accounts and budget figures are nominal unless stated; base-year rebasing and changes in
        budget presentation create breaks that are not spliced away.</li>
      <li>The input-output snapshot is a single year (2015-16) and structural, not a trend.</li>
      <li><a href="https://easydata.sbp.org.pk/" target="_blank" rel="noopener">State Bank</a> series are taken as published: the BPM6 monthly balance of payments begins in July
        2013, the commodity breakdowns in July 2013 and repatriation by sector in July 2007, so the
        treemap's drill-downs cover a shorter span than the headline series. Goods breakdowns are
        payments-basis (through banks) and will not match PBS customs figures on the Trade Atlas.</li>
      <li>Census 2023 results used are provisional and may differ from final published figures.</li>
    </ul>
  `;


  /* ---- What is a mouza? (opened from the Rural Facilities layer) ---- */
  var MOUZA = `
    <p class="dd-mz-lede">Every figure on the Rural Facilities layer counts mouzas. Almost nothing
      else published about Pakistan uses this unit, so it is worth knowing what is being counted —
      and what a share of mouzas can and cannot tell you.</p>

    <h4>A unit of land, not a settlement</h4>
    <p>A mouza is a <strong>revenue village</strong>: a bounded piece of land in the provincial land
      records, with a name, a fixed boundary and an area measured plot by plot. It is the smallest
      permanent unit of Pakistani land administration, and most boundaries were drawn during
      colonial-era settlements and have barely moved since.</p>
    <p>Because it is a unit of <em>land</em>, a mouza is not the same thing as a village. One may
      hold several hamlets, a single large village, or nobody at all — 1,808 were recorded as
      unpopulated. Sizes vary enormously: a few hundred acres in canal-irrigated Punjab, hundreds of
      thousands in Balochistan.</p>

    <h4>Where it sits</h4>
    <p>The mouza is the bottom rung of the revenue hierarchy. Everything mapped here at tehsil level
      is, underneath, a bundle of these.</p>
    <div class="dd-ladder">
      <div class="dd-ladder-row"><span class="dd-ladder-n">5</span><span class="dd-ladder-l">Provinces</span><span class="dd-ladder-bar" style="width:2%"></span></div>
      <div class="dd-ladder-row"><span class="dd-ladder-n">36</span><span class="dd-ladder-l">Divisions</span><span class="dd-ladder-bar" style="width:4%"></span></div>
      <div class="dd-ladder-row"><span class="dd-ladder-n">151</span><span class="dd-ladder-l">Districts</span><span class="dd-ladder-bar" style="width:8%"></span></div>
      <div class="dd-ladder-row is-here"><span class="dd-ladder-n">595</span><span class="dd-ladder-l">Tehsils <em>— the finest level mapped</em></span><span class="dd-ladder-bar" style="width:16%"></span></div>
      <div class="dd-ladder-row"><span class="dd-ladder-n">1,715</span><span class="dd-ladder-l">Qanungo halqas</span><span class="dd-ladder-bar" style="width:26%"></span></div>
      <div class="dd-ladder-row"><span class="dd-ladder-n">12,734</span><span class="dd-ladder-l">Patwar circles</span><span class="dd-ladder-bar" style="width:52%"></span></div>
      <div class="dd-ladder-row is-unit"><span class="dd-ladder-n">48,738</span><span class="dd-ladder-l">Mouzas <em>— what every figure counts</em></span><span class="dd-ladder-bar" style="width:100%"></span></div>
    </div>

    <h4>What the census does</h4>
    <p>The <strong>Mouza Census</strong> is run by PBS with the provincial revenue departments as a
      hundred-per-cent count rather than a sample. An enumerator goes to each mouza and records what
      is there: a girls' primary school, a basic health unit, a metalled street, mains electricity,
      a bazar, a police post. The 2020 round is the most recent; 2008 was the one before it. It
      supplies the sampling frame for PBS's rural household surveys, and it is the only source that
      describes rural facilities for the whole country at once.</p>

    <h4>How to read a percentage</h4>
    <div class="dd-mz-cards">
      <div class="dd-mz-card"><h5>Places, not people</h5>
        <p>64.8% of mouzas have a girls' primary school — 30,554 of 47,120. That is <em>not</em>
          64.8% of rural girls. A mouza of 12,000 and a mouza of 300 each count once.</p></div>
      <div class="dd-mz-card"><h5>Presence, not access</h5>
        <p>The question is whether a facility sits <em>inside the boundary</em>. One 500 metres over
          the line counts as absent; one at the far end of a 40 km² mouza counts as present.</p></div>
    </div>
    <p>Plain percentages come from mutually exclusive questions. Drinking water, health facility
      type, fuel and street surface are <strong>multiple response</strong> — a mouza can report
      three water sources — so those are divided by the reporting base and can exceed 100%.</p>

    <h4>Where it stops</h4>
    <p><strong>The denominator is not published.</strong> PBS reports how many mouzas have a facility
      and how many do not, but never how many were asked. Its own blocks disagree: only 33 of the 544
      enumerated tehsils give a single consistent base, and they can differ by up to 181 mouzas.</p>
    <p><strong>The rural edge is soft.</strong> A city is not a revenue village, so built-up cores are
      absent — 22 Karachi towns and several city tehsils show as no-data. But mouzas that urbanise
      stay in: 4.9% are classified urban and 2.4% partly urban, above 20% in 49 tehsils and
      effectively everything in Model Town, Shalimar, Raiwind and Sukkur City. The Settlement group
      in the indicator list shows each tehsil's exposure.</p>
    <p><strong>Three gaps.</strong> Azad Jammu and Kashmir and Gilgit-Baltistan appear in the frame
      but were not enumerated. Neither were Mand and Tump in Kech, or Kallag in Panjgur. And it is a
      single snapshot — nothing here supports a claim about change over time.</p>

    <p style="font-size:12px;color:#6b7280;margin-top:16px">Source: Pakistan Bureau of Statistics,
      <a href="https://www.pbs.gov.pk/mouza-census/" target="_blank" rel="noopener">Mouza Census 2020</a>.</p>
  `;

  /* ---- build ---- */
  function modal(id, title, body) {
    return '<div id="' + id + '" class="dd-modal-overlay dd-hidden" role="dialog" aria-modal="true" aria-label="' + title + '">' +
      '<div class="dd-modal"><div class="dd-modal-header"><h2>' + title + '</h2>' +
      '<button class="dd-modal-close" data-dd-close aria-label="Close">&times;</button></div>' +
      '<div class="dd-modal-body">' + body + '</div></div></div>';
  }

  function boot() {
    if (document.getElementById('ddAboutModal')) return;
    var style = document.createElement('style');
    style.textContent = CSS;
    document.head.appendChild(style);

    var host = document.createElement('div');
    host.id = 'ddModalHost';
    host.innerHTML =
      modal('ddAboutModal', 'About Data Darbar', ABOUT) +
      modal('ddMethodologyModal', 'Methodology', METH) +
      modal('ddContactModal', 'Contact', CONTACT) +
      modal('ddMouzaModal', 'What is a mouza?', MOUZA);
    document.body.appendChild(host);

    var MAP = { about: 'ddAboutModal', methodology: 'ddMethodologyModal', contact: 'ddContactModal',
                mouza: 'ddMouzaModal' };

    function open(id) {
      var m = document.getElementById(id);
      if (!m) return;
      m.classList.remove('dd-hidden');
      window.__ddModalOpen = true;
      var mn = document.getElementById('mobileNav');
      if (mn) mn.classList.add('hidden');
    }
    function closeAll() {
      document.querySelectorAll('.dd-modal-overlay').forEach(function (m) { m.classList.add('dd-hidden'); });
      window.__ddModalOpen = false;
    }

    // open triggers (works for header links + mobile-nav links + in-modal links)
    document.addEventListener('click', function (e) {
      var t = e.target.closest('[data-modal]');
      if (t) {
        var key = t.getAttribute('data-modal');
        if (MAP[key]) { e.preventDefault(); closeAll(); open(MAP[key]); }
        return;
      }
      if (e.target.closest('[data-dd-close]')) { closeAll(); return; }
      // click on the dark overlay itself (not the dialog) closes
      if (e.target.classList && e.target.classList.contains('dd-modal-overlay')) closeAll();
    });

    // Escape closes the top modal (registered before app.js's zoom handler runs it)
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && window.__ddModalOpen) { closeAll(); }
    });

    // Contact form → FormSubmit.co (email assembled at runtime to avoid scraping)
    var form = document.getElementById('ddContactForm');
    if (form) {
      form.addEventListener('submit', function (e) {
        e.preventDefault();
        var btn = document.getElementById('ddcSubmit');
        var status = document.getElementById('ddcStatus');
        btn.disabled = true; btn.textContent = 'Sending…';
        status.textContent = ''; status.className = 'dd-status';
        var r = ['hiba', 'sameen', '@', 'gmail', '.com'].join('');
        fetch('https://formsubmit.co/ajax/' + r, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
          body: JSON.stringify({
            name: document.getElementById('ddcName').value.trim(),
            email: document.getElementById('ddcEmail').value.trim(),
            _subject: document.getElementById('ddcSubject').value.trim() || 'Data Darbar Contact',
            message: document.getElementById('ddcMessage').value.trim(),
            _template: 'table'
          })
        }).then(function (res) { return res.json(); })
          .then(function (d) {
            if (d.success === 'true' || d.success === true) {
              status.textContent = 'Message sent successfully.'; status.className = 'dd-status ok';
              form.reset();
            } else { status.textContent = 'Something went wrong. Please try again.'; status.className = 'dd-status err'; }
          })
          .catch(function () { status.textContent = 'Network error. Please try again later.'; status.className = 'dd-status err'; })
          .finally(function () {
            btn.disabled = false;
            btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg> Send message';
          });
      });
    }
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
