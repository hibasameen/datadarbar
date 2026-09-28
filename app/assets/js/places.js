/*
   places.js — one picker over every indicator Data Darbar can draw.
   -----------------------------------------------------------------
   The index ships with the page: 5,801 indicators, 0.65 MB, interned so that
   searching all of them needs no query engine. Values arrive by a route chosen
   on size, which is the decision Phase 0 measured rather than assumed:

     curated   one file per indicator group under data/places/, about 34 KB,
               fetched the first time something in that group is chosen.
     census    17 MB across two Parquet panels, read by range request through
               DuckDB-WASM when a census series is picked. The engine is 6.3 MB,
               so it loads then and not before.
     crops     78,425 rows, same treatment as census.

   Facets follow the design: locality and sex are chosen after an indicator, not
   searched through. The year is offered only where the indicator has more than
   one, which for census series is rare - 34 of 4,039 district definitions exist
   in both censuses, because the two censuses did not publish the same tables.
*/
(function () {
  'use strict';

  var IX = window.DD_PLACES_IX;
  if (!IX) { return; }

  var SEP = '\u001f';
  var N = IX.level.i.length;

  /* The index arrives columnar and interned. These read one row back out. */
  function col(name, row) {
    var c = IX[name];
    return c.v ? c.v[c.i[row]] : c[row];
  }
  function list(name, row) {
    var s = col(name, row);
    return s ? s.split(SEP) : [];
  }

  var state = {
    level: 'district',
    row: null,          // index row of the chosen indicator
    year: null,
    locality: 'all',
    sex: 'all',
    norm: '',           // '', 'pct' or 'per1000' - how a count is read
    values: null,       // map_key -> number
    meta: null,         // map_key -> {relation, note}
    place: null,        // selected map key
    query: '',
  };

  /* ── geography ──────────────────────────────────────────────────────────
     Both layers are PBS's Digital Census 2023. AJK and Gilgit-Baltistan are
     always drawn and grey where nothing reaches them; Indian-occupied Kashmir
     is never drawn. */
  var GEO = {
    district: { file: 'data/districts_2023_geo.js', global: 'DD_GEO_D23',
                key: function (p) { return p.code; },
                name: function (p) { return p.n; },
                prov: function (p) { return p.p; }, noun: 'district' },
    tehsil:   { file: 'data/tehsils_2023_geo.js', global: 'DD_GEO_T23',
                key: function (p) { return p.dds_id; },
                name: function (p) { return p.n; },
                prov: function (p) { return p.p; }, noun: 'tehsil' },
  };

  var map, layer, loaded = {}, geoCache = {}, groupCache = {}, wh = null, whBooting = null;

  function $(id) { return document.getElementById(id); }
  function esc(t) {
    return String(t == null ? '' : t).replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }
  function fmt(v, dp) {
    if (v == null || isNaN(v)) return '—';
    return Number(v).toLocaleString(undefined,
      dp > 0 ? { minimumFractionDigits: dp, maximumFractionDigits: dp }
             : { maximumFractionDigits: 0 });
  }

  function script(src) {
    if (loaded[src]) return loaded[src];
    loaded[src] = new Promise(function (res, rej) {
      var el = document.createElement('script');
      el.src = src; el.async = false;
      el.onload = res;
      el.onerror = function () { rej(new Error('could not load ' + src)); };
      document.head.appendChild(el);
    });
    return loaded[src];
  }

  /* ── the picker ─────────────────────────────────────────────────────────
     Topics come from the index in the order the ETL wrote them, which is the
     order the design lists. An indicator is shown only where it exists at the
     chosen level: the district and tehsil catalogues genuinely differ. */
  function rowsForLevel() {
    var out = [];
    for (var i = 0; i < N; i++) if (col('level', i) === state.level) out.push(i);
    return out;
  }

  function latestYear(yrs) {
    var plain = yrs.filter(function (y) { return /^\d{4}(-\d{2})?$/.test(y); });
    return (plain.length ? plain : yrs)[(plain.length ? plain : yrs).length - 1];
  }

  /* The hierarchy's words are searchable alongside the source's own, so
     "attainment", "disability" or "tenure" find the series filed under them
     even though no published label uses those words. The source label stays
     searchable exactly as it is: an alias is a way in, not a rename. */
  function matches(i, q) {
    return (col('label', i) + ' ' + col('group_label', i) + ' ' +
            col('topic_label', i) + ' ' + col('h_topic', i) + ' ' +
            col('h_sub', i) + ' ' + col('h_family', i))
      .toLowerCase().indexOf(q) >= 0;
  }

  /* Relevance, because the census outnumbers everything else forty to one and
     an unranked list of 393 literacy matches opens on "10 -- 14 · FORMAL"
     rather than on a literacy rate. Higher is better. */
  function score(i, q) {
    var label = col('label', i).toLowerCase();
    var at = label.indexOf(q);
    var s = 0;
    if (at === 0) s += 100;                       // the label starts with it
    else if (at > 0) {
      s += 50;
      if (/[^a-z0-9]/.test(label.charAt(at - 1))) s += 20;   // starts a word
    } else if (col('group_label', i).toLowerCase().indexOf(q) >= 0) s += 15;
    // a curated indicator is one of 296 chosen by hand; a census cell is one of
    // 4,051 enumerated. Where both match, the chosen one is the better guess.
    if (col('source', i) === 'place') s += 25;
    // shorter labels are more likely to be the thing itself rather than a cell
    // inside it
    s -= Math.min(20, label.length / 6);
    // an indicator that colours more shapes is more useful than a thin one
    s += Math.min(10, col('shapes', i) / 65);
    return s;
  }

  /* The list is now search results only. With no query there is nothing to
     show, because the Topic dropdown is how you browse. */
  function renderPicker() {
    var host = $('pickerList');
    var rows = rowsForLevel();
    var q = state.query.trim().toLowerCase();

    host.hidden = !q;
    if (!q) { host.innerHTML = ''; return; }

    var hits = rows.filter(function (i) { return matches(i, q); })
      .map(function (i) { return { i: i, s: score(i, q) }; })
      .sort(function (a, b) { return b.s - a.s; })
      .map(function (x) { return x.i; });
    $('searchHint').textContent = hits.length
      ? hits.length.toLocaleString() + ' of ' + rows.length.toLocaleString()
        + ' ' + GEO[state.level].noun + ' indicators match'
      : 'Nothing matches. Try a shorter word.';
    host.innerHTML = hits.length
      ? '<div class="topic-items" style="margin-left:0;border:0;padding-left:0">'
        + hits.slice(0, 200).map(indHtml).join('')
        + (hits.length > 200
           ? '<p class="empty">' + (hits.length - 200).toLocaleString()
             + ' more — keep typing to narrow it.</p>' : '')
        + '</div>'
      : '<p class="empty">No indicator matches “' + esc(state.query) + '”.</p>';
    bind(host);
  }

  function indHtml(i) {
    var yrs = list('years', i);
    var bits = [];
    if (yrs.length === 1) bits.push(yrs[0]);
    else if (yrs.length > 1) bits.push(yrs[0] + '–' + yrs[yrs.length - 1]);
    bits.push(col('shapes', i).toLocaleString() + ' shapes');
    /* The family first, then where the figure was published. The source
       table stays visible: it is the provenance, and the family is only a
       shelf we put it on. */
    var fam = col('h_family', i);
    return '<button class="ind" type="button" data-row="' + i + '"'
         + (state.row === i ? ' aria-current="true"' : '') + '>'
         + esc(col('label', i))
         + (col('h_review', i) === 1
            ? ' <span class="ind-flag" title="The source label for this series'
              + ' is ambiguous and has not been resolved. It is kept exactly as'
              + ' published rather than guessed at.">definition needs review</span>'
            : '')
         + '<span class="meta">' + (fam ? esc(fam) + ' · ' : '')
         + esc(col('group_label', i)) + ' · '
         + bits.join(' · ') + '</span></button>';
  }

  function bind(host) {
    host.querySelectorAll('.ind').forEach(function (b) {
      b.onclick = function () { choose(Number(b.dataset.row)); };
    });
  }

  window.DD_PLACES = { state: state, render: renderPicker };

  /* ── values ─────────────────────────────────────────────────────────────
     Three routes, one interface. Each returns {key -> value} for the facets
     currently chosen. */
  function loadGroup(gk) {
    if (groupCache[gk]) return Promise.resolve(groupCache[gk]);
    var safe = gk.replace(/[^A-Za-z0-9_-]/g, '_');
    var name = 'DD_PLACES_G_' + gk.replace(/[^A-Za-z0-9_]/g, '_');
    return script('data/places/' + safe + '.js').then(function () {
      groupCache[gk] = window[name];
      return groupCache[gk];
    });
  }

  function engine() {
    if (whBooting) return whBooting;
    whBooting = script('assets/js/warehouse.js').then(function () {
      wh = window.DDWarehouse.create({ base: 'data/warehouse/' });
      return wh.init();
    }).then(function () { return wh; });
    return whBooting;
  }

  function q(s) {
    return "'" + String(s == null ? '' : s).replace(/'/g, "''") + "'";
  }

  function fetchValues(i) {
    var src = col('source', i), ind = col('indicator', i), gk = col('group_key', i);

    /* Per 1,000 people applies to any count, whatever published it. The
       census route does its own division below, because it has a year and a
       locality to match; everything else divides here against the latest
       census, which is the only population these places have. */
    if (state.norm && src !== 'census') {
      return Promise.all([rawValues(i), population('2023',
                                                   'POPULATION-2023 / ALL SEXES')])
        .then(function (both) {
          return perThousand(both[0], both[1]);
        });
    }
    return rawValues(i);
  }

  function rawValues(i) {
    var src = col('source', i), ind = col('indicator', i), gk = col('group_key', i);

    if (src === 'place') {
      var fams = (col('families', i) || '').split(' ').filter(Boolean);
      return Promise.all([loadGroup(gk), fams.length ? loadProvenance() : null])
        .then(function (both) {
          var blob = both[0], d = blob.d, out = {}, meta = {}, seenUnit = {};
          var get = function (c, r) { return c.v ? c.v[c.i[r]] : c[r]; };
          for (var r = 0; r < d.value.length; r++) {
            if (get(d.level, r) !== state.level) continue;
            if (get(d.indicator, r) !== ind) continue;
            if (state.year && get(d.year, r) !== state.year) continue;
            var keys = String(get(d.map_key, r)).split(' ');
            var note = get(d.note, r), rel = get(d.relation, r);
            seenUnit[String(get(d.map_key, r))] = { keys: keys, v: d.value[r] };
            for (var k = 0; k < keys.length; k++) {
              out[keys[k]] = d.value[r];
              if (note) meta[keys[k]] = { relation: rel, note: note };
            }
          }
          applyFlags(gk, fams, out, meta);
          var units = [];
          Object.keys(seenUnit).forEach(function (u) {
            units.push({ keys: seenUnit[u].keys, v: seenUnit[u].v });
          });
          return { values: out, meta: meta, units: units };
        });
    }

    if (src === 'crops') {
      var parts = ind.split('|');          // crop|<id>|<measure>
      return engine().then(function (w) {
        return w.query(
          'SELECT district_code AS k, ' + parts[2] + ' AS v FROM crops_district_fy'
          + ' WHERE crop_id = ' + Number(parts[1])
          + '   AND fy = ' + q(state.year)
          + '   AND ' + parts[2] + ' IS NOT NULL');
      }).then(toMap);
    }

    // census: table|indicator|col_label, with the year and facets chosen.
    // A cell published in both censuses carries a key per year, because PBS
    // spells the same band differently in each - "2017=<key>\x1f2023=<key>".
    var year = state.year || list('years', i)[0];

    function cellFor(y) {
      if (ind.indexOf('=') < 0) return ind.split('|');
      var want = null;
      ind.split('\u001f').forEach(function (part) {
        var at = part.indexOf('=');
        if (part.slice(0, at) === String(y)) want = part.slice(at + 1);
      });
      return (want || ind.split('\u001f')[0].split('=').pop()).split('|');
    }

    /* Whether this indicator's values may be added is decided in the build
       and travels on the row; a footprint of several units can only be
       combined for a count. */
    function summable() { return col('h_sum', state.row) === 1; }

    function censusYear(y) {
      var c = cellFor(y);
      return engine().then(function (w) {
        return w.query(
          /* unit, because a shape and a source row are not the same thing and
             two different faults look identical without it. One unit twice is
             an ambiguous source cell; two units once each is a district that
             merged. The first must not be added up, the second must be. */
          'SELECT map_key AS k, value AS v, unit AS u FROM census_panel_' + y
          + ' WHERE table_id = ' + q(c[0])
          + '   AND indicator = ' + q(c[1])
          + "   AND coalesce(col_label, '') = " + q(c[2])
          + '   AND locality = ' + q(state.locality)
          + '   AND sex = ' + q(state.sex)
          + (state.level === 'district' ? "   AND unit_type = 'district'"
                                        : "   AND unit_type <> 'district'")
          + '   AND map_key IS NOT NULL AND value IS NOT NULL');
      }).then(toMap).then(function (r) {
        /* A headcount below zero is not a small number; it is not a number.
           The 2017 panel carries eighteen of them, all in Sherani and all in
           table 7: -20,478 spouses, -20,298 household heads, -15,649 sons and
           daughters. They are dropped from the map and from everything
           derived from it - including a change, because the change of an
           impossible figure is impossible too - and counted so the strip can
           say so. The values stay in the warehouse and in the catalogue; what
           they need is the source re-read, not a guess here.

           Only for census counts. Curated groups keep their change series as
           indicators of their own, where a negative is the whole point. */
        var invalid = 0;
        if (summable()) {
          r.units = (r.units || []).filter(function (u) {
            if (u.v < 0) { invalid += 1; return false; }
            return true;
          });
        }
        var f = byFootprint(r, summable());
        return { values: f.values, meta: r.meta, units: r.units,
                 ambiguous: r.ambiguous, shared: f.uncombinable,
                 invalid: invalid, fpOf: f };
      });
    }

    /* Change is computed here rather than stored, for the 34 cell definitions
       PBS publishes on both censuses. It is a straight difference on the 2023
       boundaries - which is the only reason it can be one, because both panels
       are already drawn on that frame. A place missing from either year is
       left out rather than treated as a zero: an absent 2017 figure would
       otherwise read as growth equal to the whole 2023 value. */
    // Order matters: the normalised routes below handle change themselves,
    // as the change in the RATE. This branch is the change in the count, and
    // it ran first - so picking Change with a share selected quietly gave the
    // count difference while the legend said "change in % of population".
    if (/^\u0394/.test(year) && !state.norm) {
      return Promise.all([censusYear('2017'), censusYear('2023')])
        .then(function (both) {
          /* Grouped across BOTH censuses before subtracting. Keyed on the
             shape string, the old code never matched a split parent to its
             children - "015 170" is not "015" - so the strip skipped them
             while the map subtracted the whole parent from each child. */
          var can = summable();
          var fp = footprints();
          both.forEach(function (yr) {
            (yr.units || []).forEach(function (u) { fp.join(u.keys); });
          });
          var g17 = {}, g23 = {}, keysOf = {}, nameOf = {};
          var gather = function (yr, into) {
            (yr.units || []).forEach(function (u) {
              if (u.v == null) return;
              var g = fp.of(u.keys[0]);
              into[g] = into[g] === undefined ? u.v : into[g] + u.v;
              (keysOf[g] = keysOf[g] || {});
              u.keys.forEach(function (k) { keysOf[g][k] = 1; });
              into[g + '\u0000n'] = (into[g + '\u0000n'] || 0) + 1;
              // which census units the footprint is made of, for the export
              var nm = u.u ? String(u.u).split('\u001f')[1] : '';
              if (nm) (nameOf[g] = nameOf[g] || {})[nm] = 1;
            });
          };
          gather(both[0], g17); gather(both[1], g23);
          var out = {}, units = [], shared = 0;
          Object.keys(keysOf).forEach(function (g) {
            var v17 = g17[g], v23 = g23[g];
            if (v17 == null || v23 == null) return;
            var many = (g17[g + '\u0000n'] || 0) > 1 || (g23[g + '\u0000n'] || 0) > 1;
            var keys = Object.keys(keysOf[g]);
            if (many && !can) { shared += keys.length; return; }
            var d = v23 - v17;
            keys.forEach(function (k) { out[k] = d; });
            units.push({ keys: keys, v: d,
                         u: '\u001f' + Object.keys(nameOf[g] || {}).sort().join(' + ') });
          });
          // Same shape every other route returns; a bare map here read as an
          // undefined .values and the legend silently kept the old scale.
          // A change inherits either year's ambiguity: subtracting a figure
          // we cannot pin down does not pin it down.
          return { values: out, meta: {}, units: units, shared: shared,
                   invalid: (both[0].invalid || 0) + (both[1].invalid || 0),
                   ambiguous: (both[0].ambiguous || 0) + (both[1].ambiguous || 0) };
        });
    }
    if (!state.norm) return censusYear(year);

    /* Per 1,000 people, or as a share, against the whole population of the
       place - not the population of the band. "Divorced, 35-44 per 1,000" is
       therefore per 1,000 of everyone, a rate of the whole place rather than
       a prevalence within the age group, which is why the label says people
       rather than anything narrower.

       Locality is matched, so a rural count is divided by the rural
       population, and each year uses its OWN population. */
    function popIndFor(y) {
      return y === '2017' ? 'POPULATION - 2017 / ALL SEXES'
                          : 'POPULATION-2023 / ALL SEXES';
    }

    function normalisedYear(y) {
      return Promise.all([censusYear(y), population(y, popIndFor(y))])
        .then(function (b) { return perThousand(b[0], b[1]); });
    }

    if (!/^\u0394/.test(year)) return normalisedYear(year);

    /* The change in the RATE, not the rate of the change. Taking the change in
       the count and dividing it by today's population answers a different
       question - it spreads the growth over the people who are here now - and
       where the population itself grew it is not a change in the share at
       all. So each census is normalised against its own population first, and
       the two are subtracted after.

       Both years' parts travel on each unit, because the national change is
       the change in the national rate, not the mean of 136 district changes. */
    return Promise.all([normalisedYear('2017'), normalisedYear('2023')])
      .then(function (b) {
        var a = b[0], c2 = b[1], out = {};
        Object.keys(c2.values).forEach(function (k) {
          if (a.values[k] != null && c2.values[k] != null) {
            out[k] = c2.values[k] - a.values[k];
          }
        });
        var was = {};
        a.units.forEach(function (u) { was[u.keys.join(' ')] = u; });
        var units = [];
        c2.units.forEach(function (u) {
          var prev = was[u.keys.join(' ')];
          if (!prev) return;
          units.push({ keys: u.keys, v: u.v - prev.v,
                       n1: prev.num, d1: prev.den, k1: prev.denKey,
                       n2: u.num, d2: u.den, k2: u.denKey });
        });
        return { values: out, meta: {}, units: units,
                 invalid: (a.invalid || 0) + (c2.invalid || 0),
                 ambiguous: (a.ambiguous || 0) + (c2.ambiguous || 0) };
      });
  }

  /* The count over the population, per unit - and the two parts kept, so the
     strip can total them. Adding up district rates and dividing by 136 would
     give every district equal weight and is not the country's rate. */
  function normFactor() { return state.norm === 'pct' ? 100 : 1000; }
  function normLabel() {
    return state.norm === 'pct' ? '% of population' : 'per 1,000 people';
  }

  function perThousand(res, pop) {
    var v = res.values || res, out = {}, units = [], f = normFactor();
    Object.keys(v).forEach(function (k) {
      if (v[k] != null && pop[k]) out[k] = v[k] / pop[k] * f;
    });
    var unitOf = pop.__unit || {};
    (res.units || []).forEach(function (u) {
      var den = null, denKey = null;
      u.keys.some(function (k) {
        if (pop[k]) { den = pop[k]; denKey = unitOf[k] || k; return true; }
        return false;
      });
      if (u.v != null && den) {
        units.push({ keys: u.keys, v: u.v / den * f, u: u.u,
                     num: u.v, den: den, denKey: denKey });
      }
    });
    return { values: out, meta: {}, units: units,
             invalid: (res.invalid || 0), ambiguous: (res.ambiguous || 0) };
  }

  /* The place's population, from table 1 of the same census, at the same
     locality and unit type. */
  function population(year, indicator) {
    return engine().then(function (w) {
      return w.query(
        'SELECT map_key AS k, value AS v FROM census_panel_' + year
        + " WHERE table_id = '1'"
        + '   AND indicator = ' + q(indicator)
        + '   AND locality = ' + q(state.locality)
        + (state.level === 'district' ? "   AND unit_type = 'district'"
                                      : "   AND unit_type <> 'district'")
        + '   AND map_key IS NOT NULL AND value IS NOT NULL');
    }).then(function (res) {
      // Per shape, and which population unit each shape belongs to. The unit
      // is what stops a total double counting: a district that split after
      // 2017 is one population row drawn on two shapes, and adding the shapes
      // gives 214.8m for a 2017 population of 207.7m.
      var out = {}, unitOf = {};
      res.rows.forEach(function (r) {
        var id = String(r.k);
        id.split(' ').forEach(function (k) { out[k] = r.v; unitOf[k] = id; });
      });
      out.__unit = unitOf;
      return out;
    });
  }

  /* low_n and n_obs are filed under the survey family - dhs_fert - while the
     figures they qualify are filed under the app's group - dhsFertility - so
     they travel in a file of their own and are joined here. Without it the
     flags never meet the figures, and a district map that suppressed an
     estimate from under thirty households would quietly show it. */
  var provCache = null;
  function loadProvenance() {
    if (provCache) return Promise.resolve(provCache);
    return script('data/places/provenance.js').then(function () {
      provCache = window.DD_PLACES_PROV;
      return provCache;
    });
  }

  function applyFlags(gk, fams, out, meta) {
    var P = provCache;
    if (!P || !fams.length) return;
    var get = function (c, r) { return c.v ? c.v[c.i[r]] : c[r]; };
    // MPI and night-lights are withheld outright: a poverty estimate from
    // under thirty households, and a radiance reading off snow and sand
    // rather than activity, are not figures to publish.
    var hide = (gk === 'mpi' || gk === 'nightlights');
    var want = {};
    fams.forEach(function (f) { want[f + '_low_n'] = 1; });
    want.low_n = 1; want.nl_lowc = 1;
    for (var r = 0; r < P.value.length; r++) {
      if (!P.value[r]) continue;
      if (get(P.level, r) !== state.level) continue;
      var fi = get(P.indicator, r);
      if (!want[fi]) continue;
      String(get(P.map_key, r)).split(' ').forEach(function (k) {
        if (!(k in out) && !hide) return;
        meta[k] = { relation: 'low_n', flag: fi, note: noteFor(gk, fi) };
        if (hide) delete out[k];
      });
    }
  }

  function noteFor(gk, flag) {
    if (gk === 'mpi') return 'Suppressed: the PSLM sample for this district is '
      + 'below thirty households, which is too few to estimate from.';
    if (flag === 'nl_lowc') return 'Suppressed: uninhabited terrain, where the '
      + 'radiance is snow and sand albedo rather than activity.';
    return 'Small sample \u2014 this figure rests on fewer observations than the '
      + 'survey\u2019s reliability threshold, so read it as indicative.';
  }

  /* `units` is the rows as published, one per unit. `values` spreads each
     onto every shape that unit is drawn as - which is not the same list.
     Six districts split after 2017 are drawn twice on the 2023 frame and
     carry the same pre-split figure on both halves, so adding up the shapes
     gives 214,818,543 for the 2017 population against a true 207,684,626.
     Anything that totals must use units. */
  /* One row per source unit, not per row returned.

     PBS's 2017 panel carries the same cell more than once for a unit, and the
     warehouse flags 561 series where those copies disagree. Lasbela's owned
     housing units come back as 28,675, 50,989 and 79,664 - rural, urban and
     the total, all three labelled locality "all". Pushing every row put all
     three into the totals strip, so the national figure for owned homes read
     52.2 million against a true figure near half that, and the map drew
     whichever row happened to arrive last.

     Where the copies agree - and 563 series repeat a value up to fifteen
     times - collapsing them is not a judgement, it is the same observation
     counted once, and the total is simply right afterwards.

     Where they disagree there is no rule here that could pick the right one;
     different tables would need different ones. So the value is drawn, the
     reader is told, and the figures that would be wrong - the total and the
     ranking - are withheld rather than guessed. */
  function toMap(res) {
    var out = {}, units = [], byUnit = {}, conflict = {}, ambiguous = 0;
    res.rows.forEach(function (r) {
      /* The shape AND the name. A unit name is not unique - Khanpur, Nowshera
         and Sahiwal are each the name of two different tehsils - so keying on
         the name alone collapsed two real places into one and then reported
         them as a source that contradicted itself. Keying on the name alone
         also would have merged Khanpur's 186,886 with its namesake's
         1,169,138. A true duplicate repeats the same unit on the same shape;
         two places that share a name do not share a map key. */
      var id = r.u == null ? String(r.k) : String(r.k) + '\u001f' + String(r.u);
      if (byUnit[id] !== undefined) {
        // Counted once per place, not once per discarded row: a district with
        // three disagreeing copies is one district we cannot pin down.
        if (byUnit[id].v !== r.v && !conflict[id]) {
          conflict[id] = 1;
          ambiguous += 1;
        }
        return;
      }
      var keys = String(r.k).split(' ');
      byUnit[id] = { keys: keys, v: r.v, u: id };
      keys.forEach(function (k) { out[k] = r.v; });
      units.push(byUnit[id]);
    });
    return { values: out, meta: {}, units: units, ambiguous: ambiguous };
  }

  /* ── comparable footprints ──────────────────────────────────────────────
     A map shape and a census unit are not the same thing, and on the 2023
     frame they disagree in both directions.

       merged   FR Bannu (43,112) and Bannu District (1,167,071) are two 2017
                units drawn on one 2023 shape. The footprint held 1,210,183
                people; the map showed 1,167,071, because the second
                assignment replaced the first.

       split    old Chitral is one 2017 unit carrying map_key "015 170",
                drawn on two 2023 shapes. Its figure was copied onto both, so
                subtracting it from each child gave Lower Chitral a change of
                45,223 - 59,247 = -14,024 children aged 0-4. The comparable
                figure is the old parent against BOTH children,
                70,296 - 59,247 = +11,049.

     Both are the same question - which shapes have to be taken together
     before the arithmetic means anything - so both are answered by grouping
     the shapes into footprints: every key a unit touches is joined to every
     other key that unit touches, and the groups that fall out are the
     smallest areas comparable across the two censuses. A district that
     neither split nor merged is a group of one and nothing changes for it. */
  function footprints() {
    var parent = {};
    function find(k) {
      while (parent[k] !== k) { parent[k] = parent[parent[k]]; k = parent[k]; }
      return k;
    }
    function add(k) { if (parent[k] === undefined) parent[k] = k; }
    return {
      join: function (keys) {
        keys.forEach(add);
        for (var i = 1; i < keys.length; i++) {
          var a = find(keys[0]), b = find(keys[i]);
          if (a !== b) parent[b] = a;
        }
      },
      of: function (k) { return parent[k] === undefined ? k : find(k); },
    };
  }

  /* One value per footprint. A count is the sum of the units in it, which is
     the footprint's own figure. A rate is not: two districts' literacy rates
     do not average into the rate of the pair without their populations, so
     where a footprint holds more than one unit a rate is withheld rather than
     guessed, and the strip says how many shapes that covered. */
  function byFootprint(res, summable) {
    var fp = footprints();
    (res.units || []).forEach(function (u) { fp.join(u.keys); });
    var groups = {}, shared = 0;
    (res.units || []).forEach(function (u) {
      var g = fp.of(u.keys[0]);
      if (!groups[g]) groups[g] = { keys: {}, units: [], v: 0 };
      u.keys.forEach(function (k) { groups[g].keys[k] = 1; });
      groups[g].units.push(u);
    });
    var out = {};
    Object.keys(groups).forEach(function (g) {
      var grp = groups[g], keys = Object.keys(grp.keys);
      var v;
      if (grp.units.length === 1) {
        v = grp.units[0].v;                       // one unit: unchanged
      } else if (summable) {
        v = grp.units.reduce(function (a, u) { return a + u.v; }, 0);
      } else {
        v = null;                                 // rates cannot be combined
        shared += keys.length;
      }
      if (v != null) keys.forEach(function (k) { out[k] = v; });
      grp.v = v;
      grp.keyList = keys;
    });
    return { values: out, groups: groups, fp: fp, uncombinable: shared };
  }

  /* ── choosing ───────────────────────────────────────────────────────────
     The year is offered only where the indicator has more than one; for most
     census series it has exactly one, because the two censuses published
     different tables. */
  var rail = null;

  /* The years an indicator can be drawn for, which is not quite what the index
     stores. Where both census panels carry the same cell, a change between
     them is computable - both sit on the 2023 frame - so it is offered as a
     third year. choose() and the legend both read this, because an earlier
     version built the buttons from here and validated against the raw index:
     the Change button appeared, and clicking it silently snapped back to
     2023. */
  function yearsFor(i) {
    var yrs = list('years', i);
    if (col('source', i) === 'census' && yrs.length === 2
        && yrs[0] === '2017' && yrs[1] === '2023') {
      return yrs.concat(['\u03942017-23']);
    }
    return yrs;
  }

  /* Bumped by every choose(); a response carrying a stale token is dropped. */
  var drawSeq = 0;

  function choose(i, norm) {
    state.row = i;
    if (norm !== undefined) state.norm = norm || '';
    /* A conversion does not survive a move to a series that cannot take one.
       Every route in - a search result, a hierarchy choice, a shared link, a
       district/tehsil switch - ends here, so this is the one place it has to
       be checked. Without it, turning on "% of population" over a disability
       count and then opening "Population over 60 min from care, walking (%)"
       left n=pct alive and divided a published percentage by the population
       again: the strip read 0.00% while the metric dropdown said "All" and
       was greyed out. */
    if (state.norm && col('h_norm', i) !== 1) state.norm = '';
    if (rail) rail.follow(i, state.norm);
    var yrs = yearsFor(i);
    // Default to the most recent actual year, not to the change. The curated
    // pairs carry a third entry - the difference between the censuses - and it
    // sorts last, so taking the last would open every indicator on a change of
    // -21 to 13 rather than on a level.
    state.year = yrs.length ? (yrs.indexOf(state.year) >= 0 ? state.year : latestYear(yrs))
                            : null;
    state.locality = list('localities', i).indexOf(state.locality) >= 0 ? state.locality : 'all';
    state.sex = list('sexes', i).indexOf(state.sex) >= 0 ? state.sex : 'all';
    renderPicker();
    renderLegend();
    $('legend').hidden = false;
    $('legendSub').textContent = 'Loading…';
    /* Whichever request answered last used to win, and on a shared link two
       are always in flight: the page opens on its default indicator and the
       URL then chooses another. The default's values arrived second and were
       written over the ones already drawn, so a deep link to tehsil travel
       time showed 591 tehsils ranging 3,574 to 4,123,354 - the population -
       under a legend that said minutes. Only the newest request may write. */
    var mine = ++drawSeq;
    fetchValues(i).then(function (r) {
      if (mine !== drawSeq) return;
      state.values = r.values;
      state.meta = r.meta;
      state.units = r.units || null;
      state.ambiguous = r.ambiguous || 0;
      state.shared = r.shared || 0;
      state.invalid = r.invalid || 0;
      return paint().then(function () {
        renderLegend();
        renderTotals();
        renderDetail(placeProps(state.place));
        writeUrl();
      });
    }).catch(function (e) {
      if (mine !== drawSeq) return;
      $('legendSub').textContent = 'Could not load this indicator: ' + e.message;
    });
  }

  /* ── the map ────────────────────────────────────────────────────────────
     A count of people or things is heavily skewed here, so counts get
     equal-count classes and rates the smooth ramp, the same rule the district
     map already uses. */
  /* One ramp for every indicator made the map say nothing about what it was
     showing - literacy, night lights and travel time to a clinic all came out
     the same green. The scale follows the indicator's group, as the live map
     does. */
  /* White boundaries disappeared into the cream ground at the edge of the
     country, where a pale fill meets the page. The live map draws them in a
     grey-green that reads against both. */
  var BOUNDARY = '#8a9480';
  /* The selected shape. Gold because it has to read against every one of the
     13 ramps - a dark outline disappears into the dark end of the greens, a
     white one into the pale end of all of them. It is also drawn thicker and
     brought to the front, so a selected district is not half-hidden under its
     neighbours' edges. */
  var SELECTED = '#d4a017';

  /* One place decides how a shape's edge is drawn, so the selected outline
     cannot drift from the ordinary one. */
  function edge(key) {
    return key === state.place
      ? { weight: 3, color: SELECTED, opacity: 1 }
      : { weight: .6, color: BOUNDARY, opacity: 1 };
  }

  /* Restyle in place rather than rebuild the layer: redrawing 649 tehsils to
     move one highlight is slow enough to feel like a stall. */
  function paintSelection() {
    if (!layer) return;
    var g = GEO[state.level];
    layer.eachLayer(function (l) {
      var k = g.key(l.feature.properties);
      var cur = l.options;
      if (cur.fillOpacity === 0) return;          // filtered out by province
      l.setStyle(edge(k));
      if (k === state.place && l.bringToFront) l.bringToFront();
    });
  }

  /* Decimal places for whatever is on screen. A count carries none, but its
     per-1,000 twin is a small number - six divorced people in a district of a
     million is 0.006 - and printing it with the count's precision showed a
     legend running 0 to 0. */
  function dpFor(row) {
    if (state.norm) return 2;
    var dp = col('dp', row);
    return dp == null ? dp : dp;
  }

  function rampFor(row) {
    if (row == null || !window.DDMapScales) return ['#e6f4ec', '#145228'];
    return window.DDMapScales.for(col('group_key', row), col('topic', row));
  }

  function scaleFor(vals, isRate, ramp) {
    var RAMP = ramp || rampFor(state.row);
    if (!vals.length) return null;
    var breaks = chroma.limits(vals, 'q', 5);
    if (isRate) {
      var sc = chroma.scale(RAMP).domain([breaks[0], breaks[breaks.length - 1]]);
      return { colour: function (v) { return sc(v).hex(); }, breaks: breaks };
    }
    var cols = chroma.scale(RAMP).colors(breaks.length - 1);
    return {
      colour: function (v) {
        for (var b = 1; b < breaks.length; b++) if (v <= breaks[b]) return cols[b - 1];
        return cols[cols.length - 1];
      },
      breaks: breaks,
    };
  }

  function ensureGeo() {
    var g = GEO[state.level];
    if (geoCache[state.level]) return Promise.resolve(geoCache[state.level]);
    return script(g.file).then(function () {
      geoCache[state.level] = window[g.global];
      return geoCache[state.level];
    });
  }

  function paint() {
    var g = GEO[state.level];
    // returns a promise: the geometry may still be loading, and the legend
    // cannot draw its range until the scale exists
    return ensureGeo().then(function (geo) {
      var prov = $('provFilter').value;
      var vals = [];
      geo.features.forEach(function (f) {
        var v = state.values[g.key(f.properties)];
        if (v != null && !isNaN(v) && (!prov || g.prov(f.properties) === prov)) vals.push(v);
      });
      var isRate = state.norm || col('dp', state.row) > 0;
      var sc = scaleFor(vals, isRate);

      if (layer) { map.removeLayer(layer); }
      layer = L.geoJSON(geo, {
        style: function (f) {
          var p = f.properties, v = state.values[g.key(p)];
          if (prov && g.prov(p) !== prov) {
            return { fillOpacity: 0, weight: 0, opacity: 0 };
          }
          if (v == null || isNaN(v) || !sc) {
            var why = (state.meta || {})[g.key(p)];
            // a place withheld for a small sample is marked, not just blank:
            // blank reads as "no data", and this is "data we will not stand by"
            return why && why.relation === 'low_n'
              ? { fillColor: '#f5e6b8', fillOpacity: .45, weight: 1,
                  color: '#c49515', opacity: .8, dashArray: '3 3' }
              : { fillColor: '#e2e5ea', fillOpacity: .5, weight: .6,
                  color: '#b9c2b9', opacity: 1 };
          }
          return Object.assign(
            { fillColor: sc.colour(v), fillOpacity: .9 }, edge(g.key(p)));
        },
        onEachFeature: function (f, lyr) {
          lyr.on('click', function () {
            state.place = g.key(f.properties);
            renderDetail(f.properties);
            renderRanks();
            paintSelection();
            writeUrl();
          });
        },
      }).addTo(map);
      if (!map.__fitted) { map.fitBounds(layer.getBounds(), { padding: [12, 12] }); map.__fitted = true; }
      state.scale = sc;
      renderRanks();
      paintSelection();
      buildProvinces(geo, g);
    });
  }

  function buildProvinces(geo, g) {
    var sel = $('provFilter');
    if (sel.dataset.filled) return;
    var seen = {};
    geo.features.forEach(function (f) { seen[g.prov(f.properties)] = 1; });
    Object.keys(seen).sort().forEach(function (p) {
      var o = document.createElement('option');
      o.value = p; o.textContent = p.replace(/\b\w+/g, function (w) {
        return w.charAt(0) + w.slice(1).toLowerCase(); });
      sel.appendChild(o);
    });
    sel.dataset.filled = '1';
    if (state.provWanted) {
      sel.value = state.provWanted;
      if (sel.value !== state.provWanted) sel.value = '';   // not a province here
      state.provWanted = '';
    }
  }

  /* ── legend, with the facet controls the design puts on the map ───────── */
  function renderLegend() {
    var i = state.row;
    if (i == null) return;
    $('legendTitle').textContent = (col('label', i))
      + (state.norm
         ? (/^\u0394/.test(state.year || '') ? ', change in ' : ', ')
           + normLabel()
         : '');
    var n = state.values ? Object.keys(state.values).length : 0;
    $('legendSub').textContent = col('group_label', i) + (n ? ' · ' + n.toLocaleString()
      + ' ' + GEO[state.level].noun + 's' : '');

    var sc = state.scale;
    var dp = dpFor(i);
    $('legendRamp').style.background = 'linear-gradient(90deg,' +
      chroma.scale(rampFor(i)).colors(5).join(',') + ')';
    $('legendLo').textContent = sc ? fmt(sc.breaks[0], dp) : '';
    $('legendHi').textContent = sc ? fmt(sc.breaks[sc.breaks.length - 1], dp) : '';

    var yrs = yearsFor(i), locs = list('localities', i), sexes = list('sexes', i);
    var h = '';
    /* Three shapes, because the series are three different lengths. The census
       pairs have two years and a change; crops run to 39 fiscal years, and 39
       buttons in a 616px card is not a control. Past five, it becomes a slider
       with the year named beside it - the design's YEAR control. */
    if (yrs.length > 5) {
      var at = Math.max(0, yrs.indexOf(state.year));
      h += '<div class="year-slide">'
         + '<span class="year-slide-lab">Year</span>'
         + '<input type="range" id="yearRange" min="0" max="' + (yrs.length - 1)
         + '" value="' + at + '" step="1" aria-label="Year"/>'
         + '<b id="yearNow">' + esc(yrs[at]) + '</b>'
         + '<span class="year-slide-span">' + esc(yrs[0]) + ' to '
         + esc(yrs[yrs.length - 1]) + '</span></div>';
    } else if (yrs.length > 1) {
      /* The two census panels both sit on the 2023 frame, so where a cell
         exists in each the difference is meaningful and is offered. The
         curated pairs already ship a stored change; these 34 get a computed
         one, marked the same way. */
      h += yrs.map(function (y) {
        var lab = /^\u0394/.test(y) ? 'Change' : y;
        return '<button type="button" data-year="' + esc(y) + '" aria-pressed="'
             + (y === state.year) + '">' + esc(lab) + '</button>';
      }).join('');
    } else if (yrs.length === 1) {
      /* Two greyed-out buttons read as a broken control. Most indicators
         genuinely exist in one census only - 4,187 of the 4,601 district
         rows - because the two censuses ask different questions, so this is
         a fact about the indicator and is said as one. The 414 that do span
         both get real buttons above. */
      h += '<span class="year-one">' + esc(yrs[0])
         + ' \u00b7 the only year this is published for</span>';
    }
    $('yearCtl').innerHTML = h;
    $('yearCtl').hidden = !h;
    $('yearCtl').querySelectorAll('button[data-year]').forEach(function (b) {
      b.onclick = function () { state.year = b.dataset.year; choose(state.row); };
    });
    var slider = document.getElementById('yearRange');
    if (slider) {
      /* Name the year as the handle moves, but only redraw on release: each
         step is a fresh query over the warehouse, and re-running it for every
         pixel of a drag makes the slider feel stuck. */
      slider.oninput = function () {
        document.getElementById('yearNow').textContent = yrs[+slider.value];
      };
      slider.onchange = function () {
        state.year = yrs[+slider.value];
        choose(state.row);
      };
    }

    var notes = [];
    if (locs.length > 1) notes.push(locs.join(' / '));
    if (sexes.length > 1) notes.push(sexes.join(' / '));
    $('legendNote').textContent = notes.length ? 'Also published by ' + notes.join('; ') : '';
  }

  /* The strip over the map: how many places are showing, and what they come
     to. It follows the province filter, so picking Punjab totals Punjab.

     Two things it must not do. It must not add up shapes - six districts that
     split after 2017 are drawn twice on the 2023 frame and carry the same
     pre-split figure on each half, which turns a 2017 population of
     207,684,626 into 214,818,543 - so it adds units. And it must not add up
     rates: a total literacy rate is not a number. A per-1,000 view is totalled
     as the whole count over the whole population, which is the only honest
     aggregate of it; a published rate gets its range instead. */
  function renderTotals() {
    var box = $('totals');
    if (!box) return;
    if (state.row == null || !state.units) { box.hidden = true; return; }
    var g = GEO[state.level], prov = $('provFilter').value;
    var keep = state.units.filter(function (u) {
      if (u.v == null || isNaN(u.v)) return false;
      if (!prov) return true;
      return u.keys.some(function (k) {
        var p = placeProps(k);
        return p && g.prov(p) === prov;
      });
    });
    var n = keep.length;
    var where = prov || 'Pakistan';

    /* The source gave more than one figure for the same place and they do not
       agree. Adding them up, or ranking places by whichever arrived last, is
       a number nobody published. The map still shows what is there and the
       coverage count still says how many places reported; the two figures
       that would be invented are not shown. */
    if (state.ambiguous) {
      box.hidden = false;
      box.innerHTML =
        '<div class="tot"><span class="tot-k">' + esc(g.noun + 's showing') + '</span>'
        + '<b>' + n + '</b></div>'
        + '<div class="tot tot-warn"><span class="tot-k">Not totalled or ranked'
        + '</span><b>' + state.ambiguous + ' ' + esc(g.noun)
        + (state.ambiguous === 1 ? '' : 's') + ' with conflicting source rows</b>'
        + '</div>'
        + '<div class="tot"><span class="tot-k">Year</span><b>'
        + esc(yearLabel() || '\u2014') + '</b></div>';
      return;
    }
    /* A mean, a rate, an index or a distance cannot be totalled across
       places. Decided in the build from the kind of quantity, not from
       whether the label happens to contain the word "mean". */
    var rate = !state.norm && col('h_sum', state.row) !== 1;

    var figure, caption;
    if (state.norm) {
      var f = normFactor(), suffix = state.norm === 'pct' ? '%' : '';
      if (keep.length && keep[0].d2 !== undefined) {
        // a change: the change in the whole area's rate, not a mean of
        // district changes, which would weight Harnai like Lahore
        var n1 = 0, d1 = 0, n2 = 0, d2 = 0, s1 = {}, s2 = {};
        keep.forEach(function (u) {
          n1 += (u.n1 || 0); n2 += (u.n2 || 0);
          if (!s1[u.k1]) { s1[u.k1] = 1; d1 += (u.d1 || 0); }
          if (!s2[u.k2]) { s2[u.k2] = 1; d2 += (u.d2 || 0); }
        });
        var delta = (d1 && d2) ? (n2 / d2 - n1 / d1) * f : null;
        figure = delta == null ? '\u2014'
          : (delta > 0 ? '+' : '') + delta.toFixed(2) + suffix;
        caption = 'Change in ' + normLabel().replace(/^%/, 'share')
                + ' \u00b7 ' + where;
      } else {
        var num = 0, den = 0, seenDen = {};
        keep.forEach(function (u) {
          num += (u.num || 0);
          var dk = u.denKey || u.keys.join(' ');
          if (!seenDen[dk]) { seenDen[dk] = 1; den += (u.den || 0); }
        });
        figure = den ? (num / den * f).toFixed(2) + suffix : '\u2014';
        caption = normLabel().replace(/^%/, 'Share') + ' \u00b7 ' + where;
      }
    } else if (rate) {
      var vals = keep.map(function (u) { return u.v; }).sort(function (a, b) { return a - b; });
      figure = vals.length
        ? fmt(vals[0], dpFor(state.row)) + ' to ' + fmt(vals[vals.length - 1], dpFor(state.row))
        : '\u2014';
      caption = 'Range across ' + g.noun + 's \u00b7 a rate cannot be totalled';
    } else {
      figure = big(keep.reduce(function (a, u) { return a + u.v; }, 0));
      caption = 'Total \u00b7 ' + where;
    }

    box.hidden = false;
    box.innerHTML =
      '<div class="tot"><span class="tot-k">' + esc(g.noun + 's showing') + '</span>'
      + '<b>' + n + '</b></div>'
      + (figure === null ? ''
         : '<div class="tot"><span class="tot-k">' + esc(caption) + '</span>'
           + '<b>' + esc(figure) + '</b></div>')
      + '<div class="tot"><span class="tot-k">' + esc(state.norm ? 'Showing' : 'Year')
      + '</span><b>' + esc(state.norm ? normLabel()
                                      : (yearLabel() || '\u2014')) + '</b></div>'
      /* A hole in the map with nothing said about it reads as missing data.
         These are shapes whose 2017 footprint held more than one census unit -
         Bannu and its frontier region, and five like it - and this indicator
         is a rate, which two units' worth of cannot be combined without their
         denominators. The figure is not missing; it is not derivable. */
      + (state.shared
         ? '<div class="tot tot-warn"><span class="tot-k">Not shown</span><b>'
           + state.shared + ' ' + esc(g.noun) + (state.shared === 1 ? '' : 's')
           + ' where a rate cannot be combined across the 2017 units</b></div>'
         : '')
      + (state.invalid
         ? '<div class="tot tot-warn"><span class="tot-k">Dropped</span><b>'
           + state.invalid + ' ' + esc(g.noun) + (state.invalid === 1 ? '' : 's')
           + ' reporting a negative count, which the source cannot mean</b></div>'
         : '');
  }

  /* Fit the map to the chosen province, and back to the country when the
     filter is cleared. The shapes outside it are drawn with no fill, so
     without this you are looking at a mostly empty Pakistan. */
  function zoomProvince() {
    if (!layer || !map) return;
    var g = GEO[state.level], prov = $('provFilter').value, bounds = null;
    layer.eachLayer(function (l) {
      if (prov && g.prov(l.feature.properties) !== prov) return;
      bounds = bounds ? bounds.extend(l.getBounds()) : L.latLngBounds(l.getBounds());
    });
    if (bounds && bounds.isValid()) {
      map.fitBounds(bounds, { padding: [24, 24] });
    }
  }

  function yearLabel() {
    if (!state.year) return '';
    return /^\u0394/.test(state.year) ? 'Change 2017\u201323' : state.year;
  }

  /* 241,499,431 reads as noise in a strip; 241.5m reads as a number. */
  function big(v) {
    var a = Math.abs(v);
    if (a >= 1e9) return (v / 1e9).toFixed(2) + 'bn';
    if (a >= 1e6) return (v / 1e6).toFixed(1) + 'm';
    if (a >= 1e4) return Math.round(v).toLocaleString();
    return fmt(v, dpFor(state.row));
  }

  /* Top and bottom ten, as the live map has. The map shows the pattern; this
     gives the order, which a choropleth genuinely cannot - two districts a
     shade apart are indistinguishable by eye. Counts units rather than shapes,
     because a district split after 2017 is drawn twice on the 2023 frame and
     would otherwise appear twice in one list. */
  /* Selecting from the list does what clicking the shape does, and moves the
     map to it, so the two ways in agree. */
  function pickPlace(key) {
    var g = GEO[state.level], hit = null;
    if (layer) {
      layer.eachLayer(function (l) {
        if (!hit && g.key(l.feature.properties) === key) hit = l;
      });
    }
    state.place = key;
    if (hit) {
      map.fitBounds(hit.getBounds(), { padding: [40, 40] });
      renderDetail(hit.feature.properties);
    } else {
      renderDetail(null);
    }
    renderRanks();
    paintSelection();
    writeUrl();
  }

  function renderRanks() {
    var box = $('ranks');
    if (state.row == null || !state.values) { box.hidden = true; return; }
    // Ranking places by a figure the source states more than one way orders
    // them by which row arrived last. Withheld with the total.
    if (state.ambiguous) { box.hidden = true; return; }
    var dp = dpFor(state.row);
    var g = GEO[state.level], prov = $('provFilter').value;
    var seen = {}, rows = [];
    Object.keys(state.values).forEach(function (k) {
      var v = state.values[k];
      if (v == null) return;
      /* The same filter the map and the total already obey. Choosing Sindh
         moved both and left the ranking on Lahore, Faisalabad and
         Rawalpindi - a national list under a provincial heading. */
      if (prov) {
        var pp = placeProps(k);
        if (!pp || g.prov(pp) !== prov) return;
      }
      var name = nameFor(k);
      if (seen[name]) return;
      seen[name] = 1;
      rows.push({ k: k, name: name, v: v });
    });
    if (rows.length < 4) { box.hidden = true; return; }
    rows.sort(function (a, b) { return b.v - a.v; });
    box.hidden = false;
    fillRank($('rankTopList'), rows.slice(0, 10), dp, 1);
    fillRank($('rankBottomList'), rows.slice(-10).reverse(), dp, rows.length, true);
  }

  function fillRank(host, rows, dp, from, up) {
    host.innerHTML = '';
    rows.forEach(function (r, n) {
      var li = document.createElement('li');
      li.className = 'rank-item' + (r.k === state.place ? ' is-on' : '');
      li.innerHTML = '<span class="rank-n"></span><span class="rank-name"></span>'
                   + '<span class="rank-v"></span>';
      li.querySelector('.rank-n').textContent = up ? (from - n) : (from + n);
      li.querySelector('.rank-name').textContent = r.name;
      li.querySelector('.rank-v').textContent = fmt(r.v, dp);
      li.onclick = function () { pickPlace(r.k); };
      host.appendChild(li);
    });
  }

  /* ── detail ─────────────────────────────────────────────────────────────- */
  function renderDetail(props) {
    var host = $('detailBody');
    if (state.row == null) { return; }
    var g = GEO[state.level];
    if (!state.place) {
      host.innerHTML = '<p class="placeholder">Choose a ' + g.noun
        + ' on the map to see its figure, its rank and the places around it.</p>';
      return;
    }
    var key = state.place, v = state.values ? state.values[key] : null;
    var dp = dpFor(state.row);

    var entries = Object.keys(state.values || {}).map(function (k) {
      return { k: k, v: state.values[k] };
    }).filter(function (e) { return e.v != null && !isNaN(e.v); })
      .sort(function (a, b) { return b.v - a.v; });
    var pos = entries.findIndex(function (e) { return e.k === key; });

    var name = props ? g.name(props) : key;
    var where = props ? (g.prov(props) || '') : '';
    var m = (state.meta || {})[key];

    var h = '<div><div class="eyebrow">Selected</div>'
          + '<div class="unit-name">' + esc(name) + '</div>'
          + '<div class="unit-where">' + esc(title(where)) + '</div></div>';

    if (v == null || isNaN(v)) {
      h += '<p class="notice">No figure here for this indicator. '
         + (props && props.nc
            ? 'Azad Jammu &amp; Kashmir and Gilgit-Baltistan are drawn on every map, '
              + 'but the census does not cover them and most surveys do not reach them.'
            : 'The source does not report this place.')
         + '</p>';
    } else {
      h += '<div class="headline"><span class="v">' + fmt(v, dp) + '</span>'
         + '<span class="w">' + esc(col('label', state.row))
         + (pos >= 0 ? '<br><b>rank ' + (pos + 1) + '</b> of ' + entries.length : '')
         + '</span></div>';
    }
    if (m && m.note) h += '<p class="notice">' + esc(m.note) + '</p>';
    if (m && m.relation === 'low_n' && v != null) {
      h += '<p class="notice">' + esc(noteFor(col('group_key', state.row), m.flag))
         + '</p>';
    }

    if (entries.length > 2) {
      h += '<div><div class="eyebrow">Ranking</div><div class="rank-list">'
         + rankRows(entries, pos, dp) + '</div></div>';
    }
    host.innerHTML = h;
  }

  function title(s) {
    return String(s || '').replace(/\b[\w&]+/g, function (w) {
      return w.length <= 3 && w === w.toUpperCase() ? w
        : w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
    });
  }

  function rankRows(entries, pos, dp) {
    var pick = [0, 1, 2], out = '';
    if (pos > 3) pick.push(-1);
    if (pos >= 0 && pick.indexOf(pos) < 0) pick.push(pos);
    if (pos < entries.length - 2) pick.push(-1);
    pick.push(entries.length - 1);
    var last = null;
    pick.forEach(function (p) {
      if (p === -1) { out += '<div class="rank-gap">···</div>'; return; }
      if (p === last || p < 0 || p >= entries.length) return;
      last = p;
      var e = entries[p];
      out += '<div class="rank-row' + (p === pos ? ' here' : '') + '">'
           + '<span>' + (p + 1) + ' · ' + esc(nameFor(e.k)) + '</span>'
           + '<b>' + fmt(e.v, dp) + '</b></div>';
    });
    return out;
  }

  /* The properties of a shape by key, so a shared link can open on a place
     without the reader having clicked it. */
  function placeProps(key) {
    if (!key) return null;
    var geo = geoCache[state.level], g = GEO[state.level];
    if (!geo) return null;
    var hit = null;
    geo.features.some(function (f) {
      if (g.key(f.properties) === key) { hit = f.properties; return true; }
      return false;
    });
    return hit;
  }

  var nameIndex = null;
  function nameFor(key) {
    if (!nameIndex) {
      nameIndex = {};
      var geo = geoCache[state.level], g = GEO[state.level];
      if (geo) geo.features.forEach(function (f) {
        nameIndex[g.key(f.properties)] = title(g.name(f.properties));
      });
    }
    return nameIndex[key] || key;
  }


  /* ── the schools overlay ─────────────────────────────────────────────────
     121,020 points is far too many for one marker each, so they are drawn to a
     canvas, and only the ones inside the current view. A school with a real fix
     is a filled dot; one placed at a settlement or tehsil centroid is a hollow
     ring, because a centroid is a claim about an area and not about a building.
     Below the zoom where individual schools mean anything, the layer says how
     many are in view rather than drawing a solid mass of ink. */
  var SchoolLayer = L.Layer.extend({
    onAdd: function (m) {
      this._map = m;
      this._c = L.DomUtil.create('canvas', 'leaflet-zoom-animated');
      this._c.style.pointerEvents = 'none';
      m.getPanes().overlayPane.appendChild(this._c);
      m.on('moveend zoomend resize', this._draw, this);
      this._draw();
    },
    onRemove: function (m) {
      m.off('moveend zoomend resize', this._draw, this);
      if (this._c && this._c.parentNode) this._c.parentNode.removeChild(this._c);
      this._c = null;
    },
    _draw: function () {
      var pts = window.DD_SCHOOL_POINTS;
      if (!pts || !this._c) return;
      var m = this._map, size = m.getSize(), tl = m.containerPointToLayerPoint([0, 0]);
      L.DomUtil.setPosition(this._c, tl);
      var dpr = window.devicePixelRatio || 1;
      this._c.width = size.x * dpr; this._c.height = size.y * dpr;
      this._c.style.width = size.x + 'px'; this._c.style.height = size.y + 'px';
      var g = this._c.getContext('2d');
      g.setTransform(dpr, 0, 0, dpr, 0, 0);
      g.clearRect(0, 0, size.x, size.y);

      var b = m.getBounds(), z = m.getZoom();
      var s = Math.max(1, Math.min(3, (z - 5) * 0.8));
      var ink = getComputedStyle(document.documentElement)
        .getPropertyValue('--green-900').trim() || '#0c3a1e';
      g.strokeStyle = ink; g.fillStyle = ink; g.lineWidth = 1;

      var n = pts.n, LAT = pts.lat, LNG = pts.lng, F = pts.f;
      var S = b.getSouth(), N = b.getNorth(), W = b.getWest(), E = b.getEast();
      var cap = z < 8 ? 12000 : 60000;      // enough to read, not enough to blot

      // Two passes. The points are stored in latitude order, so drawing the
      // first `cap` of them would paint the southern third of the country and
      // leave the north bare - a picture of the storage order, not of the
      // schools. Count first, then take every stride-th, which thins the whole
      // country evenly.
      var la = 0, ln = 0, inView = 0, i, y, x;
      for (i = 0; i < n; i++) {
        la += LAT[i]; ln += LNG[i];
        y = la / 1e4; x = ln / 1e4;
        if (y >= S && y <= N && x >= W && x <= E) inView++;
      }
      var stride = Math.max(1, Math.ceil(inView / cap));

      la = 0; ln = 0;
      var drawn = 0, seen = 0;
      for (i = 0; i < n; i++) {
        la += LAT[i]; ln += LNG[i];
        y = la / 1e4; x = ln / 1e4;
        if (y < S || y > N || x < W || x > E) continue;
        if (seen++ % stride) continue;
        var p = m.latLngToContainerPoint([y, x]);
        g.beginPath();
        g.arc(p.x, p.y, s, 0, 6.283);
        if (F[i] >= 100) { g.globalAlpha = .8; g.fill(); }
        else { g.globalAlpha = .55; g.stroke(); }
        drawn++;
      }
      g.globalAlpha = 1;
      var note = document.getElementById('ovSchoolsN');
      if (note) {
        // say plainly when the map is showing a sample rather than everything
        note.textContent = drawn < inView
          ? '1 in ' + stride + ' of ' + inView.toLocaleString()
          : inView.toLocaleString() + (inView >= n ? '' : ' in view');
      }
    },
  });

  var schoolLayer = null;

  function toggleSchools(on) {
    document.getElementById('ovKey').hidden = !on;
    if (!on) {
      if (schoolLayer) { map.removeLayer(schoolLayer); schoolLayer = null; }
      document.getElementById('ovSchoolsN').textContent = '121k';
      return;
    }
    var box = document.getElementById('ovSchools');
    box.disabled = true;
    document.getElementById('ovSchoolsN').textContent = 'loading\u2026';
    script('data/places/overlay_schools.js').then(function () {
      schoolLayer = new SchoolLayer();
      map.addLayer(schoolLayer);
    }).catch(function () {
      document.getElementById('ovSchoolsN').textContent = 'could not load';
      box.checked = false;
    }).then(function () { box.disabled = false; });
  }


  /* ── sharing ─────────────────────────────────────────────────────────────
     The URL carries what is on screen, so a link opens on the same indicator,
     level, year and place. The indicator is identified by its group and id
     rather than its position in the index, because the index is rebuilt every
     time the data is and a row number would not survive that. */
  function writeUrl() {
    if (state.row == null) return;
    var q = new URLSearchParams();
    q.set('g', col('group_key', state.row));
    q.set('i', col('indicator', state.row));
    q.set('lv', state.level);
    if (state.year) q.set('y', state.year);
    if (state.norm) q.set('n', state.norm);
    if (state.locality !== 'all') q.set('loc', state.locality);
    if (state.sex !== 'all') q.set('sex', state.sex);
    if (state.place) q.set('p', state.place);
    if (document.getElementById('ovSchools').checked) q.set('ov', 'schools');
    // Shared without it, a link to "Sindh" opened on the whole country.
    var pv = $('provFilter') && $('provFilter').value;
    if (pv) q.set('prov', pv);
    history.replaceState(null, '', location.pathname + '?' + q.toString());
  }

  function readUrl() {
    var q = new URLSearchParams(location.search);
    var g = q.get('g'), ind = q.get('i');
    if (q.get('lv') === 'tehsil') setLevel('tehsil');
    if (!g || !ind) return false;
    var row = -1;
    for (var i = 0; i < N; i++) {
      if (col('level', i) === state.level && col('group_key', i) === g
          && col('indicator', i) === ind) { row = i; break; }
    }
    if (row < 0) return false;
    if (q.get('y')) state.year = q.get('y');
    if (q.get('loc')) state.locality = q.get('loc');
    state.norm = ['pct', 'per1000'].indexOf(q.get('n')) >= 0 ? q.get('n') : '';
    if (q.get('sex')) state.sex = q.get('sex');
    state.place = q.get('p') || null;
    /* Applied after buildProvinces has filled the dropdown - setting .value
       before the options exist selects nothing and the filter is silently
       lost on every shared link. */
    state.provWanted = q.get('prov') || '';
    if (q.get('ov') === 'schools') {
      var box = document.getElementById('ovSchools');
      box.checked = true;
      toggleSchools(true);
    }
    choose(row);
    return true;
  }

  function share() {
    writeUrl();
    var btn = document.getElementById('shareBtn');
    var said = function (t) {
      btn.textContent = t;
      setTimeout(function () { btn.textContent = 'Share'; }, 1800);
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(location.href)
        .then(function () { said('Link copied'); })
        .catch(function () { said('Link is in the address bar'); });
    } else {
      said('Link is in the address bar');
    }
  }

  /* ── boot ───────────────────────────────────────────────────────────────- */
  function boot() {
    map = L.map('placesMap', {
      zoomControl: true, attributionControl: false,
      scrollWheelZoom: true, minZoom: 4, maxZoom: 11,
    }).setView([30.2, 69.4], 5);

    $('geoDistrict').onclick = function () { setLevel('district'); };
    $('geoTehsil').onclick = function () { setLevel('tehsil'); };
    $('indSearch').oninput = function () { state.query = this.value; renderPicker(); };
    $('provFilter').onchange = function () {
      if (state.row != null) {
        paint().then(function () {
          renderTotals(); renderRanks(); writeUrl(); zoomProvince();
        });
      } else { writeUrl(); zoomProvince(); }
    };
    $('placeSearch').oninput = function () { findPlace(this.value); };
    $('csvBtn').onclick = downloadCsv;
    $('ovSchools').onchange = function () { toggleSchools(this.checked); writeUrl(); };
    $('shareBtn').onclick = share;

    renderPicker();
    rail = window.DDPlacesRail && window.DDPlacesRail.mount({
      el: $('rail'), IX: IX, N: N, col: col, list: list, level: state.level,
      row: state.row,
      onChange: function (row, norm) { state.norm = norm || ''; choose(row); },
    });
    if (!readUrl() && rail) rail.fire();
  }

  function setLevel(lv) {
    if (state.level === lv) return;
    state.level = lv;
    state.row = null; state.values = null; state.place = null; nameIndex = null;
    $('geoDistrict').setAttribute('aria-pressed', String(lv === 'district'));
    $('geoTehsil').setAttribute('aria-pressed', String(lv === 'tehsil'));
    $('provFilter').removeAttribute('data-filled');
    $('provFilter').innerHTML = '<option value="">All provinces</option>';
    $('legend').hidden = true;
    if (layer) { map.removeLayer(layer); layer = null; }
    map.__fitted = false;
    renderPicker();
    renderDetail();
    // Last, and not before the teardown above: the rebuild draws the new
    // level's default map, and running it first meant the lines below then
    // hid the legend and removed the layer it had just made.
    if (rail) rail.rebuild(lv);
  }

  function findPlace(q) {
    q = q.trim().toLowerCase();
    if (q.length < 3 || !layer) return;
    var g = GEO[state.level], hit = null;
    layer.eachLayer(function (l) {
      if (hit) return;
      if (String(g.name(l.feature.properties) || '').toLowerCase().indexOf(q) === 0) hit = l;
    });
    if (hit) {
      map.fitBounds(hit.getBounds(), { padding: [40, 40] });
      state.place = g.key(hit.feature.properties);
      renderDetail(hit.feature.properties);
      renderRanks();
      paintSelection();
      writeUrl();
    }
  }

  /* The export used to carry four columns - place, province, shape key and a
     heading taken from the indicator label - so a share of the population and
     the count it came from downloaded under the same heading and the same
     filename, and a figure published for a larger 2017 unit appeared on each
     of its child shapes with nothing to say they were the same observation.
     Everything that decides what the number MEANS travels with it now: when
     it was measured, for whom, what was done to it, and what it was divided
     by. The province filter applies here too - it already applied to the map
     and the total, so a download that quietly ignored it described a
     different country from the one on screen. */
  function downloadCsv() {
    if (state.row == null || !state.values) return;
    var g = GEO[state.level], prov = $('provFilter').value;
    var src = col('source', state.row);
    var mode = state.norm === 'pct' ? '% of population'
             : state.norm === 'per1000' ? 'per 1,000 people' : 'as published';

    // shape key -> the source observation behind it
    var by = {};
    (state.units || []).forEach(function (u) {
      u.keys.forEach(function (k) { by[k] = u; });
    });

    // "breakdown", not "unit": source_unit below is a place, and two columns
    // called unit meaning different things is how a reader mis-joins a table.
    var rows = [[g.noun, 'province', 'shape_key', 'value', 'breakdown',
                 'year', 'locality', 'sex', 'metric_mode',
                 'numerator', 'denominator', 'source_unit',
                 'shapes_sharing_this_figure', 'dataset', 'indicator_key',
                 'quality']];
    var geo = geoCache[state.level];
    if (geo) geo.features.forEach(function (f) {
      var k = g.key(f.properties), v = state.values[k];
      if (v == null) return;
      if (prov && g.prov(f.properties) !== prov) return;
      var u = by[k] || {};
      var unitName = u.u ? String(u.u).split('\u001f')[1] || '' : '';
      var shares = u.keys ? u.keys.length : 1;
      var quality = state.ambiguous ? 'source rows for this series conflict'
                  : shares > 1 ? 'figure published for a larger source unit'
                  : '';
      rows.push([g.name(f.properties), g.prov(f.properties), k, v,
                 state.norm ? mode : (col('metric', state.row) || ''),
                 yearLabel() || '', state.locality, state.sex, mode,
                 u.num == null ? '' : u.num, u.den == null ? '' : u.den,
                 unitName, shares,
                 col('dataset', state.row) || src,
                 col('indicator', state.row), quality]);
    });
    var csv = rows.map(function (r) {
      return r.map(function (c) {
        var t = String(c == null ? '' : c);
        return /[",\n]/.test(t) ? '"' + t.replace(/"/g, '""') + '"' : t;
      }).join(',');
    }).join('\n');
    var a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
    /* The year and the conversion in the name, because two downloads of the
       same indicator are two different tables. */
    a.download = 'data_darbar_'
      + col('indicator', state.row).replace(/[^A-Za-z0-9]+/g, '_')
      + '_' + state.level
      + (state.year ? '_' + String(state.year).replace(/[^A-Za-z0-9]+/g, '') : '')
      + (state.norm ? '_' + state.norm : '')
      + (prov ? '_' + prov.replace(/[^A-Za-z0-9]+/g, '') : '')
      + '.csv';
    a.click();
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 1000);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
