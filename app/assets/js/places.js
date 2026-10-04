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

  /* A merged key is "2017=<cell>" or "2017:female=<cell>", parts joined by
     \x1f, and it is recognised by that year prefix - never by the presence of
     an "=". Census table 12 publishes "Literate >=10", "Population >=10" and
     "Population >=5", and reading their ">=" as a merge asked the warehouse
     for an indicator called "10": three measures, 727 places each, drew an
     empty map. */
  var MERGED = /^((?:19|20)\d\d)(?::([a-z]+))?=/;
  function mergedParts(ind) {
    ind = String(ind || '');
    if (!MERGED.test(ind)) return null;
    return ind.split(SEP).map(function (part) {
      var m = MERGED.exec(part);
      return m ? { year: m[1], sex: m[2] || null, cell: part.slice(m[0].length) } : null;
    }).filter(Boolean);
  }

  var state = {
    level: 'district',
    row: null,          // index row of the chosen indicator
    year: null,
    locality: 'all',
    sex: 'all',
    norm: '',           // '', 'pct' or 'per1000' - how a count is read
    plain: false,       // shading off: a plain map for reading the point layers
    street: false,      // OpenStreetMap under the plain map, districts as outlines
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

  /* Search, the topic list and the library live in places-rail.js now, and
     open in a panel over the map rather than in a list under six dropdowns.
     This stays as a hook other scripts call. */
  function renderPicker() {}

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

    /* The key carries a cell per year, and per sex where PBS published the
       sexes as separate columns rather than as a sex dimension: "2017=<key>"
       or "2017:female=<key>". Both forms are read here, because a cell that
       merged only across years keeps the shorter one.

       Falling back to the first part is deliberate and load-bearing: a sex
       chosen on one indicator persists to the next, and if that one has no
       such column the map must still draw something rather than going blank
       on a facet the reader cannot see they are holding. */
    function cellFor(y, sx) {
      var parts = mergedParts(ind);
      if (!parts) return ind.split('|');
      var want = null, yearOnly = null;
      parts.forEach(function (p) {
        if (p.year !== String(y)) return;
        if (!p.sex) { yearOnly = p.cell; return; }
        if (sx && p.sex === String(sx)) want = p.cell;
        if (yearOnly == null) yearOnly = p.cell;
      });
      var out = (want || yearOnly || parts[0].cell).split('|');
      /* Where the key carries the sex, the panel does NOT: PBS published
         those as separate columns, so every one of their rows sits under
         sex='all' and filtering on the chosen sex as well would return
         nothing at all. The key has already selected it. */
      out.bySex = parts.some(function (p) { return !!p.sex; });
      return out;
    }

    /* Whether this indicator's values may be added is decided in the build
       and travels on the row; a footprint of several units can only be
       combined for a count. */
    function summable() { return col('h_sum', state.row) === 1; }

    function censusYear(y) {
      var c = cellFor(y, state.sex);
      return engine().then(function (w) {
        return w.query(
          /* unit, because a shape and a source row are not the same thing and
             two different faults look identical without it. One unit twice is
             an ambiguous source cell; two units once each is a district that
             merged. The first must not be added up, the second must be. */
          'SELECT map_key AS k, value AS v, unit AS u, map_weight AS w,'
          + ' map_relation AS rel, map_note AS note FROM census_panel_' + y
          + ' WHERE table_id = ' + q(c[0])
          + '   AND indicator = ' + q(c[1])
          + "   AND coalesce(col_label, '') = " + q(c[2])
          + '   AND locality = ' + q(state.locality)
          + '   AND sex = ' + q(c.bySex ? 'all' : state.sex)
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
                 ambiguous: r.ambiguous, shared: f.uncombinable, weighted: f.weighted,
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
              if (u.conflict) { into[g + '\u0000c'] = 1; return; }
              into[g] = into[g] === undefined ? u.v : into[g] + u.v;
              // the rate route needs each unit's weight, and to know if any lacks one
              if (u.w > 0) {
                into[g + '\u0000w'] = (into[g + '\u0000w'] || 0) + u.w;
                into[g + '\u0000vw'] = (into[g + '\u0000vw'] || 0) + u.v * u.w;
              } else into[g + '\u0000x'] = 1;
              (keysOf[g] = keysOf[g] || {});
              u.keys.forEach(function (k) { keysOf[g][k] = 1; });
              into[g + '\u0000n'] = (into[g + '\u0000n'] || 0) + 1;
              // which census units the footprint is made of, for the export
              var nm = u.u ? String(u.u).split('\u001f')[1] : '';
              if (nm) (nameOf[g] = nameOf[g] || {})[nm] = 1;
            });
          };
          gather(both[0], g17); gather(both[1], g23);
          var out = {}, units = [], shared = 0, weighted = 0;
          var mean = function (side, g) {
            return (side[g + '\u0000n'] || 0) > 1
              ? (side[g + '\u0000x'] ? null : side[g + '\u0000vw'] / side[g + '\u0000w'])
              : side[g];
          };
          Object.keys(keysOf).forEach(function (g) {
            var v17 = g17[g], v23 = g23[g];
            if (v17 == null || v23 == null) return;
            if (g17[g + '\u0000c'] || g23[g + '\u0000c']) return;
            var many = (g17[g + '\u0000n'] || 0) > 1 || (g23[g + '\u0000n'] || 0) > 1;
            var keys = Object.keys(keysOf[g]);
            if (many && !can) {
              // a rate on a combined footprint: weighted on both sides, or not at all
              v17 = mean(g17, g); v23 = mean(g23, g);
              if (v17 == null || v23 == null) { shared += keys.length; return; }
              weighted += keys.length;
            }
            var d = v23 - v17;
            keys.forEach(function (k) { out[k] = d; });
            units.push({ keys: keys, v: d,
                         u: '\u001f' + Object.keys(nameOf[g] || {}).sort().join(' + ') });
          });
          // Same shape every other route returns; a bare map here read as an
          // undefined .values and the legend silently kept the old scale.
          // A change inherits either year's ambiguity: subtracting a figure
          // we cannot pin down does not pin it down.
          return { values: out, meta: {}, units: units, shared: shared, weighted: weighted,
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
    var out = {}, units = [], byUnit = {}, conflict = {}, ambiguous = 0, meta = {};
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
          // Neither figure is drawn: the first to arrive is not the
          // published one any more than the second.
          byUnit[id].conflict = true;
        }
        return;
      }
      var keys = String(r.k).split(' ');
      /* The boundary story travels with the figure. The query used to return
         a key and a value and nothing else, so a district that merged, split
         or was matched only approximately looked exactly like one that did
         not - and the note the warehouse wrote about it reached no one. */
      byUnit[id] = { keys: keys, v: r.v, u: id, rel: r.rel, note: r.note,
                     w: r.w == null ? null : Number(r.w) };
      keys.forEach(function (k) {
        out[k] = r.v;
        if ((r.rel && r.rel !== 'exact') || r.note) {
          meta[k] = { boundary: r.rel, boundaryNote: r.note };
        }
      });
      units.push(byUnit[id]);
    });
    return { values: out, meta: meta, units: units, ambiguous: ambiguous };
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
     do not average into the rate of the pair without their populations. Every
     2017 unit now carries its 2017 population (map_weight), so a rate is the
     units' population-weighted mean - exact for a per-head rate, close for one
     whose base is a part of the population (literacy is per person aged 10+),
     and said so in the strip. Without a weight for every unit it is still
     withheld rather than guessed. */
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
    var out = {}, weighted = 0;
    Object.keys(groups).forEach(function (g) {
      var grp = groups[g], keys = Object.keys(grp.keys);
      var v;
      if (grp.units.some(function (u) { return u.conflict; })) {
        v = null;                                 // two figures for one place
      } else if (grp.units.length === 1) {
        v = grp.units[0].v;                       // one unit: unchanged
      } else if (summable) {
        v = grp.units.reduce(function (a, u) { return a + u.v; }, 0);
      } else if (grp.units.every(function (u) { return u.w > 0; })) {
        var sw = 0, svw = 0;
        grp.units.forEach(function (u) { sw += u.w; svw += u.v * u.w; });
        v = svw / sw;                             // population-weighted mean
        weighted += keys.length;
      } else {
        v = null;                                 // no weights: not derivable
        shared += keys.length;
      }
      if (v != null) keys.forEach(function (k) { out[k] = v; });
      grp.v = v;
      grp.keyList = keys;
    });
    return { values: out, groups: groups, fp: fp, uncombinable: shared,
             weighted: weighted };
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

  /* Keep what the reader chose if this series has it; otherwise the series'
     own default, which is not always "all".

     Falling back to "all" unconditionally was how 136 advertised 2017 district
     series opened on an empty map: "Percent - Rooms per Housing Unit" is
     published for rural mouzas only, its available list is ["rural"], and
     asking the warehouse for locality "all" returned nothing at all. A series
     with no "all" is not a series with no data. */
  function pickFacet(avail, held) {
    if (!avail.length) return 'all';
    if (avail.indexOf(held) >= 0) return held;
    if (avail.indexOf('all') >= 0) return 'all';
    return avail[0];
  }

  /* Bumped by every choose(); a response carrying a stale token is dropped. */
  var drawSeq = 0;

  function choose(i, norm) {
    state.row = i;
    if (state.closeSheet) state.closeSheet();
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
    /* A breakdown the reader chose is kept when the next measure has it. When
       it does not, the map changes whose figure it shows - from women to
       everyone - and that is said, not done silently. */
    var heldSex = state.sex, heldLoc = state.locality;
    state.locality = pickFacet(list('localities', i), state.locality);
    state.sex = pickFacet(list('sexes', i), state.sex);
    var WHO = { female: 'women and girls', male: 'men and boys',
                transgender: 'transgender people', rural: 'rural areas',
                urban: 'urban areas' };
    state.resetNote = '';
    if (heldSex !== 'all' && state.sex !== heldSex && WHO[heldSex]) {
      state.resetNote = 'Not published for ' + WHO[heldSex]
        + ' - this measure is shown for all sexes.';
    } else if (heldLoc !== 'all' && state.locality !== heldLoc && WHO[heldLoc]) {
      state.resetNote = 'Not published for ' + WHO[heldLoc]
        + ' separately - this measure is shown for '
        + (state.locality === 'all' ? 'urban and rural together' : WHO[state.locality] || state.locality) + '.';
    }
    renderLegend();
    $('legend').hidden = false;
    if ($('measureHead').hidden) {
      $('measureHead').hidden = false;
      // The stage under the header just got shorter; Leaflet only notices a
      // resized container when told.
      if (map) map.invalidateSize();
    }
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
      state.weighted = r.weighted || 0;
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
  /* The plain background: a warm pale grey a shade off the page, so districts
     still read as shapes and every point colour sits on the same ground. */
  var PLAIN = '#efece3';

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

  /* The country is fitted into the part of the map the reader can see: below
     the tools along the top and above the key card, which sits over the
     south-west corner and otherwise covered Sindh and Karachi. Padding the
     bottom by the card's height keeps the country large; padding the left by
     its width left 362 pixels and dropped a whole zoom level. */
  function fitPad() {
    var card = $('legend'), h = 0;
    if (card && !card.hidden) h = card.offsetHeight;
    var stage = $('placesMap'), H = stage ? stage.offsetHeight : 600;
    // Clear the tools along the top: one row on a desktop, two on a phone.
    var tools = document.querySelector('.map-tools');
    var top = tools ? tools.offsetTop + tools.offsetHeight + 10 : 56;
    return { paddingTopLeft: L.point(12, Math.max(56, top)),
             paddingBottomRight: L.point(12, Math.min(h + 24, H * 0.4)) };
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
          if (state.plain) {
            // Over the street map a district is its outline only, so the
            // streets and towns read through it.
            return Object.assign({ fillColor: PLAIN, fillOpacity: state.street ? 0 : 1 },
                                 edge(g.key(p)));
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
      if (!map.__fitted) { map.fitBounds(layer.getBounds(), fitPad()); map.__fitted = true; }
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

  /* One line that says what the map is: when, for whom, from what, and how
     much of the country it covers. It stays on screen with the map, because
     the controls that set it are in a card a reader may have collapsed. */
  function renderHead() {
    var i = state.row;
    if (i == null) return;
    var g = GEO[state.level];
    var bits = [];
    // A survey's date is in its name - "PSLM 2019-20" - so "undated" is said
    // only where nothing else dates it, and never invented.
    var y = yearLabel();
    if (y && !(y === 'undated' && /(19|20)\d\d/.test(col('dataset', i)))) bits.push(y);
    var FAC = { all: null, rural: 'Rural', urban: 'Urban', male: 'Men and boys',
                female: 'Women and girls', transgender: 'Transgender' };
    if (list('sexes', i).length > 1) bits.push(FAC[state.sex] || 'All sexes');
    else if (FAC[state.sex]) bits.push(FAC[state.sex]);
    if (list('localities', i).length > 1) bits.push(FAC[state.locality] || 'Urban and rural');
    else if (FAC[state.locality]) bits.push(FAC[state.locality] + ' only');
    bits.push(String(col('dataset', i) || '').replace(/,? pull of [0-9-]+$/, ''));
    if (state.values) {
      var prov = $('provFilter').value;
      var shown = Object.keys(state.values).filter(function (k) {
        if (!prov) return true;
        var pp = placeProps(k);
        return pp && g.prov(pp) === prov;
      }).length;
      var whole = (geoCache[state.level] || { features: [] }).features.length;
      bits.push(prov
        ? shown.toLocaleString() + ' ' + g.noun + 's with data'
        : shown.toLocaleString() + (whole && shown < whole ? ' of ' + whole : '')
          + ' ' + g.noun + 's with data');
    }
    var sub = $('legendSub');
    sub.textContent = bits.filter(Boolean).join(' \u00b7 ');
    /* Whose figure is on the map. PBS asks that derived figures not pass as
       official ones, and the same is owed to every source: a figure Data
       Darbar computed - from survey microdata, satellite grids, published
       counts, or here on the page as a change, a rate or a combined area - is
       labelled as ours, with the reason on hover. */
    var why = [];
    if (IX.derived && col('derived', i) === 1) why.push('constructed by Data Darbar from the source (computed, estimated from survey microdata or aggregated from grids), not published by it');
    if (/^\u0394/.test(state.year || '')) why.push('the change is computed here from the two censuses');
    if (state.norm) why.push('the rate is computed here against the population');
    if (state.weighted) why.push('combined areas are averaged here on 2017 population');
    if (why.length) {
      var tag = document.createElement('span');
      tag.className = 'mh-derived';
      tag.textContent = 'Derived';
      tag.title = 'Derived: ' + why.join('; ') + '.';
      sub.appendChild(document.createTextNode(' \u00b7 '));
      sub.appendChild(tag);
    }
  }

  /* The measure's own breakdowns - age bands, a table's columns - and, for a
     count of people, how to read it against the population. Offered only
     where they exist: a single-option selector is furniture, and a greyed one
     reads as broken. */
  function renderBreak(i, m) {
    var h = '';
    if (m && m.rows.length > 1) {
      var ages = m.rows.every(function (r) {
        return /\d|under|above|below|ages|^all$/i.test(r.metric);
      });
      h += '<label class="bk"><span class="facet-lab">'
         + (ages ? 'Age group' : 'Breakdown') + '</span>'
         + '<select id="breakSel" aria-label="' + (ages ? 'Age group' : 'Breakdown') + '">'
         + m.rows.map(function (r) {
             return '<option value="' + r.row + '"' + (r.row === i ? ' selected' : '')
                  + '>' + esc(r.metric) + '</option>';
           }).join('')
         + '</select></label>';
    }
    if (col('h_norm', i) === 1) {
      h += '<div class="showas" role="group" aria-label="Show as">'
         + '<span class="facet-lab">Show as</span>'
         + [['', 'Count'], ['pct', '% of population'], ['per1000', 'per 1,000']]
             .map(function (o) {
               return '<button type="button" data-norm="' + o[0] + '" aria-pressed="'
                    + ((state.norm || '') === o[0]) + '">' + o[1] + '</button>';
             }).join('')
         + '</div>';
    }
    var box = $('breakCtl');
    box.innerHTML = h;
    box.hidden = !h;
    var sel = document.getElementById('breakSel');
    if (sel) sel.onchange = function () { choose(Number(sel.value)); };
    box.querySelectorAll('[data-norm]').forEach(function (b) {
      b.onclick = function () { choose(state.row, b.dataset.norm); };
    });
    // The exact table and key belong one step away, with the definition,
    // not in the way of choosing: they are how a researcher checks the
    // number, not how a reader finds it.
    $('legendSrc').textContent = 'Source: '
      + credited(String(col('dataset', i) || '').replace(/,? pull of [0-9-]+$/, ''))
      + (col('group_label', i) ? ' \u00b7 ' + col('group_label', i) : '')
      + '. Series key: ' + col('group_key', i) + ' / '
      + ((mergedParts(col('indicator', i)) || [{ cell: col('indicator', i) }])[0].cell)
      + '.';
  }

  /* ── legend, with the facet controls the design puts on the map ───────── */
  function renderLegend() {
    var i = state.row;
    if (i == null) return;
    /* The facets belong in the title. A map of 18 transgender Afghans aged
       15-19 headed "Afghani, 15-19" is a map of the wrong thing, and the
       reader has no other place to read what they are looking at once they
       scroll past the controls. */
    var m = rail && rail.measureOf ? rail.measureOf(i) : null;
    var band = null;
    if (m && m.rows.length > 1) {
      m.rows.forEach(function (r) { if (r.row === i) band = r.metric; });
    }
    var picked = [];
    if (band && band !== 'All') picked.push(band);
    var cap = function (v) { return v.charAt(0).toUpperCase() + v.slice(1); };
    if (state.locality && state.locality !== 'all') picked.push(cap(state.locality));
    if (state.sex && state.sex !== 'all') picked.push(cap(state.sex));
    $('legendTitle').textContent = (m ? m.name : col('label', i))
      + (picked.length ? ' \u00b7 ' + picked.join(', ') : '')
      + (state.norm
         ? (/^\u0394/.test(state.year || '') ? ', change in ' : ', ')
           + normLabel()
         : '');
    renderHead();
    renderBreak(i, m);

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

    /* Residence and sex as controls, not as a sentence. The legend used to
       say "Also published by all / rural / urban" and leave it there: 4,258
       entries advertised a breakdown the reader had no way to select, and
       naming a thing you cannot reach is worse than not naming it. The
       buttons offer only what this series actually publishes, so a
       rural-only series shows one button and says so rather than offering a
       choice that returns nothing. */
    var FACET_LABEL = { all: 'All', rural: 'Rural', urban: 'Urban',
                        male: 'Male', female: 'Female',
                        transgender: 'Transgender' };
    var lab = function (v) {
      return FACET_LABEL[v] || v.charAt(0).toUpperCase() + v.slice(1);
    };
    var group = function (name, key, avail, held) {
      if (avail.length < 2) {
        return avail.length === 1 && avail[0] !== 'all'
          ? '<span class="year-one">' + esc(name) + ': ' + esc(lab(avail[0]))
            + ' \u00b7 the only one published</span>'
          : '';
      }
      // One row per group: residence and sex on a single line ran past the
      // edge of a phone and squeezed the buttons on a desktop card.
      return '<div class="facet-grp" role="group" aria-label="' + esc(name) + '">'
        + '<span class="facet-lab">' + esc(name) + '</span>'
        + avail.map(function (v) {
            return '<button type="button" data-facet="' + esc(key) + '"'
              + ' data-val="' + esc(v) + '" aria-pressed="' + (v === held)
              + '">' + esc(lab(v)) + '</button>';
          }).join('') + '</div>';
    };
    var fh = group('Residence', 'locality', locs, state.locality)
           + group('Sex', 'sex', sexes, state.sex);
    $('facetCtl').innerHTML = fh;
    $('facetCtl').hidden = !fh;
    $('facetCtl').querySelectorAll('button[data-facet]').forEach(function (b) {
      b.onclick = function () {
        state[b.dataset.facet] = b.dataset.val;
        choose(state.row);
      };
    });

    $('legendNote').textContent = state.resetNote || '';

    /* What this percentage is a percentage OF, where the source's own
       denominator needs saying. The Mouza Census divides by one of three
       different things and only two of them are bounded by 100, so dirt
       streets in Keti Bunder read 176% and the page said nothing about why. */
    var bits = [];
    /* 482 entries carry a label the source never resolved to a definition. A
       badge on a search result does not reach someone who arrived through the
       hierarchy or a shared link, and this is the one place every route
       passes. */
    if (col('h_review', i) === 1) {
      bits.push('The source label for this series is ambiguous and has not '
                + 'been resolved. It is kept exactly as published rather '
                + 'than guessed at.');
    }
    var note = col('h_note', i);
    if (note) bits.push(note);
    var box = $('legendDef');
    if (box) {
      box.textContent = bits.join(' ');
      box.hidden = !bits.length;
    }
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
        '<div class="tot tot-warn"><span class="tot-k">Not totalled'
        + '</span><b>' + state.ambiguous + ' ' + esc(g.noun)
        + (state.ambiguous === 1 ? '' : 's') + ' left blank: the source prints '
        + 'two different figures</b></div>';
      renderHead();
      return;
    }
    /* A mean, a rate, an index or a distance cannot be totalled across
       places. Decided in the build from the kind of quantity, not from
       whether the label happens to contain the word "mean". */
    var rate = !state.norm && col('h_sum', state.row) !== 1;

    var figure, caption, rangeTitle = '';
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
      /* A rate or a proportion cannot be totalled, but a RANGE is not the
         summary a reader wants at the top of the map: two extremes say
         nothing about the middle, and the top bar is where the eye goes for
         "so what is it, roughly".

         This is the plain average of the places on the map, not a national
         rate, and the caption says so. It is unweighted on purpose: the
         numerator and denominator behind a stored rate are not in the
         payload, and weighting every rate by total population would be wrong
         for any whose denominator is not population - a literacy rate is out
         of those aged ten and over, an unemployment rate out of the labour
         force. Where the page computes a share itself it has both parts and
         does weight it, which is the branch above.

         The range is kept on hover, so nothing that was there is lost. */
      var vals = keep.map(function (u) { return u.v; })
        .filter(function (v) { return v != null && isFinite(v); })
        .sort(function (a, b) { return a - b; });
      var dp = dpFor(state.row);
      if (!vals.length) { figure = '\u2014'; caption = 'No values'; }
      else {
        var mean = vals.reduce(function (a, b) { return a + b; }, 0) / vals.length;
        figure = fmt(mean, dp);
        caption = 'Average ' + g.noun + ' \u00b7 unweighted';
        rangeTitle = 'Range across the ' + vals.length + ' ' + g.noun
          + 's shown: ' + fmt(vals[0], dp) + ' to '
          + fmt(vals[vals.length - 1], dp)
          + '. This average gives every ' + g.noun
          + ' equal weight, so it is not a national rate.';
      }
    } else {
      figure = big(keep.reduce(function (a, u) { return a + u.v; }, 0));
      caption = 'Total \u00b7 ' + where;
    }

    /* How many of the places on the map this figure covers is said once, in
       the line under the measure's name: the census does not reach Azad Jammu
       & Kashmir or Gilgit-Baltistan, so 136 of 156 is the normal state of a
       national total here, and a strip box repeating it on every map stopped
       being read. */
    renderHead();
    box.hidden = false;
    box.innerHTML =
      (figure === null ? ''
         : '<div class="tot"' + (rangeTitle ? ' title="' + esc(rangeTitle) + '"' : '')
           + '><span class="tot-k">' + esc(caption) + '</span>'
           + '<b>' + esc(figure) + '</b></div>')
      /* A rate combined across 2017 units is the units' population-weighted
         mean, not a published figure, and the strip says so. */
      + (state.weighted
         ? '<div class="tot tot-note"><span class="tot-k">Combined</span><b>'
           + state.weighted + ' ' + esc(g.noun) + (state.weighted === 1 ? '' : 's')
           + ' show a 2017 rate for units that merged or were redrawn, weighted by'
           + ' each unit\u2019s 2017 population</b></div>'
         : '')
      /* A hole in the map with nothing said about it reads as missing data.
         Where a footprint's units cannot all be weighted, the figure is not
         missing; it is not derivable. */
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
      map.fitBounds(bounds, fitPad());
    }
  }

  /* 280 of the 344 curated entries carry no structured year, so the strip
     showed a dash where a date belongs and a dash reads as a value that
     failed to load. Undated is a fact about the source and is said as one. */
  function yearLabel() {
    if (!state.year) return col('source', state.row) === 'census' ? '' : 'undated';
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
    // A place the source states two ways is blank rather than ranked by
    // whichever row arrived last; the rest of the map still ranks.
    var dp = dpFor(state.row);
    var g = GEO[state.level], prov = $('provFilter').value;
    /* A figure drawn across a footprint is ONE figure. Peshawar's 2017
       population belongs to the seven 2023 tehsils it was redrawn into
       together, and ranked shape by shape it filled six of the top ten
       with the same 4,267,198. So a footprint is ranked once, under the
       names of its shapes. */
    var fp = footprints(), srcNames = {};
    (state.units || []).forEach(function (u) { if (u.keys && u.keys.length) fp.join(u.keys); });
    // the census units the footprint was published as, to name it by
    (state.units || []).forEach(function (u) {
      var nm = u.u ? String(u.u).split('\u001f')[1] : '';
      if (!nm || !u.keys || !u.keys.length) return;
      nm.split(' + ').forEach(function (one) {
        (srcNames[fp.of(u.keys[0])] = srcNames[fp.of(u.keys[0])] || {})[title(one)] = 1;
      });
    });
    var members = {};
    Object.keys(state.values).forEach(function (k) {
      if (state.values[k] == null) return;
      (members[fp.of(k)] = members[fp.of(k)] || []).push(k);
    });
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
      var grp = members[fp.of(k)] || [k];
      if (grp.length > 1 && grp[0] !== k) return;
      var name = nameFor(k);
      if (grp.length > 1) {
        // Named by the unit(s) the census published, with the count of
        // today's shapes it covers: "Peshawar Tehsil (6 tehsils)".
        var src = Object.keys(srcNames[fp.of(k)] || {}).sort();
        var names = src.length ? src : grp.map(nameFor).sort();
        name = (names.length <= 2 ? names.join(' + ')
                : names[0] + ' + ' + (names.length - 1) + ' more')
             + ' (' + grp.length + ' ' + g.noun + 's)';
      }
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
    /* What the 2023 shape was in the census that published this figure. The
       warehouse records it on every row and it reached nobody: a district
       that merged, split or matched only approximately looked exactly like
       one that did not. */
    if (m && (m.boundary || m.boundaryNote)) {
      var BOUND = {
        merged: 'This shape covers more than one unit in the census that '
              + 'published the figure, so the figure shown is the whole '
              + 'shape\u2019s, not one unit\u2019s.',
        split: 'The unit that published this figure was later split, and the '
             + 'figure belongs to the whole of it rather than to this shape '
             + 'alone.',
        approximate: 'This shape is matched to the census unit approximately.',
      };
      var msg = BOUND[m.boundary] || (m.boundary
                ? 'Boundary relation: ' + m.boundary : '');
      if (m.boundaryNote) msg = (msg ? msg + ' ' : '') + m.boundaryNote;
      if (msg) h += '<p class="notice">' + esc(msg) + '</p>';
    }
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
     122,840 points is far too many for one marker each, so they are drawn to a
     canvas, and only the ones inside the current view. A school with a real fix
     is a filled dot; one placed at a settlement or tehsil centroid is a hollow
     ring, because a centroid is a claim about an area and not about a building.
     Below the zoom where individual schools mean anything, the layer says how
     many are in view rather than drawing a solid mass of ink. */
  /* The health facilities use the same machinery with their own data, ink and
     encoding: government filled, private a ring, hospitals drawn larger. So the
     layer takes its source and its drawing rules from a config. */
  var OVERLAY = {
    /* Schools pack f = located*100 + level*10 + sex (0 boys, 1 girls, 2 not
       stated); health f = government*10 + care group (0 is hospital). */
    schools: {
      label: 'Government schools', box: 'ovSchools',
      data: function () { return window.DD_SCHOOL_POINTS; },
      ink: '--pt-schools', note: 'ovSchoolsN', idle: '123k', cap: [12000, 60000],
      file: 'data/places/overlay_schools.js',
      filled: function (f) { return f >= 100; }, big: function () { return false; },
      filter: { a: 'all', b: 'all' },
      show: function (f, q) {
        var sx = f % 10, lv = Math.floor(f / 10) % 10;
        return (q.a === 'all' || (q.a === 'girls' ? sx === 1 : sx === 0))
            && (q.b === 'all' || lv === +q.b);
      },
      tally: function (f, t) {
        var sx = f % 10;
        if (sx === 1) t.girls++; else if (sx === 0) t.boys++;
        if (f >= 100) t.fixed++;
      },
      facts: function (t) {
        return ['<b>' + t.girls.toLocaleString() + '</b> girls\u2019',
                '<b>' + t.boys.toLocaleString() + '</b> boys\u2019',
                (t.n ? Math.round(100 * t.fixed / t.n) : 0) + '% on the school'];
      },
      seg: [['all', 'All'], ['girls', 'Girls\u2019'], ['boys', 'Boys\u2019']],
      pick: [['all', 'Every level'], ['0', 'Primary'], ['1', 'Middle'], ['2', 'High']],
      key: [['solid', 'located on the school'], ['hollow', 'placed at a centroid']],
      src: 'Provincial school registers, release 2026-09-30',
      table: 'datasets/schools-pk/',
    },
    health: {
      label: 'Health facilities', box: 'ovHealth',
      data: function () { return window.DD_HEALTH_POINTS; },
      // 20,536 points draw quickly, so the whole set is drawn at every zoom.
      ink: '--pt-health', note: 'ovHealthN', idle: '21k', cap: [30000, 30000],
      file: 'data/places/overlay_health.js',
      filled: function (f) { return f >= 10; }, big: function (f) { return f % 10 === 0; },
      filter: { a: 'all', b: 'all' },
      show: function (f, q) {
        return (q.a === 'all' || (q.a === 'gov' ? f >= 10 : f < 10))
            && (q.b === 'all' || f % 10 === +q.b);
      },
      tally: function (f, t) {
        if (f >= 10) t.gov++; else t.pvt++;
        if (f % 10 === 0) t.hosp++;
      },
      facts: function (t) {
        return ['<b>' + t.gov.toLocaleString() + '</b> government',
                '<b>' + t.pvt.toLocaleString() + '</b> private',
                '<b>' + t.hosp.toLocaleString() + '</b> hospitals'];
      },
      seg: [['all', 'All'], ['gov', 'Govt'], ['pvt', 'Private']],
      pick: [['all', 'Every type'], ['0', 'Hospitals'], ['1', 'Primary care'],
             ['2', 'Mother and child'], ['3', 'Clinics'], ['4', 'Traditional medicine'],
             ['5', 'Laboratories'], ['6', 'TB and leprosy']],
      key: [['solid', 'government'], ['hollow', 'private'], ['solid big', 'hospital']],
      src: 'ALHASAN Systems, 2017 (CC0, via HDX)',
      table: 'datasets/health-facilities-pk/',
    },
  };

  /* Decode once. The payload is delta-encoded so it ships small, but
     decoding 122,840 points twice on every redraw was most of the cost of a
     zoom. Each point is kept as Web Mercator world coordinates in [0, 1], so
     its screen position is two multiply-adds per redraw instead of a Leaflet
     projection call. */
  function decodePoints(pts) {
    if (pts._wx) return pts;
    var n = pts.n, la = 0, ln = 0, y, x, sn, R = Math.PI / 180;
    var lat = new Float32Array(n), lng = new Float32Array(n);
    var wx = new Float64Array(n), wy = new Float64Array(n);
    for (var i = 0; i < n; i++) {
      la += pts.lat[i]; ln += pts.lng[i];
      y = la / 1e4; x = ln / 1e4;
      lat[i] = y; lng[i] = x;
      wx[i] = (x + 180) / 360;
      sn = Math.sin(y * R);
      wy[i] = 0.5 - Math.log((1 + sn) / (1 - sn)) / (4 * Math.PI);
    }
    pts._la = lat; pts._ln = lng; pts._wx = wx; pts._wy = wy;
    return pts;
  }

  /* One small bitmap per mark style, drawn once per redraw and stamped for
     every point: a copy is far cheaper than a path, a stroke and a fill. */
  function sprite(ink, r, filled, dpr) {
    var w = Math.ceil(2 * r + 5), c = document.createElement('canvas');
    c.width = c.height = w * dpr;
    var g = c.getContext('2d');
    g.scale(dpr, dpr);
    g.beginPath();
    g.arc(w / 2, w / 2, r, 0, 6.283);
    // A white halo under every mark, so the points read against every one of
    // the choropleth ramps - dark green dots vanished into dark green districts.
    var halo = 'rgba(255,255,255,.95)';
    if (filled) {
      g.lineWidth = 1; g.strokeStyle = halo; g.stroke();
      g.fillStyle = ink; g.fill();
    } else {
      g.lineWidth = 2.6; g.strokeStyle = halo; g.stroke();
      g.lineWidth = 1.3; g.strokeStyle = ink; g.stroke();
    }
    return { img: c, w: w };
  }

  var SchoolLayer = L.Layer.extend({
    initialize: function (cfg) {
      this._cfg = cfg || OVERLAY.schools;
      this._key = Object.keys(OVERLAY).filter(function (k) { return OVERLAY[k] === this._cfg; }, this)[0];
    },
    onAdd: function (m) {
      this._map = m;
      this._c = L.DomUtil.create('canvas', 'leaflet-zoom-animated');
      this._c.style.pointerEvents = 'none';
      /* The points get a pane of their own above the districts. In the shared
         overlay pane every repaint re-added the district shapes on top, so a
         plain or shaded fill covered the dots entirely. */
      var pane = m.getPane('points');
      if (!pane) {
        pane = m.createPane('points');
        pane.style.zIndex = 450;            // above overlays (400), below popups
        pane.style.pointerEvents = 'none';
      }
      pane.appendChild(this._c);
      // moveend follows every zoom too, so listening to zoomend as well drew
      // each frame twice.
      m.on('moveend resize', this._draw, this);
      // Scale with the map during its zoom animation, as Leaflet's own layers
      // do, rather than sitting still and jumping when the zoom ends.
      if (m._zoomAnimated) m.on('zoomanim', this._animateZoom, this);
      this._draw();
    },
    onRemove: function (m) {
      m.off('moveend resize', this._draw, this);
      m.off('zoomanim', this._animateZoom, this);
      if (this._c && this._c.parentNode) this._c.parentNode.removeChild(this._c);
      this._c = null;
    },
    _animateZoom: function (e) {
      var m = this._map;
      var scale = m.getZoomScale(e.zoom);
      var offset = m._latLngBoundsToNewLayerBounds(m.getBounds(), e.zoom, e.center).min;
      L.DomUtil.setTransform(this._c, offset, scale);
    },
    _draw: function () {
      var cfg = this._cfg, pts = cfg.data();
      if (!pts || !this._c) return;
      decodePoints(pts);
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
        .getPropertyValue(cfg.ink).trim() || '#0c3a1e';
      var marks = {
        fs: sprite(ink, s, true, dpr), fb: sprite(ink, s + 1.6, true, dpr),
        rs: sprite(ink, s, false, dpr), rb: sprite(ink, s + 1.6, false, dpr),
      };
      // screen position = world * scale - top-left world pixel
      var W0 = m.options.crs.scale(z);
      var tlw = m.project(m.containerPointToLatLng([0, 0]), z);

      var n = pts.n, LA = pts._la, LN = pts._ln, WX = pts._wx, WY = pts._wy, F = pts.f;
      var S = b.getSouth(), N = b.getNorth(), Wb = b.getWest(), E = b.getEast();
      var cap = z < 8 ? cfg.cap[0] : cfg.cap[1];   // enough to read, not enough to blot

      // Two passes. The points are stored in latitude order, so drawing the
      // first `cap` of them would paint the southern third of the country and
      // leave the north bare - a picture of the storage order, not of the
      // schools. Count first, then take every stride-th, which thins the whole
      // country evenly.
      var inView = 0, i, y, x, q = cfg.filter;
      var t = { n: 0, girls: 0, boys: 0, fixed: 0, gov: 0, pvt: 0, hosp: 0 };
      for (i = 0; i < n; i++) {
        y = LA[i]; x = LN[i];
        if (y >= S && y <= N && x >= Wb && x <= E && cfg.show(F[i], q)) {
          inView++; cfg.tally(F[i], t);
        }
      }
      t.n = inView; cfg.inView = t;
      var stride = Math.max(1, Math.ceil(inView / cap));

      var drawn = 0, seen = 0, mk, f;
      for (i = 0; i < n; i++) {
        y = LA[i]; x = LN[i];
        if (y < S || y > N || x < Wb || x > E) continue;
        f = F[i];
        if (!cfg.show(f, q) || seen++ % stride) continue;
        mk = cfg.filled(f) ? (cfg.big(f) ? marks.fb : marks.fs)
                           : (cfg.big(f) ? marks.rb : marks.rs);
        g.drawImage(mk.img, WX[i] * W0 - tlw.x - mk.w / 2, WY[i] * W0 - tlw.y - mk.w / 2, mk.w, mk.w);
        drawn++;
      }
      updatePts(this._key);
      var note = document.getElementById(cfg.note);
      if (note) {
        // say plainly when the map is showing a sample rather than everything
        note.textContent = drawn < inView
          ? '1 in ' + stride + ' of ' + inView.toLocaleString()
          : inView.toLocaleString() + (inView >= n ? '' : ' in view');
      }
    },
  });

  var overlays = {};

  /* The card under the map describes what is drawn on it. With point layers
     on it says, for each, what is in view, lets the reader narrow it, and
     names the source; on a plain background it is only that. */
  function renderPts() {
    var box = $('ptsCard'), on = Object.keys(OVERLAY).filter(function (k) { return overlays[k]; });
    box.hidden = !on.length;
    $('legend').classList.toggle('has-pts', !!on.length);
    box.innerHTML = on.map(function (k) {
      var c = OVERLAY[k], t = c.inView, f = c.filter;
      var head = t ? t.n.toLocaleString() + ' in view' : 'loading\u2026';
      // Over the shading the card already carries the indicator's key, so each
      // layer folds to one line until opened; on a plain map it starts open.
      var open = c.open != null ? c.open : state.plain;
      return '<details class="pts-sec" data-l="' + k + '" style="--pt:var(' + c.ink + ')"'
        + (open ? ' open' : '') + '>'
        + '<summary class="pts-head"><i class="dot solid"></i><b>' + c.label + '</b>'
        + '<span class="pts-n">' + head + '</span></summary>'
        + (t ? '<div class="pts-facts">' + c.facts(t).join(' \u00b7 ') + '</div>' : '')
        + '<div class="pts-ctl"><div class="pts-seg" role="group" aria-label="' + c.label + ': which">'
        + c.seg.map(function (o) {
            return '<button type="button" data-a="' + o[0] + '" aria-pressed="' + (f.a === o[0]) + '">'
              + o[1] + '</button>'; }).join('')
        + '</div><select aria-label="' + c.label + ': type">'
        + c.pick.map(function (o) {
            return '<option value="' + o[0] + '"' + (f.b === o[0] ? ' selected' : '') + '>' + o[1] + '</option>';
          }).join('') + '</select></div>'
        + '<div class="pts-key">' + c.key.map(function (o) {
            return '<span><i class="dot ' + o[0] + '"></i>' + o[1] + '</span>'; }).join('')
        + '<a class="pts-src" href="' + c.table + '" title="' + c.src + '">source</a></div>'
        + '</details>';
    }).join('');
  }

  /* After a redraw only the numbers change; rebuilding the card's markup on
     every pan closed an open dropdown and cost a layout. */
  function updatePts(k) {
    var sec = document.querySelector('.pts-sec[data-l="' + k + '"]');
    var c = OVERLAY[k], t = c.inView;
    if (!sec || !t) return;
    var facts = sec.querySelector('.pts-facts');
    if (!facts) { renderPts(); return; }
    sec.querySelector('.pts-n').textContent = t.n.toLocaleString() + ' in view';
    facts.innerHTML = c.facts(t).join(' \u00b7 ');
  }

  function wirePts() {
    var box = $('ptsCard');
    var redraw = function (k) { if (overlays[k]) overlays[k]._draw(); renderPts(); };
    box.addEventListener('click', function (e) {
      var b = e.target.closest('.pts-seg button'); if (!b) return;
      var k = b.closest('.pts-sec').dataset.l;
      OVERLAY[k].filter.a = b.dataset.a; redraw(k);
    });
    box.addEventListener('toggle', function (e) {
      var d = e.target.closest && e.target.closest('.pts-sec');
      if (d) OVERLAY[d.dataset.l].open = d.open;
    }, true);
    box.addEventListener('change', function (e) {
      if (e.target.tagName !== 'SELECT') return;
      var k = e.target.closest('.pts-sec').dataset.l;
      OVERLAY[k].filter.b = e.target.value; redraw(k);
    });
  }

  /* OpenStreetMap under the plain map, for reading schools and facilities
     against roads and towns. Their tiles need their credit on the map, and
     street detail needs zooms the district map never offered, so both come
     and go with the layer. */
  var streetLayer = null, streetCredit = null;
  var MAX_ZOOM = 11, STREET_MAX_ZOOM = 17;
  function setStreet(on) {
    state.street = !!on;
    $('ovStreet').checked = state.street;
    // The street map is the one background without shading: on, the
    // districts become outlines over it; off, the shading comes back.
    setPlain(state.street);
    if (state.street) {
      if (!streetLayer) {
        streetLayer = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
          maxZoom: 19, crossOrigin: true,
          attribution: '\u00a9 <a href="https://www.openstreetmap.org/copyright" '
            + 'target="_blank" rel="noopener">OpenStreetMap</a> contributors',
        });
        streetCredit = L.control.attribution({ position: 'bottomright', prefix: false });
      }
      streetLayer.addTo(map);
      streetCredit.addTo(map);
      streetCredit.addAttribution(streetLayer.getAttribution());
      map.setMaxZoom(STREET_MAX_ZOOM);
    } else {
      if (streetLayer && map.hasLayer(streetLayer)) map.removeLayer(streetLayer);
      if (streetCredit) streetCredit.remove();
      if (map.getZoom() > MAX_ZOOM) map.setZoom(MAX_ZOOM);
      map.setMaxZoom(MAX_ZOOM);
    }
    $('placesMap').classList.toggle('is-street', state.street);
  }

  /* Shading off. No longer a choice of its own - a plain map with nothing
     under it showed nothing a reader could use - so only the street map
     sets it. */
  function setPlain(on) {
    state.plain = !!on;
    // a change of background resets each layer to that background's default
    Object.keys(OVERLAY).forEach(function (k) { OVERLAY[k].open = null; });
    $('legend').classList.toggle('is-plain', state.plain);
    renderPts();
  }

  function toggleOverlay(name, on) {
    var cfg = OVERLAY[name];
    var box = document.getElementById(cfg.box);
    if (!on) {
      if (overlays[name]) { map.removeLayer(overlays[name]); overlays[name] = null; }
      document.getElementById(cfg.note).textContent = cfg.idle;
      renderPts();
      return;
    }
    box.disabled = true;
    document.getElementById(cfg.note).textContent = 'loading\u2026';
    script(cfg.file).then(function () {
      overlays[name] = new SchoolLayer(cfg);
      map.addLayer(overlays[name]);
      renderPts();
    }).catch(function () {
      document.getElementById(cfg.note).textContent = 'could not load';
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
    var ov = ['schools', 'health'].filter(function (k) {
      return document.getElementById(k === 'schools' ? 'ovSchools' : 'ovHealth').checked;
    });
    if (ov.length) q.set('ov', ov.join(','));
    if (state.street) q.set('bg', 'street');
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
    var row = -1, via = null;
    for (var i = 0; i < N; i++) {
      if (col('level', i) === state.level && col('group_key', i) === g
          && col('indicator', i) === ind) { row = i; break; }
    }
    /* A link shared before cells were merged across the censuses and the
       sexes names one of the cells the merged row absorbed - "Population -
       2017 / Female" - and must still open it: the measure, and the year and
       sex that cell was. Matched on the cell alone, because a merged row
       keeps any one of its tables' group keys. */
    if (row < 0) {
      for (var j = 0; j < N && row < 0; j++) {
        if (col('level', j) !== state.level) continue;
        var parts = mergedParts(col('indicator', j)) || [];
        for (var k = 0; k < parts.length; k++) {
          if (parts[k].cell === ind) {
            row = j; via = [parts[k].year, parts[k].sex]; break;
          }
        }
      }
    }
    if (row < 0) return false;
    if (via) {
      state.year = via[0];
      if (via[1]) state.sex = via[1];
    }
    if (q.get('y')) state.year = q.get('y');
    if (q.get('loc')) state.locality = q.get('loc');
    state.norm = ['pct', 'per1000'].indexOf(q.get('n')) >= 0 ? q.get('n') : '';
    if (q.get('sex')) state.sex = q.get('sex');
    state.place = q.get('p') || null;
    /* Applied after buildProvinces has filled the dropdown - setting .value
       before the options exist selects nothing and the filter is silently
       lost on every shared link. */
    state.provWanted = q.get('prov') || '';
    // A link from when the plain background was its own option opens on the
    // street map, the nearest thing to it now.
    if (q.get('bg') === 'street' || q.get('bg') === 'plain') setStreet(true);
    (q.get('ov') || '').split(',').forEach(function (k) {
      if (!OVERLAY[k]) return;
      document.getElementById(k === 'schools' ? 'ovSchools' : 'ovHealth').checked = true;
      toggleOverlay(k, true);
    });
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

  /* Who owns a series. The dataset label names the survey or the product -
     "DHS 2017-18", "VIIRS DNB" - which is what a reader looks for but not
     who to credit, so the owner is said beside it wherever the source is
     stated: under the legend and in the CSV. */
  var OWNERS = [
    [/^DHS /, 'National Institute of Population Studies and ICF'],
    [/^MICS/, 'provincial and regional bureaus of statistics, with UNICEF'],
    [/^VIIRS/, 'Earth Observation Group, Colorado School of Mines'],
    [/^Meta Relative Wealth/, 'Chi et al. 2022'],
    [/^WorldPop/, 'University of Southampton'],
    [/^Malaria Atlas/, 'Weiss et al. 2020'],
    [/Alkire/, 'Data Darbar estimate from Pakistan Bureau of Statistics microdata'],
    [/^(Population Census|Census 2023|Economic Census|Mouza Census|PSLM|HIES|LFS)/,
     'Pakistan Bureau of Statistics'],
  ];
  function credited(ds) {
    for (var n = 0; n < OWNERS.length; n++) {
      if (OWNERS[n][0].test(ds)) return ds + ' (' + OWNERS[n][1] + ')';
    }
    return ds;
  }

  /* ── boot ───────────────────────────────────────────────────────────────- */
  function boot() {
    map = L.map('placesMap', {
      zoomControl: false, attributionControl: false,
      // Quarter steps, so a fit lands on the size that fits rather than
      // rounding down to half of it.
      zoomSnap: 0.25, zoomDelta: 0.5,
      scrollWheelZoom: true, minZoom: 4, maxZoom: MAX_ZOOM,
    }).setView([30.2, 69.4], 5);

    /* The picker as a sheet on a phone: opened by "Change indicator", closed
       by Done, by Escape, or by choosing something - focus goes back to the
       button that opened it. */
    var sheet = function (open) {
      $('picker').classList.toggle('open', open);
      $('mhChange').setAttribute('aria-expanded', String(open));
      // Focus the search on a computer; on a touch screen it raises the
      // keyboard over the list and iOS zooms the page in on the field.
      var touch = window.matchMedia && window.matchMedia('(pointer: coarse)').matches;
      if (open && !touch) { var f = $('indSearch'); if (f) f.focus(); }
      else if (document.activeElement && $('picker').contains(document.activeElement)) {
        $('mhChange').focus();
      }
    };
    $('mhChange').onclick = function () { sheet(true); };
    $('pickerClose').onclick = function () { sheet(false); };
    $('picker').addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && $('picker').classList.contains('open')) sheet(false);
    });
    state.closeSheet = function () {
      if ($('picker').classList.contains('open')) sheet(false);
    };

    $('geoDistrict').onclick = function () { setLevel('district'); };
    $('geoTehsil').onclick = function () { setLevel('tehsil'); };
    $('provFilter').onchange = function () {
      if (state.row != null) {
        paint().then(function () {
          renderTotals(); renderRanks(); writeUrl(); zoomProvince();
        });
      } else { writeUrl(); zoomProvince(); }
    };
    $('placeSearch').oninput = function () { findPlace(this.value); };
    $('csvBtn').onclick = downloadCsv;
    $('ovSchools').onchange = function () { toggleOverlay('schools', this.checked); writeUrl(); };
    $('ovHealth').onchange = function () { toggleOverlay('health', this.checked); writeUrl(); };
    wirePts();
    $('ovStreet').onchange = function () {
      setStreet(this.checked);
      if (state.row != null) paint().then(writeUrl); else writeUrl();
    };
    $('shareBtn').onclick = share;

    /* Bottom right, clear of the measure header and the tools over the map's
       top edge: in the top-left corner Leaflet put it on top of both. */
    L.control.zoom({ position: 'bottomright' }).addTo(map);
    rail = window.DDPlacesRail && window.DDPlacesRail.mount({
      el: $('rail'), libraryEl: $('library'), searchEl: $('indSearch'),
      IX: IX, N: N, col: col, list: list, level: state.level,
      onChange: function (row, norm) { state.norm = norm || ''; choose(row); },
      onLevel: function (lv) { setLevel(lv); },
    });
    if (!readUrl() && rail) rail.fire();
  }

  function setLevel(lv) {
    if (state.level === lv) return;
    var prevKey = state.row != null ? col('indicator', state.row) : null;
    var prevGroup = state.row != null ? col('group_key', state.row) : null;
    state.level = lv;
    state.row = null; state.values = null; state.place = null; nameIndex = null;
    $('geoDistrict').setAttribute('aria-pressed', String(lv === 'district'));
    $('geoTehsil').setAttribute('aria-pressed', String(lv === 'tehsil'));
    $('provFilter').removeAttribute('data-filled');
    $('provFilter').innerHTML = '<option value="">All provinces</option>';
    $('legend').hidden = true;
    if (layer) { map.removeLayer(layer); layer = null; }
    map.__fitted = false;
    renderDetail();
    // Last, and not before the teardown above: the rebuild draws the new
    // level's default map, and running it first meant the lines below then
    // hid the legend and removed the layer it had just made.
    if (rail) rail.rebuild(lv, prevKey, prevGroup);
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
                 'quality', 'provenance']];
    // Whose figure each row is, as the legend says it: the publisher's, or
    // one Data Darbar constructed (in the source data, or here as a change
    // or a rate).
    var derivedHere = (IX.derived && col('derived', state.row) === 1)
      || /^\u0394/.test(state.year || '') || !!state.norm;
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
                 credited(col('dataset', state.row) || src),
                 col('indicator', state.row), quality,
                 derivedHere ? 'derived by Data Darbar' : 'as published']);
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
