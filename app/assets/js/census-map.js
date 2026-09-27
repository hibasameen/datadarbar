/*
   census-map.js — browse the census panels on the district and tehsil map.
   ------------------------------------------------------------------------
   The rest of the map colours values that are already in the page: a group's
   numbers arrive as a JavaScript object and every indicator is hand-labelled.
   The census does not fit that shape. Two panels hold 33,795 series that can be
   drawn, which is a hundred times the map's entire hand-written vocabulary and
   far too much to ship as JavaScript.

   So the census is the one layer that reads its numbers on demand. The picker
   is driven by census_series_index (0.13 MB, every series the map can colour),
   and choosing a series runs one query against the 9 MB panel through
   DuckDB-WASM, which reads only the row groups that query touches. A district
   series comes back as ~128 rows.

   Both years are drawn on one frame, PBS's Digital Census 2023, and which shape
   a unit belongs on is decided at ETL time and recorded per unit — see
   build_unit_map_2023.py. What arrives here is map_key, which may name several
   shapes, and map_comparable, which says whether the figure needs combining
   first. A unit that nothing on the 2023 frame can honestly carry has map_key
   NULL and is not drawn; the count of what was drawn is reported so a thin
   layer reads as thin rather than as broken.
*/
window.DD_CENSUS = (function () {
  'use strict';

  var LAYERS = {
    census2017d: { year: 2017, unit: 'district', geo: 'district2023',
                   label: 'Census 2017 — districts',
                   source: 'PBS Population and Housing Census 2017' },
    census2023d: { year: 2023, unit: 'district', geo: 'district2023',
                   label: 'Census 2023 — districts',
                   source: 'PBS Population and Housing Census 2023' },
    census2023t: { year: 2023, unit: 'tehsil', geo: 'tehsil2023',
                   label: 'Census 2023 — tehsils',
                   source: 'PBS Population and Housing Census 2023' },
    census2017t: { year: 2017, unit: 'tehsil', geo: 'tehsil2023',
                   label: 'Census 2017 — tehsils',
                   source: 'PBS Population and Housing Census 2017' },
  };

  var wh = null, booting = null;
  var groupMeta = {};   // groupKey -> {layer, tableId, series: [...]}
  var rows = {};        // the values for the series currently on screen
  var lastNote = '';

  function q(s) { return "'" + String(s === null || s === undefined ? '' : s).replace(/'/g, "''") + "'"; }

  function loadScriptOnce(src) {
    return new Promise(function (res, rej) {
      if (document.querySelector('script[data-src="' + src + '"]')) return res();
      var el = document.createElement('script');
      el.src = src; el.dataset.src = src;
      el.onload = function () { res(); };
      el.onerror = function () { rej(new Error('could not load ' + src)); };
      document.head.appendChild(el);
    });
  }

  function engine() {
    if (booting) return booting;
    booting = loadScriptOnce('assets/js/warehouse.js')
      .then(function () {
        wh = window.DDWarehouse.create({ base: 'data/warehouse/' });
        return wh.init();
      })
      .then(function () { return wh; })
      .catch(function (e) { booting = null; throw e; });
    return booting;
  }

  /* Every census series lives under a group of its own table, so the map's
     existing topic -> group -> indicator chain carries year, table and series
     without a fourth control. */
  function groupKey(layerKey, tableId) {
    return layerKey + '_t' + String(tableId).replace(/[^0-9a-z]/gi, '');
  }

  function seriesLabel(r, tableHasOneIndicator) {
    var parts = [r.indicator];
    if (r.col_label && r.col_label !== r.indicator) parts.push(r.col_label);
    var bits = [];
    if (r.locality && r.locality !== 'all') bits.push(r.locality);
    if (r.sex && r.sex !== 'all') bits.push(r.sex);
    var label = parts.join(' · ');
    if (bits.length) label += ' (' + bits.join(', ') + ')';
    return label;
  }

  /* Fill TOPICS[topicKey].groups and INDICATOR_GROUPS for one layer. Runs once
     per layer; the index is small enough that this is a single query. */
  function prepareLayer(layerKey, TOPICS, GROUPS) {
    var L = LAYERS[layerKey];
    if (!L || (TOPICS[layerKey] && TOPICS[layerKey].groups.length)) return Promise.resolve();
    return engine().then(function (w) {
      return w.query(
        'SELECT table_id, any_value(table_title) AS title, ' +
        '       count(*) AS series, max(mappable_units) AS units ' +
        'FROM census_series_index ' +
        'WHERE census_year = ' + L.year + ' AND unit_type = ' + q(L.unit) + ' ' +
        'GROUP BY table_id ' +
        "ORDER BY CAST(regexp_extract(table_id, '^[0-9]+') AS INTEGER), table_id");
    }).then(function (res) {
      TOPICS[layerKey].groups = res.rows.map(function (r) {
        var key = groupKey(layerKey, r.table_id);
        groupMeta[key] = { layer: layerKey, tableId: r.table_id, series: null };
        GROUPS[key] = {
          label: 'Table ' + r.table_id + ' — ' + (r.title || ''),
          dataset: L.source,
          census: true, geo: L.geo, noYear: true, hasYears: false,
          yearLabel: String(L.year),
          blurb: 'Read on demand from the ' + L.year + ' census panel. ' +
                 Number(r.series).toLocaleString() + ' series in this table, ' +
                 'drawn on up to ' + Number(r.units).toLocaleString() + ' of PBS\'s ' +
                 'Digital Census 2023 ' + L.unit + ' shapes.' +
                 (L.year === 2017
                   ? ' Both censuses share the 2023 frame so the years can be compared. ' +
                     'Where a district absorbed another, the 2017 figure is the two combined; ' +
                     'where one was split, it is drawn across all its successors and is the ' +
                     'old unit\'s figure, not a share of it. Units that were redrawn ' +
                     'many-to-many are left undrawn. Each says which it is when clicked.'
                   : ''),
          indicators: {},
        };
        return key;
      });
    });
  }

  /* The series inside one table, as the indicator dropdown wants them. */
  function prepareGroup(groupKey_, GROUPS) {
    var meta = groupMeta[groupKey_];
    if (!meta || meta.series) return Promise.resolve();
    var L = LAYERS[meta.layer];
    return engine().then(function (w) {
      return w.query(
        'SELECT indicator, col_label, locality, sex, is_rate, mappable_units ' +
        'FROM census_series_index ' +
        'WHERE census_year = ' + L.year + ' AND unit_type = ' + q(L.unit) +
        '  AND table_id = ' + q(meta.tableId) + ' ' +
        'ORDER BY indicator, col_label, locality, sex');
    }).then(function (res) {
      meta.series = res.rows;
      var inds = {}, dp = {}, rate = {};
      res.rows.forEach(function (r, i) {
        var id = 's' + i;
        inds[id] = seriesLabel(r);
        if (r.is_rate) { dp[id] = 2; rate[id] = true; }
      });
      GROUPS[groupKey_].indicators = inds;
      GROUPS[groupKey_].dp = dp;
      // Which series are rates, for the map's colour scale: a count of people
      // by district or tehsil is heavily right-skewed here — Karachi East has
      // 150 times the people of the median tehsil — and a smooth ramp from the
      // smallest to the largest leaves almost every unit the palest colour. A
      // rate is not skewed that way and reads better on the smooth ramp.
      GROUPS[groupKey_].rateInds = rate;
    });
  }

  /* The values for one series: {shapeKey: {indicatorId: value, _name, _prov}}

     Two things stand between a panel row and a shape on the map, and both come
     from the unit map built at ETL time (see build_unit_map_2023.py).

     A unit that was split after 2017 has no shape of its own on the 2023 frame,
     but its successors together are exactly the ground it covered, so map_key
     names all of them and the figure is drawn across the group. One value, one
     colour, several shapes — nothing is divided between them.

     A unit that absorbed another shares one shape with it, so two rows arrive
     for one shape and have to be combined: added for a count, and averaged on
     2017 population for a rate, because adding two literacy rates is
     meaningless. This is the six districts that took in an FR.

     Everything else is one row, one shape, and passes through untouched. */
  function loadValues(groupKey_, indicatorId) {
    var meta = groupMeta[groupKey_];
    if (!meta || !meta.series) return Promise.resolve(rows);
    var s = meta.series[Number(String(indicatorId).replace(/^s/, ''))];
    if (!s) return Promise.resolve(rows);
    var L = LAYERS[meta.layer];
    return engine().then(function (w) {
      return w.query(
        'SELECT map_key, map_comparable, map_note, map_weight, value, unit, province_area ' +
        'FROM census_panel_' + L.year + ' ' +
        'WHERE table_id = ' + q(meta.tableId) +
        // The index collapses every tier below the district into one
        // geography; the panel keeps PBS's own word for each unit, so the
        // values query has to take all of them.
        (L.unit === 'district' ? "  AND unit_type = 'district'"
                               : "  AND unit_type <> 'district'") +
        '  AND indicator = ' + q(s.indicator) +
        "  AND coalesce(col_label, '') = " + q(s.col_label || '') +
        '  AND locality = ' + q(s.locality) +
        '  AND sex = ' + q(s.sex) +
        '  AND map_key IS NOT NULL AND value IS NOT NULL');
    }).then(function (res) {
      // Gather by map_key first, so the rows that share a shape meet before
      // anything is written to a shape.
      var byKey = {};
      res.rows.forEach(function (r) {
        (byKey[r.map_key] || (byKey[r.map_key] = [])).push(r);
      });

      var out = {}, shapes = 0, combined = 0, spread = 0;
      Object.keys(byKey).forEach(function (key) {
        var group = byKey[key], value, name, note = '';
        if (group.length === 1) {
          value = Number(group[0].value);
          name = group[0].unit;
          note = group[0].map_note || '';
        } else {
          // Several units on one shape. A rate is averaged on the weight the
          // unit map carries (2017 population); with no weight to go on, the
          // shape is left undrawn rather than given an unweighted average that
          // would read as if it were published.
          name = group.map(function (r) { return r.unit; }).sort().join(' with ');
          note = group[0].map_note || '';
          if (s.is_rate) {
            var wsum = 0, vsum = 0;
            group.forEach(function (r) {
              var wt = Number(r.map_weight);
              if (isFinite(wt) && wt > 0) { wsum += wt; vsum += Number(r.value) * wt; }
            });
            if (!wsum) return;
            value = vsum / wsum;
          } else {
            value = group.reduce(function (a, r) { return a + Number(r.value); }, 0);
          }
          combined += 1;
        }
        // One key can name several shapes: a unit drawn across its successors.
        var keys = String(key).split(' ').filter(Boolean);
        if (keys.length > 1) spread += 1;
        keys.forEach(function (k) {
          // _unit is the census unit this shape is showing, which is not the
          // shape: a split parent is drawn on each of its successors and is
          // still one unit. Anything that counts or totals has to count units,
          // or Chitral is two districts and the country gains 7.5 million
          // people.
          out[k] = { _name: name, _prov: group[0].province_area, _unit: key };
          if (note) out[k]._note = note;
          out[k][indicatorId] = value;
          shapes += 1;
        });
      });

      rows = out;
      var bits = [shapes + ' shape' + (shapes === 1 ? '' : 's') + ' drawn'];
      if (combined) bits.push(combined + ' where two units now share one');
      if (spread) bits.push(spread + ' drawn across the units that replaced them');
      bits.push(Math.round(res.ms) + ' ms');
      lastNote = bits.join(' · ');
      return rows;
    });
  }



  return {
    layers: LAYERS,
    isCensusTopic: function (t) { return Object.prototype.hasOwnProperty.call(LAYERS, t); },
    prepareLayer: prepareLayer,
    prepareGroup: prepareGroup,
    loadValues: loadValues,
    rows: function () { return rows; },
    note: function () { return lastNote; },
  };
})();
