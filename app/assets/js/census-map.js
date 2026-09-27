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

   The map key is resolved at build time, not here — see build_web_warehouse.py.
   A unit with no boundary of its own, or one sharing a polygon with another
   unit, has map_key NULL and simply is not drawn; the count of those is
   reported so a thin layer reads as thin rather than as broken.
*/
window.DD_CENSUS = (function () {
  'use strict';

  var LAYERS = {
    census2017d: { year: 2017, unit: 'district', geo: 'district',
                   label: 'Census 2017 — districts',
                   source: 'PBS Population and Housing Census 2017' },
    census2023d: { year: 2023, unit: 'district', geo: 'district',
                   label: 'Census 2023 — districts',
                   source: 'PBS Population and Housing Census 2023' },
    census2023t: { year: 2023, unit: 'tehsil', geo: 'tehsil2023',
                   label: 'Census 2023 — tehsils',
                   source: 'PBS Population and Housing Census 2023' },
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
                 Number(r.series).toLocaleString() + ' series in this table; ' +
                 'up to ' + Number(r.units).toLocaleString() + ' ' + L.unit + 's carry a value ' +
                 'and a boundary. Units with no boundary of their own, and units that share ' +
                 'one with a neighbour, are not drawn.',
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
      var inds = {}, dp = {};
      res.rows.forEach(function (r, i) {
        var id = 's' + i;
        inds[id] = seriesLabel(r);
        if (r.is_rate) dp[id] = 2;
      });
      GROUPS[groupKey_].indicators = inds;
      GROUPS[groupKey_].dp = dp;
    });
  }

  /* The values for one series: {map_key: {indicatorId: value, _name, _prov}} */
  function loadValues(groupKey_, indicatorId) {
    var meta = groupMeta[groupKey_];
    if (!meta || !meta.series) return Promise.resolve(rows);
    var s = meta.series[Number(String(indicatorId).replace(/^s/, ''))];
    if (!s) return Promise.resolve(rows);
    var L = LAYERS[meta.layer];
    return engine().then(function (w) {
      return w.query(
        'SELECT map_key, value, unit, province_area FROM census_panel_' + L.year + ' ' +
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
      var out = {};
      res.rows.forEach(function (r) {
        out[r.map_key] = { _name: r.unit, _prov: r.province_area };
        out[r.map_key][indicatorId] = r.value;
      });
      rows = out;
      lastNote = res.rows.length + ' ' + L.unit + 's drawn · ' + Math.round(res.ms) + ' ms';
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
