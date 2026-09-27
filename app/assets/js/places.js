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
    values: null,       // map_key -> number
    meta: null,         // map_key -> {relation, note}
    place: null,        // selected map key
    openTopic: null,
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

  function matches(i, q) {
    return (col('label', i) + ' ' + col('group_label', i) + ' ' +
            col('topic_label', i)).toLowerCase().indexOf(q) >= 0;
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

  function renderPicker() {
    var host = $('pickerList');
    var rows = rowsForLevel();
    var q = state.query.trim().toLowerCase();

    if (q) {
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
      return;
    }

    $('searchHint').textContent = rows.length.toLocaleString() + ' '
      + GEO[state.level].noun + ' indicators. Boundaries: PBS Digital Census 2023.';

    var order = [], seen = {};
    rows.forEach(function (i) {
      var t = col('topic', i);
      if (!seen[t]) { seen[t] = []; order.push(t); }
      seen[t].push(i);
    });

    host.innerHTML = order.map(function (t) {
      var items = seen[t], open = state.openTopic === t;
      var h = '<button class="topic" type="button" data-topic="' + esc(t) + '"'
            + ' aria-expanded="' + open + '"><span>' + esc(col('topic_label', items[0]))
            + '</span><span class="n">' + items.length.toLocaleString() + '</span></button>';
      if (open) {
        h += '<div class="topic-items">'
           + items.slice(0, 60).map(indHtml).join('')
           + (items.length > 60
              ? '<button class="more" type="button" data-all="' + esc(t) + '">'
                + (items.length - 60).toLocaleString() + ' more…</button>' : '')
           + '</div>';
      }
      return h;
    }).join('');
    bind(host);
  }

  function indHtml(i) {
    var yrs = list('years', i);
    var bits = [];
    if (yrs.length === 1) bits.push(yrs[0]);
    else if (yrs.length > 1) bits.push(yrs[0] + '–' + yrs[yrs.length - 1]);
    bits.push(col('shapes', i).toLocaleString() + ' shapes');
    return '<button class="ind" type="button" data-row="' + i + '"'
         + (state.row === i ? ' aria-current="true"' : '') + '>'
         + esc(col('label', i))
         + '<span class="meta">' + esc(col('group_label', i)) + ' · '
         + bits.join(' · ') + '</span></button>';
  }

  function bind(host) {
    host.querySelectorAll('.topic').forEach(function (b) {
      b.onclick = function () {
        state.openTopic = state.openTopic === b.dataset.topic ? null : b.dataset.topic;
        renderPicker();
      };
    });
    host.querySelectorAll('.ind').forEach(function (b) {
      b.onclick = function () { choose(Number(b.dataset.row)); };
    });
    host.querySelectorAll('.more').forEach(function (b) {
      b.onclick = function () {
        state.query = col('topic_label', rowsForLevel().find(function (i) {
          return col('topic', i) === b.dataset.all; }));
        $('indSearch').value = state.query;
        renderPicker();
      };
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

    if (src === 'place') {
      return loadGroup(gk).then(function (blob) {
        var d = blob.d, out = {}, meta = {};
        var get = function (c, r) { return c.v ? c.v[c.i[r]] : c[r]; };
        for (var r = 0; r < d.value.length; r++) {
          if (get(d.level, r) !== state.level) continue;
          if (get(d.indicator, r) !== ind) continue;
          if (state.year && get(d.year, r) !== state.year) continue;
          var keys = String(get(d.map_key, r)).split(' ');
          var note = get(d.note, r), rel = get(d.relation, r);
          for (var k = 0; k < keys.length; k++) {
            out[keys[k]] = d.value[r];
            if (note) meta[keys[k]] = { relation: rel, note: note };
          }
        }
        return { values: out, meta: meta };
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

    // census: table|indicator|col_label, with the year and facets chosen
    var c = ind.split('|');
    var year = state.year || list('years', i)[0];
    return engine().then(function (w) {
      return w.query(
        'SELECT map_key AS k, value AS v FROM census_panel_' + year
        + ' WHERE table_id = ' + q(c[0])
        + '   AND indicator = ' + q(c[1])
        + "   AND coalesce(col_label, '') = " + q(c[2])
        + '   AND locality = ' + q(state.locality)
        + '   AND sex = ' + q(state.sex)
        + (state.level === 'district' ? "   AND unit_type = 'district'"
                                      : "   AND unit_type <> 'district'")
        + '   AND map_key IS NOT NULL AND value IS NOT NULL');
    }).then(toMap);
  }

  function toMap(res) {
    var out = {};
    res.rows.forEach(function (r) {
      String(r.k).split(' ').forEach(function (k) { out[k] = r.v; });
    });
    return { values: out, meta: {} };
  }

  /* ── choosing ───────────────────────────────────────────────────────────
     The year is offered only where the indicator has more than one; for most
     census series it has exactly one, because the two censuses published
     different tables. */
  function choose(i) {
    state.row = i;
    var yrs = list('years', i);
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
    fetchValues(i).then(function (r) {
      state.values = r.values;
      state.meta = r.meta;
      return paint().then(function () {
        renderLegend();
        renderDetail(placeProps(state.place));
        writeUrl();
      });
    }).catch(function (e) {
      $('legendSub').textContent = 'Could not load this indicator: ' + e.message;
    });
  }

  /* ── the map ────────────────────────────────────────────────────────────
     A count of people or things is heavily skewed here, so counts get
     equal-count classes and rates the smooth ramp, the same rule the district
     map already uses. */
  var RAMP = ['#e6f4ec', '#145228'];

  function scaleFor(vals, isRate) {
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
      var isRate = col('dp', state.row) > 0;
      var sc = scaleFor(vals, isRate);

      if (layer) { map.removeLayer(layer); }
      layer = L.geoJSON(geo, {
        style: function (f) {
          var p = f.properties, v = state.values[g.key(p)];
          if (prov && g.prov(p) !== prov) {
            return { fillOpacity: 0, weight: 0, opacity: 0 };
          }
          if (v == null || isNaN(v) || !sc) {
            return { fillColor: '#e2e5ea', fillOpacity: .5, weight: .6,
                     color: '#b9c2b9', opacity: 1 };
          }
          return { fillColor: sc.colour(v), fillOpacity: .9, weight: .6,
                   color: '#ffffff', opacity: 1 };
        },
        onEachFeature: function (f, lyr) {
          lyr.on('click', function () {
            state.place = g.key(f.properties);
            renderDetail(f.properties);
            writeUrl();
          });
        },
      }).addTo(map);
      if (!map.__fitted) { map.fitBounds(layer.getBounds(), { padding: [12, 12] }); map.__fitted = true; }
      state.scale = sc;
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
  }

  /* ── legend, with the facet controls the design puts on the map ───────── */
  function renderLegend() {
    var i = state.row;
    if (i == null) return;
    $('legendTitle').textContent = col('label', i);
    var n = state.values ? Object.keys(state.values).length : 0;
    $('legendSub').textContent = col('group_label', i) + (n ? ' · ' + n.toLocaleString()
      + ' ' + GEO[state.level].noun + 's' : '');

    var sc = state.scale;
    var dp = col('dp', i);
    $('legendRamp').style.background = 'linear-gradient(90deg,' +
      chroma.scale(RAMP).colors(5).join(',') + ')';
    $('legendLo').textContent = sc ? fmt(sc.breaks[0], dp) : '';
    $('legendHi').textContent = sc ? fmt(sc.breaks[sc.breaks.length - 1], dp) : '';

    var yrs = list('years', i), locs = list('localities', i), sexes = list('sexes', i);
    var h = '';
    if (yrs.length > 1) {
      h += yrs.map(function (y) {
        var lab = /^\u0394/.test(y) ? 'Change' : y;
        return '<button type="button" data-year="' + esc(y) + '" aria-pressed="'
             + (y === state.year) + '">' + esc(lab) + '</button>';
      }).join('');
    } else if (yrs.length === 1) {
      h += '<button type="button" disabled aria-pressed="true">' + esc(yrs[0])
         + '</button><button type="button" disabled title="This indicator appears in only one census">'
         + 'only year</button>';
    }
    $('yearCtl').innerHTML = h;
    $('yearCtl').hidden = !h;
    $('yearCtl').querySelectorAll('button[data-year]').forEach(function (b) {
      b.onclick = function () { state.year = b.dataset.year; choose(state.row); };
    });

    var notes = [];
    if (locs.length > 1) notes.push(locs.join(' / '));
    if (sexes.length > 1) notes.push(sexes.join(' / '));
    $('legendNote').textContent = notes.length ? 'Also published by ' + notes.join('; ') : '';
  }

  /* ── detail ─────────────────────────────────────────────────────────────- */
  function renderDetail(props) {
    var host = $('detail');
    if (state.row == null) { return; }
    var g = GEO[state.level];
    if (!state.place) {
      host.innerHTML = '<p class="placeholder">Choose a ' + g.noun
        + ' on the map to see its figure, its rank and the places around it.</p>';
      return;
    }
    var key = state.place, v = state.values ? state.values[key] : null;
    var dp = col('dp', state.row);

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
    if (state.locality !== 'all') q.set('loc', state.locality);
    if (state.sex !== 'all') q.set('sex', state.sex);
    if (state.place) q.set('p', state.place);
    if (document.getElementById('ovSchools').checked) q.set('ov', 'schools');
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
    if (q.get('sex')) state.sex = q.get('sex');
    state.place = q.get('p') || null;
    state.openTopic = col('topic', row);
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
    $('provFilter').onchange = function () { if (state.row != null) paint(); };
    $('placeSearch').oninput = function () { findPlace(this.value); };
    $('csvBtn').onclick = downloadCsv;
    $('ovSchools').onchange = function () { toggleSchools(this.checked); writeUrl(); };
    $('shareBtn').onclick = share;

    renderPicker();
    readUrl();
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
      writeUrl();
    }
  }

  function downloadCsv() {
    if (state.row == null || !state.values) return;
    var g = GEO[state.level];
    var rows = [[g.noun, 'province', 'shape_key', col('label', state.row)]];
    var geo = geoCache[state.level];
    if (geo) geo.features.forEach(function (f) {
      var k = g.key(f.properties), v = state.values[k];
      if (v == null) return;
      rows.push([g.name(f.properties), g.prov(f.properties), k, v]);
    });
    var csv = rows.map(function (r) {
      return r.map(function (c) {
        var t = String(c == null ? '' : c);
        return /[",\n]/.test(t) ? '"' + t.replace(/"/g, '""') + '"' : t;
      }).join(',');
    }).join('\n');
    var a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
    a.download = 'data_darbar_' + col('indicator', state.row).replace(/[^A-Za-z0-9]+/g, '_')
      + '_' + state.level + '.csv';
    a.click();
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 1000);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
