/*
   map-scales.js — a colour scale per indicator group, as the live map has.

   The rebuilt Places drew every indicator on one green ramp. That is tidy and
   it is also uninformative: literacy, night lights, travel time to a clinic
   and women's decision-making all came out the same colour, so the map
   carried no signal about what kind of thing you were looking at, and moving
   between indicators felt like nothing had changed.

   These are the 38 scales darbar.adaad.org already uses, taken from it rather
   than reinvented, plus a fallback by topic so the groups that arrived since
   - census tables, crops, Mouza, entities - get a hue that belongs to their
   subject instead of falling back to green.

   Light end first: chroma interpolates between the stops, and a three-stop
   ramp bends through its middle colour rather than running straight.
*/
(function () {
  'use strict';

  var BY_GROUP = {
    demographics: ['#e6f4ec', '#145228'],
    urbanRural: ['#e6f4ec', '#1a5632'],
    literacy: ['#fef6dc', '#b8941a'],
    education: ['#fef6dc', '#d4a017'],
    pslmEducation: ['#fef6dc', '#8d6e0f'],
    censusSchooling: ['#fef6dc', '#d4a017'],
    employment: ['#e6f4ec', '#1e6b3e'],
    pslmEmployment: ['#e6f4ec', '#22804a'],
    lfs: ['#e6f4ec', '#0c3a1e'],
    lfs25: ['#e6f4ec', '#1e6b3e'],
    econCensus: ['#fef6dc', '#1a5632'],
    pslmFies: ['#fef6dc', '#8a6d0f'],
    hies: ['#fef6dc', '#b8941a'],
    mpi: ['#fdf3e3', '#a8471c', '#6b1503'],
    rwi: ['#f4efe2', '#1a5632'],
    satPop: ['#eef2f7', '#3d6f9e', '#10243d'],
    nightlights: ['#fffbe6', '#d4a017', '#4a2c00'],
    schoolAccess: ['#fef6dc', '#b8941a', '#5a3b06'],
    healthAccess: ['#fdf3e3', '#c2410c', '#5c1a06'],
    healthAccessDistrict: ['#fdf3e3', '#c2410c', '#5c1a06'],
    hiesHousing: ['#fbe9e7', '#bf360c'],
    pslmWash: ['#e0f2f1', '#004d40'],
    micsWash: ['#e0f2f1', '#004d40'],
    hiesWaste: ['#e0f2f1', '#00695c'],
    hiesIct: ['#e8eaf6', '#1a237e'],
    pslmDigital: ['#e3f2fd', '#0d47a1'],
    pslmHealth: ['#e8f5e9', '#1b5e20'],
    micsMaternal: ['#e8f5e9', '#1b5e20'],
    micsChildHealth: ['#e8f5e9', '#2e7d32'],
    micsNutrition: ['#fff3e0', '#e65100'],
    dhsImmunisation: ['#e8f5e9', '#2e7d32'],
    dhsMaternal: ['#e8f5e9', '#1b5e20'],
    dhsNutrition: ['#fff3e0', '#e65100'],
    dhsFamilyPlanning: ['#ede7f6', '#4527a0'],
    dhsFertility: ['#ede7f6', '#5e35b1'],
    // Women's status, decision-making, equity and protection share one hue on
    // the live map, which is how a reader learns to recognise them.
    hiesDecisions: ['#fce4ec', '#880e4f'],
    micsWomen: ['#fce4ec', '#880e4f'],
    micsProtection: ['#fce4ec', '#880e4f'],
    micsEquity: ['#fce4ec', '#ad1457'],
  };

  /* Everything that arrived after the live map: census tables, crops, Mouza
     groups, enumerated structures. A hue per subject, so the map still says
     what kind of thing it is showing. */
  var BY_TOPIC = {
    demographics: ['#e6f4ec', '#145228'],
    education: ['#fef6dc', '#d4a017'],
    employment: ['#e6f4ec', '#1e6b3e'],
    economic: ['#fef6dc', '#8a6d0f'],
    welfare: ['#fef6dc', '#b8941a'],
    poverty: ['#fdf3e3', '#a8471c', '#6b1503'],
    housing: ['#fbe9e7', '#bf360c'],
    infrastructure: ['#e0f2f1', '#004d40'],
    health: ['#e8f5e9', '#1b5e20'],
    agriculture: ['#f1f4e3', '#5c6b3a'],
    migration: ['#ede7f6', '#4527a0'],
    access: ['#fdf3e3', '#c2410c', '#5c1a06'],
    facilities: ['#eef2f7', '#2f5d7c'],
    satellite: ['#eef2f7', '#3d6f9e', '#10243d'],
    women: ['#fce4ec', '#880e4f'],
  };

  var FALLBACK = ['#e6f4ec', '#145228'];

  window.DDMapScales = {
    /* Group first, because a group can sit in a topic whose general hue would
       lose what makes it particular - night lights are satellite, but they
       are their own thing and read as one. */
    for: function (groupKey, topic) {
      return BY_GROUP[groupKey] || BY_TOPIC[topic] || FALLBACK;
    },
    byGroup: BY_GROUP,
    byTopic: BY_TOPIC,
  };
})();
