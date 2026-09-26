"""Reviewed sub-district -> polygon decisions that no automatic rule can make.

Karachi is the bulk of it. The 553-polygon ADM3 layer carries Karachi's old
*town* system plus six cantonments; Census 2023 publishes 31 *sub-divisions*
across seven districts. The two frames do not correspond one to one, so each
decision below is recorded individually with the reason.

COD-AB, despite being reviewed in 2024, holds the same old town system — 18
towns in six districts — so it does not resolve Karachi either.

Fifteen sub-divisions share a name with a town and are matched on that basis.
Nine do not, and are withheld: they were carved out of the old towns, and
placing them would mean asserting a boundary neither source gives. Withheld
units keep their data and their district; they are absent only from the map.
"""

# Karachi in COD-AB is also the old town system, in six districts rather than
# the census's seven — Keamari is not a district there, and Baldia and Site sit
# under West Karachi rather than Keamari. So the COD match has to be made by
# name across the whole city, not within the census district.
#
# census unit -> (geoBoundaries shapeName, COD-AB adm3_name, note)
KARACHI = {
    'BALDIA SUB-DIVISION':          ('BALDIA TOWN', 'Baldia Town', 'same name'),
    'GULBERG SUB-DIVISION':         ('GULBERG TOWN', 'Gulberg Town', 'same name'),
    'GULSHAN-E-IQBAL SUB-DIVISION': ('GULSHAN E IQBAL TOWN', 'Gulshan Iqbal Town', 'same name, punctuation differs'),
    'JAMSHED QUARTERS SUB-DIVISION': ('JAMSHED TOWN', 'Jamshaid Town', 'Jamshed Quarters is the sub-division covering Jamshed Town'),
    'KEAMARI SUB-DIVISION':         ('KIAMARI TOWN', 'Kemari Town', 'same name, spelling differs'),
    'KORANGI SUB-DIVISION':         ('KORANGI TOWN', 'Korangi Town', 'same name'),
    'LANDHI SUB-DIVISION':          ('LANDHI TOWN', 'Landhi Town', 'same name'),
    'LIAQUATABAD SUB-DIVISION':     ('LIAQATABAD TOWN', 'Liaqatabad Town', 'same name, spelling differs'),
    'LYARI SUB-DIVISION':           ('LYARI TOWN', 'Liyari Town', 'same name, spelling differs'),
    'NORTH NAZIMABAD SUB-DIVISION': ('N.NAZIMABAD TOWN', 'North Nazimabad Town', 'same name, abbreviated in geoBoundaries'),
    'NEW KARACHI SUB-DIVISION':     ('NEW KARACHI TOWN', 'New Karachi Town', 'same name'),
    'ORANGI SUB-DIVISION':          ('ORANGI TOWN', 'Orangi Town', 'same name'),
    'SADDAR SUB-DIVISION':          ('SADDAR TOWN', 'Saddar Town', 'same name'),
    'SHAH FAISAL SUB-DIVISION':     ('SHAH FAISAL TOWN', 'Shah Faisal Town', 'same name'),
    'SITE SUB-DIVISION':            ('SITE TOWN', 'Site Town', 'same name'),
}

# census unit -> reason for withholding
WITHHELD = {
    'AIRPORT SUB-DIVISION':        'Malir; no corresponding town polygon. Candidates are Malir, Malir Cantonment and Faisal Cantonment; the split is not documented in the census tables.',
    'ARAM BAGH SUB-DIVISION':      'Karachi South; carved out of the area the polygon layer holds as Saddar Town. Boundary not given by any source to hand.',
    'CIVIL LINES SUB-DIVISION':    'Karachi South; carved out of Saddar Town/Clifton Cantonment. Boundary not given.',
    'GARDEN SUB-DIVISION':         'Karachi South; carved out of Saddar Town. Boundary not given.',
    'FEROZABAD SUB-DIVISION':      'Karachi East; carved out of Jamshed Town. Boundary not given.',
    'GULZAR-E-HIJRI SUB-DIVISION': 'Karachi East; carved out of Gulshan-e-Iqbal Town. Boundary not given.',
    'MODEL COLONY SUB-DIVISION':   'Korangi; no corresponding town polygon. Boundary not given.',
    'MOMINABAD SUB-DIVISION':      'Karachi West; carved out of Orangi Town. Boundary not given.',
    'NAZIMABAD SUB-DIVISION':      'Karachi Central; both boundary sources hold only North Nazimabad Town, which is matched to North Nazimabad. Boundary not given.',
}

MANUAL = dict(KARACHI)
