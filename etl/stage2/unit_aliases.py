"""Reviewed corrections to published unit labels.

Each entry maps a label as PBS prints it to the label used across the rest of
the corpus. The source label is always retained on the observation; nothing is
silently rewritten.

The first group is a single systematic defect. In Table 1 — and only Table 1 —
the literal string "ALL" has been deleted from six place names. The deletion is
exact in every case (ALLAI -> AI, KALLAR -> KAR, TANDO ALLAHYAR -> TANDO AHYAR,
KALLAG -> KAG), which is what a find-and-replace stripping the word "ALL" from
locality stubs would do to any place name containing those three letters. The
correct spellings are confirmed by the other nine Option A tables, which all
agree with each other. Stage 1 recorded the Tando Allahyar case individually;
this generalises it.

The second group is a genuine naming difference rather than a defect, and is
resolved to the form the majority of tables use.
"""

# table -> {published label: corrected label}
ALIASES = {
    '1': {
        'AI TEHSIL': 'ALLAI TEHSIL',
        'KAR KAHAR TEHSIL': 'KALLAR KAHAR TEHSIL',
        'KAR SAYADDAN TEHSIL': 'KALLAR SAYADDAN TEHSIL',
        'TANDO AHYAR DISTRICT': 'TANDO ALLAHYAR DISTRICT',
        'TANDO AHYAR TALUKA': 'TANDO ALLAHYAR TALUKA',
        'KAG SUB-TEHSIL': 'KALLAG SUB-TEHSIL',
    },
    # Table 1 calls this MALAKAND DISTRICT; tables 5-23 call it MALAKAND
    # PROTECTED AREA. It is one unit either way, and it sits in the census
    # frame as a district, so Table 1's label is the canonical one.
    '*': {
        'MALAKAND PROTECTED AREA': 'MALAKAND DISTRICT',
    },
}

REASON = {
    'AI TEHSIL': 'ALL deleted from name (Table 1 defect)',
    'KAR KAHAR TEHSIL': 'ALL deleted from name (Table 1 defect)',
    'KAR SAYADDAN TEHSIL': 'ALL deleted from name (Table 1 defect)',
    'TANDO AHYAR DISTRICT': 'ALL deleted from name (Table 1 defect)',
    'TANDO AHYAR TALUKA': 'ALL deleted from name (Table 1 defect)',
    'KAG SUB-TEHSIL': 'ALL deleted from name (Table 1 defect)',
    'MALAKAND PROTECTED AREA': 'named MALAKAND DISTRICT in table 1 (canonical)',
}


def apply(table, label):
    """(corrected label, reason or None)."""
    hit = ALIASES.get(table, {}).get(label.upper()) or ALIASES['*'].get(label.upper())
    return (hit, REASON.get(label.upper())) if hit else (label, None)
