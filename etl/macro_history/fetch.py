"""Capture the original and open sources behind Pakistan's long-run macro series.

The Global Macro Database cannot be republished (its terms forbid it), but its
Pakistan series are compiled from sources that can. This captures them, dated
and hashed, so every figure the warehouse publishes traces to a file kept here:

  SBP   Handbook of Statistics on Pakistan Economy 2020, the eleven chapter
        workbooks (archive.sbp.org.pk). Pakistan's own long series: money since
        the 1950s, consolidated public finance, national income at every base.
        SBP terms: reuse with reference to the source, non-commercial, unchanged.
  IMF   DataMapper API: Public Finances in Modern History (FPP), the Global Debt
        Database (GDD), the Fiscal Monitor (FM) and the World Economic Outlook
        (WEO). IMF terms: reuse, including derived works, with attribution.
  WB    World Development Indicators API, Pakistan and comparators. CC BY 4.0.

    python3 etl/macro_history/fetch.py --out ../raw_data/macro_history/<date>
"""
import argparse, datetime, hashlib, json, pathlib, time, urllib.request

UA = {'User-Agent': 'Mozilla/5.0 (Data Darbar; data@adaad.org)'}
HANDBOOK = 'https://archive.sbp.org.pk/departments/stats/PakEconomy_HandBook/exl/Ch-{n}.xlsx'

IMF = {
    # FPP: Public Finances in Modern History (Mauro et al. 2015), Pakistan from 1950
    'rev': 'FPP', 'exp': 'FPP', 'prim_exp': 'FPP', 'ie': 'FPP', 'pb': 'FPP', 'd': 'FPP',
    # GDD: Global Debt Database (Mbaye et al. 2018)
    'CG_DEBT_GDP': 'GDD',
    # Fiscal Monitor: general government, 1990s on
    'GGR_G01_GDP_PT': 'FM', 'G_X_G01_GDP_PT': 'FM', 'GGXCNL_G01_GDP_PT': 'FM',
    'GGXONLB_G01_GDP_PT': 'FM', 'G_XWDG_G01_GDP_PT': 'FM',
    # WEO: unemployment, population, inflation, current account, from 1980
    'LUR': 'WEO', 'LP': 'WEO', 'PCPIPCH': 'WEO', 'BCA_NGDPD': 'WEO',
}

# Pakistan and the comparators a reader is likely to ask about: South Asia, and
# the large lower-middle-income Muslim-majority economies Pakistan is usually
# set beside. Aggregates for South Asia and lower-middle income as context.
WDI_COUNTRIES = ['PAK', 'IND', 'BGD', 'LKA', 'NPL', 'AFG', 'IRN', 'EGY', 'IDN',
                 'NGA', 'TUR', 'SAS', 'LMC', 'WLD']
WDI = [
    'SP.POP.TOTL', 'SP.POP.GROW', 'SP.URB.TOTL.IN.ZS',
    'NY.GDP.MKTP.KD.ZG', 'NY.GDP.PCAP.KD', 'NY.GDP.PCAP.PP.KD', 'NY.GDP.MKTP.CD',
    'FP.CPI.TOTL.ZG', 'SL.UEM.TOTL.ZS', 'SL.UEM.TOTL.NE.ZS', 'SL.TLF.CACT.FE.ZS',
    'NE.EXP.GNFS.ZS', 'NE.IMP.GNFS.ZS', 'NE.GDI.TOTL.ZS', 'NE.CON.PRVT.ZS',
    'BN.CAB.XOKA.GD.ZS', 'BX.TRF.PWKR.DT.GD.ZS', 'GC.TAX.TOTL.GD.ZS',
    'NV.AGR.TOTL.ZS', 'NV.IND.MANF.ZS', 'PX.REX.REER',
    'SE.ADT.LITR.ZS', 'SP.DYN.LE00.IN', 'SP.DYN.TFRT.IN', 'SH.DYN.MORT',
    'SI.POV.DDAY', 'EG.ELC.ACCS.ZS',
]


def get(url, tries=4):
    for k in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=90) as r:
                return r.read()
        except Exception as e:
            if k == tries - 1:
                raise
            time.sleep(3 * (k + 1))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    out = pathlib.Path(a.out)
    files = []

    def keep(rel, url, body, **meta):
        p = out / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(body)
        files.append({'path': rel, 'url': url, 'bytes': len(body),
                      'sha256': hashlib.sha256(body).hexdigest(),
                      'retrieved_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                      **meta})
        print(f'  {rel:<44} {len(body):>10,} bytes')

    print('SBP Handbook 2020…')
    for n in range(1, 12):
        url = HANDBOOK.format(n=n)
        keep(f'sbp_handbook_2020/Ch-{n}.xlsx', url, get(url), source='SBP', chapter=n)

    print('IMF DataMapper…')
    meta = json.loads(get('https://www.imf.org/external/datamapper/api/v1/indicators'))
    keep('imf/indicators.json', 'https://www.imf.org/external/datamapper/api/v1/indicators',
         json.dumps(meta).encode(), source='IMF')
    for code, ds in IMF.items():
        url = f'https://www.imf.org/external/datamapper/api/v1/{code}/PAK'
        keep(f'imf/{code}.json', url, get(url), source='IMF', dataset=ds, indicator=code)

    print('World Bank WDI…')
    cs = ';'.join(WDI_COUNTRIES)
    for code in WDI:
        url = (f'https://api.worldbank.org/v2/country/{cs}/indicator/{code}'
               f'?format=json&per_page=20000&date=1960:2026')
        keep(f'wdi/{code}.json', url, get(url), source='WB', indicator=code)
    for code in WDI:
        url = f'https://api.worldbank.org/v2/indicator/{code}?format=json'
        keep(f'wdi/meta/{code}.json', url, get(url), source='WB', indicator=code)

    (out / 'retrieval_manifest.json').write_text(json.dumps({
        'captured_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'purpose': 'Original and open sources for the long-run macro gaps the Global Macro '
                   'Database exposed; the GMD itself is not republishable.',
        'terms': {
            'SBP': 'Reuse with reference to the source, for non-commercial use, without '
                   'change (archive.sbp.org.pk disclaimer).',
            'IMF': 'Reuse including derived works with attribution to the IMF; material '
                   'transformation must be stated (imf.org copyright and usage).',
            'WB': 'Creative Commons Attribution 4.0 (World Development Indicators).'},
        'files': files}, indent=1))
    print(f'{len(files)} files captured')


if __name__ == '__main__':
    main()
