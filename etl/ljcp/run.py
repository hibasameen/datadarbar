"""Rebuild the LJCP warehouse from retained source PDFs and reviewed OCR."""
import argparse
from pathlib import Path
import subprocess
import sys

HERE=Path(__file__).resolve().parent

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--fetch',action='store_true',help='Fetch/check the selected source editions before extracting')
    p.add_argument('--archive-only',action='store_true',help='Use archived originals when fetching')
    p.add_argument('--raw',type=Path)
    p.add_argument('--out',type=Path)
    p.add_argument('--population',type=Path)
    a=p.parse_args()
    def call(script,args):subprocess.run([sys.executable,str(HERE/script),*map(str,args)],check=True)
    raw=['--raw',a.raw] if a.raw else [];out=['--out',a.out] if a.out else []
    if a.fetch:call('fetch_sources.py',raw+(['--archive-only'] if a.archive_only else []))
    call('extract.py',raw+out)
    call('categories.py',raw+out)
    call('build.py',raw+out+(['--population',a.population] if a.population else []))

if __name__=='__main__':main()
