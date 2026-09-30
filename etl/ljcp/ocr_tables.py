"""Prepare column crops and reconstruct scanned 2021 table cells.

Requires OpenCV, NumPy, Pillow and pdftoppm. Local OCR is supplied by ocr.swift
on macOS. The digit recognition evidence is saved; OCR is never a silent
fallback for unreviewed report editions.
"""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import re

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[2]
PAGES=[65,66,67,110,111,112,149,150,151,152,184,185,213]
SELECTED={(65,0),(65,1),(66,0),(66,1),(67,0),(110,0),(110,1),(111,0),(112,0),(149,0),(149,1),(150,0),(151,1),(152,0),(184,1),(184,2),(185,0),(185,1),(213,0),(213,1),(213,2)}

def runs(values):
    out=[]
    for v in values:
        if out and v==out[-1][-1]+1:out[-1].append(int(v))
        else:out.append([int(v)])
    return [int(sum(r)/len(r)) for r in out]

def prepare(raw):
    import cv2
    import numpy as np
    from PIL import Image
    dest=raw/'ocr/annual_2021';dest.mkdir(parents=True,exist_ok=True)
    renderer=shutil.which('pdftoppm')
    if not renderer:raise RuntimeError('pdftoppm must be on PATH')
    manifest=[]
    for n in PAGES:
        path=dest/f'page_{n}.png'
        if not path.exists():
            subprocess.run([renderer,'-f',str(n),'-l',str(n),'-singlefile','-r','220','-png',str(raw/'pdfs/annual_2021.pdf'),str(path.with_suffix(''))],check=True)
        a=cv2.imread(str(path),0)
        mask=cv2.threshold(a,180,255,cv2.THRESH_BINARY_INV)[1]
        h=cv2.morphologyEx(mask,cv2.MORPH_OPEN,cv2.getStructuringElement(cv2.MORPH_RECT,(100,1)))
        v=cv2.morphologyEx(mask,cv2.MORPH_OPEN,cv2.getStructuringElement(cv2.MORPH_RECT,(1,50)))
        contours,_=cv2.findContours(h|v,cv2.RETR_EXTERNAL,cv2.CHAIN_APPROX_SIMPLE)
        boxes=sorted([cv2.boundingRect(c) for c in contours if cv2.boundingRect(c)[2]>.5*a.shape[1] and cv2.boundingRect(c)[3]>80],key=lambda b:b[1])
        for ti,(x,y,w,ht) in enumerate(boxes):
            xs=runs(np.where((v[y:y+ht,x:x+w]>0).sum(axis=0)>.45*ht)[0])+[]
            ys=runs(np.where((h[y:y+ht,x:x+w]>0).sum(axis=1)>.8*w)[0])+[]
            xs=[x+z for z in xs];ys=[y+z for z in ys]
            crops=[]
            for ci,(left,right) in enumerate(zip(xs,xs[1:])):
                name=f'crop_{n}_{ti}_{ci}.png';Image.fromarray(a[y+3:y+ht-3,left+3:right-2]).save(dest/name)
                crops.append(name)
            manifest.append(dict(page=n,table=ti,bbox=[x,y,w,ht],x_boundaries=xs,y_boundaries=ys,crops=crops))
    (dest/'crop_manifest.json').write_text(json.dumps(manifest,indent=2))
    print(f'Prepared {sum(len(t["crops"]) for t in manifest)} columns in {len(manifest)} tables')

def reconstruct(raw):
    dest=raw/'ocr/annual_2021';tables=json.loads((dest/'crop_manifest.json').read_text())
    for t in tables:
        y=t['bbox'][1];ht=t['bbox'][3]-6
        rows=[['']*len(t['crops']) for _ in range(len(t['y_boundaries'])-1)]
        confidence=[[None]*len(t['crops']) for _ in rows]
        for ci,name in enumerate(t['crops']):
            observations=json.loads((dest/name).with_suffix('.json').read_text())
            for o in observations:
                center=y+3+(1-o['y']-o['h']/2)*ht
                for ri,(top,bottom) in enumerate(zip(t['y_boundaries'],t['y_boundaries'][1:])):
                    if top<=center<bottom:
                        rows[ri][ci]+=(' ' if rows[ri][ci] else '')+o['text']
                        prev=confidence[ri][ci];confidence[ri][ci]=min(prev,o['confidence']) if prev is not None else o['confidence']
                        break
        t['rows']=rows;t['confidence']=confidence
    (dest/'tables.json').write_text(json.dumps(tables,indent=2))
    print(f'Reconstructed {len(tables)} tables; review expected row counts and totals before release')

def prepare_cells(raw):
    from PIL import Image,ImageOps
    dest=raw/'ocr/annual_2021';tables=json.loads((dest/'tables.json').read_text());manifest=[]
    for t in tables:
        if (t['page'],t['table']) not in SELECTED:continue
        image=Image.open(dest/f'page_{t["page"]}.png').convert('RGB')
        for ri,row in enumerate(t['rows']):
            if ri==0 or not any(row):continue
            for ci,value in enumerate(row):
                if not ((ci==1 and not value) or (ci>=2 and not re.fullmatch(r'\d+',value))):continue
                left,right=t['x_boundaries'][ci:ci+2];top,bottom=t['y_boundaries'][ri:ri+2]
                crop=ImageOps.expand(image.crop((left+4,top+4,right-3,bottom-3)),border=15,fill='white')
                crop=crop.resize((crop.width*3,crop.height*3));name=f'crop_cell_{t["page"]}_{t["table"]}_{ri}_{ci}.png';crop.save(dest/name)
                manifest.append(dict(page=t['page'],table=t['table'],row=ri,col=ci,file=name,previous=value))
    (dest/'cell_manifest.json').write_text(json.dumps(manifest,indent=2));print(f'Prepared {len(manifest)} fallback cells')

def reviewed_tables(raw,from_cache=False):
    # Ship the reviewed transcription with the extractor: routine rebuilds need
    # neither macOS nor OCR dependencies. Its PDF checksum is enforced upstream.
    if not from_cache:
        snapshot=json.loads((HERE/'ocr_2021_reviewed_tables.json').read_text())
        return {(t['page'],t['table']):t for t in snapshot['tables']}
    dest=raw/'ocr/annual_2021';tables=json.loads((dest/'tables.json').read_text());lookup={(t['page'],t['table']):t for t in tables}
    for m in json.loads((dest/'cell_manifest.json').read_text()):
        evidence=json.loads((dest/m['file']).with_suffix('.json').read_text())
        value=' '.join(x['text'] for x in sorted(evidence,key=lambda x:-x['y']))
        lookup[m['page'],m['table']]['rows'][m['row']][m['col']]=value
    for m in json.loads((HERE/'ocr_reviewed_cells.json').read_text()):
        lookup[m['page'],m['table']]['rows'][m['row']][m['col']]=m['value']
    return lookup

if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('action',choices=['prepare','reconstruct','prepare-cells']);ap.add_argument('--raw',type=Path,default=ROOT/'raw_data/ljcp');a=ap.parse_args()
    {'prepare':prepare,'reconstruct':reconstruct,'prepare-cells':prepare_cells}[a.action](a.raw)
