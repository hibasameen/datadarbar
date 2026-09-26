"""Archive public LJCP sources, retaining original URL and content checksums."""
from pathlib import Path
import argparse, concurrent.futures, hashlib, json, urllib.request, datetime

WORKSPACE = Path(__file__).resolve().parents[3]
RAW = WORKSPACE / 'raw_data/ljcp'

def fetch_one(source, raw, archive_only=False):
    dest = raw / 'pdfs' / (source['id'] + '.pdf')
    if dest.exists():
        data=dest.read_bytes();metadata=dest.with_suffix('.metadata.json')
        digest=hashlib.sha256(data).hexdigest()
        saved=json.loads(metadata.read_text()) if metadata.exists() else {}
        if data.startswith(b'%PDF-') and digest==saved.get('sha256') and digest==source.get('sha256',digest):
            return {'id':source['id'], 'status':'cached', 'path':str(dest),'sha256':digest}
        return {'id':source['id'],'status':'failed','attempts':[{'error':'Cache checksum or metadata mismatch; inspect the retained file before replacing it.'}]}
    attempts = []
    urls = [source['archive_url']] if archive_only else list(dict.fromkeys([source['source_url'], source.get('archive_url')]))
    for url in filter(None, urls):
        try:
            req = urllib.request.Request(url, headers={'User-Agent':'DataDarbar/1.0 public judicial statistics research'})
            with urllib.request.urlopen(req, timeout=45) as response:
                data = response.read()
            if not data.startswith(b'%PDF-'):
                raise ValueError('Response was not a PDF')
            sha = hashlib.sha256(data).hexdigest()
            if source.get('sha256') and sha!=source['sha256']:
                raise ValueError('PDF differs from pinned edition checksum; review a new source version explicitly')
            obj = raw / 'objects' / (sha + '.pdf')
            obj.parent.mkdir(parents=True, exist_ok=True)
            if not obj.exists(): obj.write_bytes(data)
            dest.parent.mkdir(parents=True, exist_ok=True)
            partial=dest.with_suffix('.pdf.part');partial.write_bytes(data);partial.replace(dest)
            record = {**source, 'retrieval_url':url, 'retrieved_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
                      'sha256':sha, 'bytes':len(data), 'path':str(dest), 'object_path':str(obj), 'status':'downloaded', 'attempts':attempts}
            (raw / 'pdfs' / (source['id'] + '.metadata.json')).write_text(json.dumps(record, indent=2)+'\n')
            return record
        except Exception as exc:
            attempts.append({'url':url, 'error':str(exc)})
    return {'id':source['id'], 'status':'failed', 'attempts':attempts}

def main():
    p=argparse.ArgumentParser();p.add_argument('--catalog',type=Path,default=Path(__file__).with_name('sources.json'))
    p.add_argument('--raw',type=Path,default=RAW);p.add_argument('--archive-only',action='store_true');p.add_argument('--ids',nargs='*')
    p.add_argument('--all',action='store_true',help='Also attempt optional historical/alternate sources')
    a=p.parse_args();sources=json.loads(a.catalog.read_text())
    if a.ids:
        unknown=set(a.ids)-{s['id'] for s in sources}
        if unknown:p.error(f'Unknown source IDs: {sorted(unknown)}')
        sources=[s for s in sources if s['id'] in a.ids]
    elif not a.all:sources=[s for s in sources if s.get('required_for_build',True)]
    a.raw.mkdir(parents=True,exist_ok=True)
    failed=0
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futures=[pool.submit(fetch_one,s,a.raw,a.archive_only) for s in sources]
        for f in concurrent.futures.as_completed(futures):
            r=f.result();failed+=r['status']=='failed'
            with (a.raw/'manifest.jsonl').open('a') as stream: stream.write(json.dumps(r)+'\n')
            print(json.dumps({k:r[k] for k in ['id','status','bytes','attempts'] if k in r}),flush=True)
    raise SystemExit(bool(failed))

if __name__=='__main__':main()
