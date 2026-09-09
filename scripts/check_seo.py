#!/usr/bin/env python3
"""Validate the rendered search surface, including dataset downloads."""
import json
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[1]
if (ROOT / 'astro.config.mjs').exists():
    OUTPUT, ORIGIN = ROOT / 'dist', 'https://adaad.org'
elif (ROOT / 'app').is_dir():
    OUTPUT, ORIGIN = ROOT / 'app', 'https://darbar.adaad.org'
else:
    OUTPUT, ORIGIN = ROOT, 'https://aiwan.adaad.org'

class Page(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.title, self.description, self.canonical, self.robots = [], [], [], []
        self.links, self.graphs, self.h1 = [], [], 0
        self.in_title, self.in_ld, self.buffer = False, False, ''
        self.feed(text)
    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == 'title': self.in_title = True
        if tag == 'h1': self.h1 += 1
        if tag == 'meta':
            if a.get('name') == 'description': self.description.append(a.get('content', ''))
            if a.get('name') == 'robots': self.robots.append(a.get('content', ''))
        if tag == 'link' and a.get('rel') == 'canonical': self.canonical.append(a.get('href', ''))
        if tag in ('a', 'link') and a.get('href'): self.links.append(a['href'])
        if tag in ('img', 'script') and a.get('src'): self.links.append(a['src'])
        if tag == 'script' and a.get('type') == 'application/ld+json': self.in_ld, self.buffer = True, ''
    def handle_endtag(self, tag):
        if tag == 'title': self.in_title = False
        if tag == 'script' and self.in_ld:
            self.graphs.append(json.loads(self.buffer))
            self.in_ld = False
    def handle_data(self, data):
        if self.in_title: self.title.append(data)
        if self.in_ld: self.buffer += data

def local_file(url):
    path = unquote(urlsplit(url).path).lstrip('/')
    target = OUTPUT / path
    if target.is_dir() or not path: return target / 'index.html'
    return target

def sitemap_urls(file):
    tree = ElementTree.parse(file).getroot()
    urls = [x.text for x in tree.iter() if x.tag.endswith('}loc')]
    if tree.tag.endswith('}sitemapindex'):
        return [url for index in urls for url in sitemap_urls(local_file(index))]
    return urls

def objects(value):
    if isinstance(value, dict):
        yield value
        for item in value.values(): yield from objects(item)
    elif isinstance(value, list):
        for item in value: yield from objects(item)

def check():
    sitemap = OUTPUT / ('sitemap-index.xml' if (OUTPUT / 'sitemap-index.xml').exists() else 'sitemap.xml')
    urls = sitemap_urls(sitemap)
    assert urls and len(urls) == len(set(urls)), 'Empty or duplicate sitemap'
    assert ORIGIN + '/' in urls, 'Homepage omitted from sitemap'
    downloads = 0
    for url in urls:
        assert url.startswith(ORIGIN + '/'), f'Wrong sitemap host: {url}'
        file = local_file(url)
        assert file.is_file(), f'Missing sitemap page: {url}'
        p = Page(file.read_text())
        assert len(p.title) == 1 and p.title[0].strip(), f'Missing/duplicate title: {url}'
        assert len(p.description) == 1 and p.description[0].strip(), f'Missing/duplicate description: {url}'
        assert p.canonical == [url], f'Canonical mismatch: {url}: {p.canonical}'
        assert not any('noindex' in r for r in p.robots), f'Production noindex: {url}'
        assert p.graphs, f'Missing structured data: {url}'
        is_research = any(part in urlsplit(url).path for part in ['/datasets/', '/districts/', '/elections/'])
        if is_research:
            assert p.h1 == 1, f'Research page needs one heading: {url}'
            for link in p.links:
                target = urljoin(url, link)
                if urlsplit(target).netloc == urlsplit(ORIGIN).netloc:
                    assert local_file(target).is_file(), f'Broken research-page link: {url} -> {target}'
        for obj in objects(p.graphs):
            if obj.get('@type') == 'DataDownload':
                target = obj['contentUrl']
                if urlsplit(target).netloc == urlsplit(ORIGIN).netloc:
                    assert local_file(target).is_file(), f'Broken schema download: {target}'
                    assert local_file(target).stat().st_size > 0, f'Empty download: {target}'
                    downloads += 1
    assert 'Sitemap: ' + ORIGIN + '/' in (OUTPUT / 'robots.txt').read_text(), 'Missing robots sitemap'
    print(f'PASS: {len(urls)} canonical, indexable pages; {downloads} dataset downloads; research-page links and JSON-LD.')

if __name__ == '__main__':
    check()
