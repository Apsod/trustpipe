import gzip
import json
import csv
from dataclasses import dataclass

germanic = set(['ofs', 'ovd', 'li', 'sv', 'nno', 'yid', 'fy', 'gsw', 'en', 'geh', 'deu', 'fao', 'pdc', 'nb', 'swe', 'afr', 'da', 'nld', 'is', 'eng', 'lb', 'no', 'bar', 'dan', 'fry', 'nob', 'sco', 'nds', 'nl', 'frr', 'yi', 'frs', 'af', 'ltz', 'de', 'isl', 'fo', 'gml', 'vls', 'nn', 'lim', 'gct', 'nor'])

@dataclass
class Row:
    id: str
    type: str
    issued: str
    title: str
    lang: str
    author: str
    subjects: str
    locc: str
    shelves: str
    
    @property
    def langs(self):
        return [l.strip() for l in self.lang.split(';')]

def chain(g0, *gs):
    def _chain(a, b):
        for x in a:
            yield from b(x)
    
    ret = g0
    for g in gs:
        ret = _chain(ret, g)
    yield from ret

def get_rows():
    with gzip.open('pg_catalog.csv.gz', 'rt') as f:
        reader = csv.reader(f)
        next(reader) # skip header
        yield from map(lambda vals: Row(*vals), reader)

def valid(row):
    if row.type == 'Text' and any(l in germanic for l in row.langs):
        yield row

def fmt(row):
    yield f'https://www.gutenberg.org/ebooks/{row.id}.epub.noimages'

if __name__ == '__main__':
    for row in chain(get_rows(), valid, fmt):
        print(row)
