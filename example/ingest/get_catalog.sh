#!/bin/bash
wget https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv.gz
python fmt_catalog.py > urls.txt
rm pg_catalog.csv.gz
