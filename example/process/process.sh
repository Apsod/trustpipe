#!/bin/sh

OUT=/output/gutenberg.json
IN=/input

parallel python process.py ::: $IN/*.epub.noimages > $OUT

gzip $OUT
