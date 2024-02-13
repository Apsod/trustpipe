#!/bin/sh

OUT=/output/gutenberg.json
IN=/input
TMPDIR=`mktemp -d`

parallel --results $TMPDIR/{1/} pandoc --from epub --to plain ::: $IN/*.epub.noimages
jq -Rsc 'capture("(?<prefix>.+?\\*{3} START OF THE PROJECT GUTENBERG EBOOK .*? \\*{3}\\s++)(?<text>.*\\S)(?<suffix>\\s*\\*\\*\\* END OF THE PROJECT GUTENBERG EBOOK.*+$)"; "m")' $TMPDIR/*.epub.noimages > $OUT

rm -rf $TMPDIR
gzip $OUT
