#!/usr/bin/env bash

dir=$(dirname "$0")
today=$(date "+%Y%m%d")
current_year=$(date "+%Y")

racket ${dir}/extract.rkt
racket ${dir}/transform-load.2019-03-28.rkt -p "$1"

7zr a /var/tmp/nyse/cta-summary/cta-summary.${current_year}.7z /var/tmp/nyse/cta-summary/CTA.Summary.EODSUM.${today}.csv
