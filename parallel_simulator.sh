#!/bin/env bash

set -eu

function simreads() {
    local fasta=$1
    local n_reads=$2
    local out_name=$3
    local part=$4
    local seed=$RANDOM

    perl simulator.pl -random-seed $seed \
                      -output-format "${out_name}_${part}_#.fq"
                      -num-frags $n_reads \
                      -frag-dist-params "456,70" \
                      -read-length 100
}
export -f simreads

processes=$1
fasta=$2
out_name=$3

n_reads=$(python -c '135000000//36')

seq 1 36 | gargs "simreads $fasta $n_reads $out_name {}"
