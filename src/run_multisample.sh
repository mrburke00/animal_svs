#!/bin/env bash

snakemake -s multisample.smk \
          -j $(grep -c ^processor /proc/cpuinfo) \
          --resources disk_mb=50000 \
          --use-conda
