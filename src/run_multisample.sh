#!/bin/env bash

snakemake -s multisample.smk \
          -j $(grep -c ^processor /proc/cpuinfo)
