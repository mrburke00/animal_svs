
#!/bin/env bash

snakemake -s multisample.smk \
          --cores 12 \
	  --until AllCall \
          --resources disk_mb=700000 \
          --use-conda --conda-frontend mamba \
          --conda-prefix /mnt/local/snakemake-conda \
          --scheduler greedy

#snakemake -s genotype.smk \
#          -j $(grep -c ^processor /proc/cpuinfo) \
#          --resources disk_mb=700000 \
#          --use-conda --conda-frontend mamba \
#          --conda-prefix /mnt/local/snakemake-conda \
#          --scheduler greedy
