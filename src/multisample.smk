"""
Multisample version of snakefile.  At the moment just testing with BAM/CRAM
as the starting point.  Will incorporate fastq alignment rule later.
"""
import os
# TODO add snakemake and boto install to setup script
import config_utils
from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider



################################################################################
## Setup
################################################################################
# configfile: "config.yaml"
# conf = config_utils.Config(config)
workdir: "/mnt/local/data"
S3 = S3RemoteProvider()

################################################################################
## Rules
################################################################################
rule all:
    input:
        # TODO test with hardcoded values
        # "{Config.workdir}/merged/{project_name}-sites.vcf.gz"
        '/mnt/local/data/merged/test-sites.vcf.gz'


### TODO Rule for getting bam paths from local or S3
    # conditional input to either use a local directory for bams or S3
    # controlled by config file params
        # s3: bool
        # s3_bucket: string -- bucket uri where the data is found
        # input_dir: string -- only used if s3 == False

# TODO Test with hard coded buckets
samples = S3.glob_wildcards("s3://layerlabcu/cow/bams/{sample}.bam")
rule get_data:
    input:
        S3.remote(expand("s3://layerlabcu/cow/bams/{sample}.bam"), sample=samples),
        S3.remote(expand("s3://layerlabcu/cow/bams/{sample}.bam.bai"), sample=samples)
    output:
        "{sample}/{sample}.bam"
        "{sample}/{sample}.bam.bai"
           
    


### TODO After Bam rules are done
# make conditional rule that gets fastqs or bam/cram
    # using glob_wildcards() check if the 

        



### TODO Rule(s) for getting high coverage regions with mosdepth
    # 1. Run mosdepth to get genome wide coverage
    # 2. Flag high cov regions (we can do this in mosdepth itself
    #    or after the fact using the coverage statistics)
    # 3. Merge close by regions (determined by some user defined metric)
    # 4. Final output is a bed file of high cov regions

### TODO get ref genome gap regions
    # input: reference fasta (s3 only for now)
    # uses extractGapRegions.py
    # output to bed file

### TODO merge high coverage regions with gap regions
    ## use 'bedtools merge -d $max_distance'

### TODO call/genotype SVs with smoove
    # use smoove call -g -d

### TODO merge all the sites with smoove
    # This is the Final step

